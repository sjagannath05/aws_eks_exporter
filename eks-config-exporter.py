#!/usr/bin/env python3

import json
import yaml
import argparse
import re
from urllib.parse import urlparse
import sys
import os
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
import subprocess
import boto3
from botocore.config import Config as BotoConfig
import kubeconfig_utils
import ratelimit
import future_utils
from concurrent.futures import ThreadPoolExecutor

DEFAULT_DESCRIBE_WORKERS = 8
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import logging
from pathlib import Path

class ExportError(Exception):
    """Fatal export problem with a user-actionable message."""


class EKSConfigExporter:
    """
    Comprehensive EKS cluster configuration exporter and visualizer.
    Exports all Kubernetes resources including pods, services, deployments, 
    daemonsets, configmaps, secrets, namespaces, and EKS-specific configurations.
    """
    
    def __init__(self, cluster_name: str = None, region: str = None, kubeconfig: str = None,
                 context: str = None, skip_aws: bool = False,
                 qps: float = ratelimit.DEFAULT_QPS, burst: int = ratelimit.DEFAULT_BURST,
                 describe_workers: int = DEFAULT_DESCRIBE_WORKERS):
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)

        self.kubeconfig = kubeconfig
        self.context = context
        self.skip_aws = skip_aws
        # One bucket shared by the Python client and kubectl describe so the
        # API server sees a single bounded stream. qps/burst <= 0 disables it.
        self.rate_limiter = ratelimit.TokenBucket(rate=qps, burst=burst)
        # kubectl describe is submitted here instead of run inline: each export_*
        # method returns immediately with a Future in 'describe_info', so describes
        # for one resource type run while later resource types are still being
        # listed. resolve_pending_describes() blocks on the results at the end.
        self.describe_workers = max(1, int(describe_workers))
        self._describe_executor = ThreadPoolExecutor(
            max_workers=self.describe_workers, thread_name_prefix="kubectl-describe")
        self.kube_context_info = None   # kubeconfig_utils.ContextInfo or None
        self.kube_identity = None       # kubeconfig_utils.EksIdentity or None
        self._resolve_kube_identity()

        self.cluster_name = self._resolve_cluster_name(cluster_name)
        self.region = self._resolve_region(region)
        self.account_id = self.kube_identity.account if self.kube_identity else None
        self.export_data = {
            'metadata': {
                'cluster_name': self.cluster_name,
                'region': self.region,
                'account_id': self.account_id,
                'kube_context': self.kube_context_info.name if self.kube_context_info else context,
                'kube_server': self.kube_context_info.server if self.kube_context_info else None,
                'kubeconfig': kubeconfig_utils.kubeconfig_paths(kubeconfig) if (kubeconfig or self.kube_context_info) else None,
                'aws_metadata': 'skipped' if skip_aws else 'included',
                'rate_limit': {'qps': qps, 'burst': burst} if self.rate_limiter.enabled else None,
                'describe_workers': self.describe_workers,
                'export_timestamp': datetime.now(timezone.utc).isoformat(),
                'exporter_version': '1.0.0'
            },
            'cluster_info': {},
            'resources': {}
        }
        
        # Initialize clients
        self.aws_client = None
        self.k8s_client = None
        self._init_clients()
    
    def _get_kubectl_describe(self, resource_type: str, resource_name: str, namespace: str = None) -> str:
        """Get kubectl describe output for a resource."""
        try:
            cmd = ['kubectl'] + self._kubectl_global_args() + ['describe', resource_type, resource_name]
            if namespace:
                cmd.extend(['-n', namespace])
            
            self.rate_limiter.acquire()
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                return result.stdout
            else:
                self.logger.warning(f"kubectl describe failed for {resource_type}/{resource_name}: {result.stderr}")
                return f"Error getting describe info: {result.stderr}"
        except subprocess.TimeoutExpired:
            self.logger.warning(f"kubectl describe timeout for {resource_type}/{resource_name}")
            return "Timeout getting describe info"
        except Exception as e:
            self.logger.warning(f"Failed to get kubectl describe for {resource_type}/{resource_name}: {e}")
            return f"Error getting describe info: {str(e)}"
    
    def _resolve_kube_identity(self):
        """Read the kubeconfig (if any) to learn which EKS cluster the context points at."""
        try:
            cfg = kubeconfig_utils.load_kubeconfig(self.kubeconfig)
            self.kube_context_info = kubeconfig_utils.resolve_context(cfg, self.context)
            self.kube_identity = kubeconfig_utils.eks_identity_for_context(cfg, self.context)
        except kubeconfig_utils.KubeconfigError as e:
            if self.kubeconfig or self.context:
                # User asked for a specific file/context: failing to read it is fatal.
                raise ValueError(str(e)) from e
            self.logger.debug(f"No usable kubeconfig ({e}); assuming in-cluster or default config")
            return
        if self.kube_identity:
            self.logger.info(
                f"kubeconfig context '{self.kube_context_info.name}' -> EKS cluster "
                f"'{self.kube_identity.cluster_name}' region={self.kube_identity.region} "
                f"account={self.kube_identity.account} (from {self.kube_identity.source})")
        else:
            self.logger.warning(
                f"kubeconfig context '{self.kube_context_info.name}' does not look like an EKS cluster; "
                "cluster name and region must be passed explicitly")

    def _resolve_cluster_name(self, cluster_name: str) -> str:
        derived = self.kube_identity.cluster_name if self.kube_identity else None
        if cluster_name and derived and cluster_name != derived:
            raise ValueError(
                f"cluster name '{cluster_name}' does not match kubeconfig context "
                f"'{self.kube_context_info.name}' which points at EKS cluster '{derived}'. "
                "Fix the argument or choose another --context.")
        if cluster_name:
            return cluster_name
        if derived:
            return derived
        raise ValueError(
            "cluster name not given and could not be derived from the kubeconfig context; "
            "pass it as the first positional argument")

    def _resolve_region(self, region: str) -> str:
        derived = self.kube_identity.region if self.kube_identity else None
        if region and derived and region != derived:
            raise ValueError(
                f"region '{region}' does not match kubeconfig context "
                f"'{self.kube_context_info.name}' which is in region '{derived}'")
        resolved = (region or derived
                    or os.environ.get('AWS_REGION') or os.environ.get('AWS_DEFAULT_REGION')
                    or boto3.session.Session().region_name)
        if not resolved:
            if self.skip_aws:
                return None
            raise ValueError("AWS region not given, not derivable from kubeconfig, and not set in AWS config; pass --region")
        return resolved

    def _verify_kube_endpoint(self, eks_endpoint: str):
        """Make sure the kubeconfig context really points at the EKS cluster we described.

        Needed when the context is not ARN-named (so no identity could be derived)
        and the user passed the cluster name by hand. A direct EKS endpoint that
        differs is a definite mismatch and fatal; a non-EKS server (SSM tunnel,
        proxy, localhost) cannot be verified and only warns.
        """
        if not eks_endpoint or not self.kube_context_info or not self.kube_context_info.server:
            return
        kube_host = urlparse(self.kube_context_info.server).hostname or ''
        eks_host = urlparse(eks_endpoint).hostname or ''
        if kube_host.lower() == eks_host.lower():
            return
        ctx = self.kube_context_info.name
        if kube_host.lower().endswith('.eks.amazonaws.com'):
            raise ExportError(
                f"kubeconfig context '{ctx}' points at {kube_host} but EKS cluster "
                f"'{self.cluster_name}' ({self.region}) is served at {eks_host}. "
                "The Kubernetes resources would be labelled with the wrong cluster; "
                "pick the right --context or cluster name.")
        self.logger.warning(
            f"kubeconfig context '{ctx}' server {kube_host} is not the EKS endpoint {eks_host} "
            f"(tunnel/proxy?). Cannot verify it belongs to cluster '{self.cluster_name}'.")

    def _get_kubectl_describe_async(self, resource_type: str, resource_name: str, namespace: str = None):
        """Non-blocking version of _get_kubectl_describe(): returns a Future.

        Resolve the whole export_data tree with resolve_pending_describes()
        before saving; a bare Future is not JSON/YAML serializable.
        """
        return self._describe_executor.submit(
            self._get_kubectl_describe, resource_type, resource_name, namespace)

    def resolve_pending_describes(self):
        """Block until every submitted kubectl describe call has finished.

        Safe to call even if nothing is pending. Logs progress periodically
        since this is typically the longest step of a full export.
        """
        futures = future_utils.collect_futures(self.export_data['resources'])
        if not futures:
            return
        self.logger.info(f"Waiting on {len(futures)} kubectl describe call(s) "
                         f"({self.describe_workers} parallel worker(s))...")
        future_utils.wait_with_progress(
            futures,
            on_progress=lambda done, total: self.logger.info(f"kubectl describe: {done}/{total} complete"))
        self.export_data['resources'] = future_utils.resolve_futures(self.export_data['resources'])
        self._describe_executor.shutdown(wait=True)

    def _kubectl_global_args(self) -> list:
        """Flags so kubectl talks to the same cluster as the Python client."""
        args = []
        if self.kubeconfig:
            args.extend(['--kubeconfig', self.kubeconfig])
        if self.context:
            args.extend(['--context', self.context])
        return args

    def _init_clients(self):
        """Initialize AWS and Kubernetes clients."""
        try:
            # AWS client (not needed with --skip-aws)
            self.aws_client = None if self.skip_aws else boto3.client(
                'eks', region_name=self.region,
                config=BotoConfig(retries={'mode': 'adaptive', 'max_attempts': 8}))
            
            # Kubernetes client
            if self.kubeconfig or self.context:
                config.load_kube_config(config_file=self.kubeconfig, context=self.context)
            else:
                try:
                    config.load_incluster_config()
                except config.ConfigException:
                    config.load_kube_config()
            
            self.k8s_client = {
                'core_v1': client.CoreV1Api(),
                'apps_v1': client.AppsV1Api(),
                'networking_v1': client.NetworkingV1Api(),
                'discovery_v1': client.DiscoveryV1Api(),
                'rbac_v1': client.RbacAuthorizationV1Api(),
                'storage_v1': client.StorageV1Api(),
                'batch_v1': client.BatchV1Api(),
                'autoscaling_v1': client.AutoscalingV1Api(),
                'autoscaling_v2': client.AutoscalingV2Api(),
                'policy_v1': client.PolicyV1Api(),
                'scheduling_v1': client.SchedulingV1Api(),
                'apiextensions_v1': client.ApiextensionsV1Api(),
                'admissionregistration_v1': client.AdmissionregistrationV1Api(),
                'apiregistration_v1': client.ApiregistrationV1Api(),
                'coordination_v1': client.CoordinationV1Api(),
                'custom_objects': client.CustomObjectsApi()
            }
            
            # Add optional clients that may not be available in all Kubernetes versions
            try:
                self.k8s_client['node_v1'] = client.NodeV1Api()
            except AttributeError:
                self.logger.warning("NodeV1Api not available in this Kubernetes client version")
            
            try:
                self.k8s_client['flowcontrol_v1beta3'] = client.FlowcontrolV1beta3Api()
            except AttributeError:
                self.k8s_client['flowcontrol_v1beta3'] = None
                self.logger.warning("FlowcontrolV1beta3Api not available in this Kubernetes client version")
            
            try:
                self.k8s_client['policy_v1beta1'] = client.PolicyV1beta1Api()
            except AttributeError:
                self.k8s_client['policy_v1beta1'] = None
                self.logger.warning("PolicyV1beta1Api not available in this Kubernetes client version")
            
            # Rate-limit + 429 retry for every API call without touching the 55 call sites.
            self.k8s_client = {
                name: (ratelimit.RateLimitedApi(api, self.rate_limiter, ApiException) if api is not None else None)
                for name, api in self.k8s_client.items()
            }
            if self.rate_limiter.enabled:
                self.logger.info(f"Rate limit: {self.rate_limiter.rate:g} req/s, burst {self.rate_limiter.burst} "
                                 "(shared by API calls and kubectl describe)")
            else:
                self.logger.warning("Rate limiting disabled (--qps 0)")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize clients: {e}")
            raise
    
    def export_cluster_info(self):
        """Export EKS cluster basic information.

        Raises ExportError when the EKS API call fails (wrong region/account,
        missing permissions, no credentials) unless skip_aws was requested.
        A silent failure here used to produce a 'successful' export whose
        AWS metadata belonged to no cluster at all.
        """
        if self.skip_aws:
            self.logger.info("Skipping EKS API metadata (--skip-aws)")
            self.export_data['cluster_info'] = {}
            return
        try:
            cluster_info = self.aws_client.describe_cluster(name=self.cluster_name)
            self._verify_kube_endpoint(cluster_info['cluster'].get('endpoint'))
            self.export_data['cluster_info'] = cluster_info['cluster']
            
            # Get node groups
            nodegroups = self.aws_client.list_nodegroups(clusterName=self.cluster_name)
            self.export_data['cluster_info']['nodegroups'] = []
            
            for ng_name in nodegroups['nodegroups']:
                ng_detail = self.aws_client.describe_nodegroup(
                    clusterName=self.cluster_name, 
                    nodegroupName=ng_name
                )
                self.export_data['cluster_info']['nodegroups'].append(ng_detail['nodegroup'])
                
        except ExportError:
            raise
        except Exception as e:
            hint = (f"cluster '{self.cluster_name}' in region '{self.region}'"
                    + (f" (kubeconfig says account {self.account_id})" if self.account_id else ""))
            raise ExportError(
                f"EKS API lookup failed for {hint}: {e}. "
                "Check --region/cluster name and AWS credentials, or pass --skip-aws to export "
                "Kubernetes resources without EKS metadata.") from e
    
    def export_namespaces(self):
        """Export all namespaces."""
        try:
            namespaces = self.k8s_client['core_v1'].list_namespace()
            self.export_data['resources']['namespaces'] = []
            
            for ns in namespaces.items:
                self.export_data['resources']['namespaces'].append({
                    'name': ns.metadata.name,
                    'labels': ns.metadata.labels or {},
                    'annotations': ns.metadata.annotations or {},
                    'status': ns.status.phase,
                    'creation_timestamp': ns.metadata.creation_timestamp.isoformat() if ns.metadata.creation_timestamp else None,
                    'describe_info': self._get_kubectl_describe_async('namespace', ns.metadata.name)
                })
                
        except ApiException as e:
            self.logger.error(f"Failed to export namespaces: {e}")
    
    def export_nodes(self):
        """Export all nodes."""
        try:
            nodes = self.k8s_client['core_v1'].list_node()
            self.export_data['resources']['nodes'] = []
            
            for node in nodes.items:
                node_info = {
                    'name': node.metadata.name,
                    'labels': node.metadata.labels or {},
                    'annotations': node.metadata.annotations or {},
                    'conditions': [],
                    'capacity': node.status.capacity or {},
                    'allocatable': node.status.allocatable or {},
                    'node_info': node.status.node_info.to_dict() if node.status.node_info else {},
                    'creation_timestamp': node.metadata.creation_timestamp.isoformat() if node.metadata.creation_timestamp else None,
                    'describe_info': self._get_kubectl_describe_async('node', node.metadata.name)
                }
                
                if node.status.conditions:
                    for condition in node.status.conditions:
                        node_info['conditions'].append({
                            'type': condition.type,
                            'status': condition.status,
                            'reason': condition.reason,
                            'message': condition.message
                        })
                
                self.export_data['resources']['nodes'].append(node_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export nodes: {e}")
    
    def export_pods(self):
        """Export all pods from all namespaces."""
        try:
            pods = self.k8s_client['core_v1'].list_pod_for_all_namespaces()
            self.export_data['resources']['pods'] = []
            
            for pod in pods.items:
                pod_info = {
                    'name': pod.metadata.name,
                    'namespace': pod.metadata.namespace,
                    'labels': pod.metadata.labels or {},
                    'annotations': pod.metadata.annotations or {},
                    'phase': pod.status.phase,
                    'pod_ip': pod.status.pod_ip,
                    'host_ip': pod.status.host_ip,
                    'node_name': pod.spec.node_name,
                    'containers': [],
                    'init_containers': [],
                    'volumes': [],
                    'conditions': [],
                    'creation_timestamp': pod.metadata.creation_timestamp.isoformat() if pod.metadata.creation_timestamp else None,
                    'describe_info': self._get_kubectl_describe_async('pod', pod.metadata.name, pod.metadata.namespace)
                }
                
                # Container information
                if pod.spec.containers:
                    for container in pod.spec.containers:
                        container_info = {
                            'name': container.name,
                            'image': container.image,
                            'ports': [{'containerPort': p.container_port, 'protocol': p.protocol} for p in (container.ports or [])],
                            'env': [{'name': e.name, 'value': e.value} for e in (container.env or [])],
                            'resources': container.resources.to_dict() if container.resources else {}
                        }
                        pod_info['containers'].append(container_info)
                
                # Init containers
                if pod.spec.init_containers:
                    for init_container in pod.spec.init_containers:
                        init_info = {
                            'name': init_container.name,
                            'image': init_container.image
                        }
                        pod_info['init_containers'].append(init_info)
                
                # Volumes
                if pod.spec.volumes:
                    for volume in pod.spec.volumes:
                        volume_info = {'name': volume.name}
                        if volume.config_map:
                            volume_info['type'] = 'configMap'
                            volume_info['configMap'] = volume.config_map.name
                        elif volume.secret:
                            volume_info['type'] = 'secret'
                            volume_info['secret'] = volume.secret.secret_name
                        elif volume.persistent_volume_claim:
                            volume_info['type'] = 'persistentVolumeClaim'
                            volume_info['pvc'] = volume.persistent_volume_claim.claim_name
                        elif volume.host_path:
                            volume_info['type'] = 'hostPath'
                            volume_info['hostPath'] = volume.host_path.path
                        
                        pod_info['volumes'].append(volume_info)
                
                # Conditions
                if pod.status.conditions:
                    for condition in pod.status.conditions:
                        pod_info['conditions'].append({
                            'type': condition.type,
                            'status': condition.status,
                            'reason': condition.reason
                        })
                
                self.export_data['resources']['pods'].append(pod_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export pods: {e}")
    
    def export_services(self):
        """Export all services."""
        try:
            services = self.k8s_client['core_v1'].list_service_for_all_namespaces()
            self.export_data['resources']['services'] = []
            
            for service in services.items:
                service_info = {
                    'name': service.metadata.name,
                    'namespace': service.metadata.namespace,
                    'labels': service.metadata.labels or {},
                    'annotations': service.metadata.annotations or {},
                    'type': service.spec.type,
                    'cluster_ip': service.spec.cluster_ip,
                    'ports': [],
                    'selector': service.spec.selector or {},
                    'creation_timestamp': service.metadata.creation_timestamp.isoformat() if service.metadata.creation_timestamp else None,
                    'describe_info': self._get_kubectl_describe_async('service', service.metadata.name, service.metadata.namespace)
                }
                
                if service.spec.ports:
                    for port in service.spec.ports:
                        port_info = {
                            'name': port.name,
                            'port': port.port,
                            'target_port': str(port.target_port) if port.target_port else None,
                            'protocol': port.protocol
                        }
                        if port.node_port:
                            port_info['node_port'] = port.node_port
                        service_info['ports'].append(port_info)
                
                if service.status.load_balancer and service.status.load_balancer.ingress:
                    service_info['load_balancer_ingress'] = []
                    for ingress in service.status.load_balancer.ingress:
                        ingress_info = {}
                        if ingress.ip:
                            ingress_info['ip'] = ingress.ip
                        if ingress.hostname:
                            ingress_info['hostname'] = ingress.hostname
                        service_info['load_balancer_ingress'].append(ingress_info)
                
                self.export_data['resources']['services'].append(service_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export services: {e}")
    
    def export_deployments(self):
        """Export all deployments."""
        try:
            deployments = self.k8s_client['apps_v1'].list_deployment_for_all_namespaces()
            self.export_data['resources']['deployments'] = []
            
            for deployment in deployments.items:
                deployment_info = {
                    'name': deployment.metadata.name,
                    'namespace': deployment.metadata.namespace,
                    'labels': deployment.metadata.labels or {},
                    'annotations': deployment.metadata.annotations or {},
                    'replicas': deployment.spec.replicas,
                    'ready_replicas': deployment.status.ready_replicas or 0,
                    'available_replicas': deployment.status.available_replicas or 0,
                    'selector': deployment.spec.selector.match_labels if deployment.spec.selector else {},
                    'strategy': deployment.spec.strategy.type if deployment.spec.strategy else None,
                    'creation_timestamp': deployment.metadata.creation_timestamp.isoformat() if deployment.metadata.creation_timestamp else None,
                    'describe_info': self._get_kubectl_describe_async('deployment', deployment.metadata.name, deployment.metadata.namespace)
                }
                
                self.export_data['resources']['deployments'].append(deployment_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export deployments: {e}")
    
    def export_daemonsets(self):
        """Export all daemonsets."""
        try:
            daemonsets = self.k8s_client['apps_v1'].list_daemon_set_for_all_namespaces()
            self.export_data['resources']['daemonsets'] = []
            
            for ds in daemonsets.items:
                ds_info = {
                    'name': ds.metadata.name,
                    'namespace': ds.metadata.namespace,
                    'labels': ds.metadata.labels or {},
                    'annotations': ds.metadata.annotations or {},
                    'desired_number_scheduled': ds.status.desired_number_scheduled or 0,
                    'current_number_scheduled': ds.status.current_number_scheduled or 0,
                    'number_ready': ds.status.number_ready or 0,
                    'selector': ds.spec.selector.match_labels if ds.spec.selector else {},
                    'creation_timestamp': ds.metadata.creation_timestamp.isoformat() if ds.metadata.creation_timestamp else None,
                    'describe_info': self._get_kubectl_describe_async('daemonset', ds.metadata.name, ds.metadata.namespace)
                }
                
                self.export_data['resources']['daemonsets'].append(ds_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export daemonsets: {e}")
    
    def export_replicasets(self):
        """Export all replicasets."""
        try:
            replicasets = self.k8s_client['apps_v1'].list_replica_set_for_all_namespaces()
            self.export_data['resources']['replicasets'] = []
            
            for rs in replicasets.items:
                rs_info = {
                    'name': rs.metadata.name,
                    'namespace': rs.metadata.namespace,
                    'labels': rs.metadata.labels or {},
                    'annotations': rs.metadata.annotations or {},
                    'replicas': rs.spec.replicas,
                    'ready_replicas': rs.status.ready_replicas or 0,
                    'available_replicas': rs.status.available_replicas or 0,
                    'selector': rs.spec.selector.match_labels if rs.spec.selector else {},
                    'owner_references': [{'kind': ref.kind, 'name': ref.name} for ref in (rs.metadata.owner_references or [])],
                    'creation_timestamp': rs.metadata.creation_timestamp.isoformat() if rs.metadata.creation_timestamp else None
                }
                
                self.export_data['resources']['replicasets'].append(rs_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export replicasets: {e}")
    
    def export_statefulsets(self):
        """Export all statefulsets."""
        try:
            statefulsets = self.k8s_client['apps_v1'].list_stateful_set_for_all_namespaces()
            self.export_data['resources']['statefulsets'] = []
            
            for sts in statefulsets.items:
                sts_info = {
                    'name': sts.metadata.name,
                    'namespace': sts.metadata.namespace,
                    'labels': sts.metadata.labels or {},
                    'annotations': sts.metadata.annotations or {},
                    'replicas': sts.spec.replicas,
                    'ready_replicas': sts.status.ready_replicas or 0,
                    'current_replicas': sts.status.current_replicas or 0,
                    'updated_replicas': sts.status.updated_replicas or 0,
                    'selector': sts.spec.selector.match_labels if sts.spec.selector else {},
                    'service_name': sts.spec.service_name,
                    'volume_claim_templates': len(sts.spec.volume_claim_templates or []),
                    'creation_timestamp': sts.metadata.creation_timestamp.isoformat() if sts.metadata.creation_timestamp else None
                }
                
                self.export_data['resources']['statefulsets'].append(sts_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export statefulsets: {e}")
    
    def export_jobs(self):
        """Export all jobs."""
        try:
            jobs = self.k8s_client['batch_v1'].list_job_for_all_namespaces()
            self.export_data['resources']['jobs'] = []
            
            for job in jobs.items:
                job_info = {
                    'name': job.metadata.name,
                    'namespace': job.metadata.namespace,
                    'labels': job.metadata.labels or {},
                    'annotations': job.metadata.annotations or {},
                    'active': job.status.active or 0,
                    'succeeded': job.status.succeeded or 0,
                    'failed': job.status.failed or 0,
                    'completions': job.spec.completions,
                    'parallelism': job.spec.parallelism,
                    'backoff_limit': job.spec.backoff_limit,
                    'selector': job.spec.selector.match_labels if job.spec.selector else {},
                    'start_time': job.status.start_time.isoformat() if job.status.start_time else None,
                    'completion_time': job.status.completion_time.isoformat() if job.status.completion_time else None,
                    'creation_timestamp': job.metadata.creation_timestamp.isoformat() if job.metadata.creation_timestamp else None
                }
                
                self.export_data['resources']['jobs'].append(job_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export jobs: {e}")
    
    def export_cronjobs(self):
        """Export all cronjobs."""
        try:
            cronjobs = self.k8s_client['batch_v1'].list_cron_job_for_all_namespaces()
            self.export_data['resources']['cronjobs'] = []
            
            for cj in cronjobs.items:
                cj_info = {
                    'name': cj.metadata.name,
                    'namespace': cj.metadata.namespace,
                    'labels': cj.metadata.labels or {},
                    'annotations': cj.metadata.annotations or {},
                    'schedule': cj.spec.schedule,
                    'suspend': cj.spec.suspend or False,
                    'concurrency_policy': cj.spec.concurrency_policy,
                    'starting_deadline_seconds': cj.spec.starting_deadline_seconds,
                    'successful_jobs_history_limit': cj.spec.successful_jobs_history_limit,
                    'failed_jobs_history_limit': cj.spec.failed_jobs_history_limit,
                    'last_schedule_time': cj.status.last_schedule_time.isoformat() if cj.status.last_schedule_time else None,
                    'active_jobs': len(cj.status.active or []),
                    'creation_timestamp': cj.metadata.creation_timestamp.isoformat() if cj.metadata.creation_timestamp else None
                }
                
                self.export_data['resources']['cronjobs'].append(cj_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export cronjobs: {e}")
    
    def export_pod_templates(self):
        """Export all pod templates."""
        try:
            pod_templates = self.k8s_client['core_v1'].list_pod_template_for_all_namespaces()
            self.export_data['resources']['pod_templates'] = []
            
            for pt in pod_templates.items:
                pt_info = {
                    'name': pt.metadata.name,
                    'namespace': pt.metadata.namespace,
                    'labels': pt.metadata.labels or {},
                    'annotations': pt.metadata.annotations or {},
                    'template_labels': pt.template.metadata.labels if pt.template.metadata else {},
                    'template_annotations': pt.template.metadata.annotations if pt.template.metadata else {},
                    'containers': len(pt.template.spec.containers or []),
                    'creation_timestamp': pt.metadata.creation_timestamp.isoformat() if pt.metadata.creation_timestamp else None
                }
                
                self.export_data['resources']['pod_templates'].append(pt_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export pod templates: {e}")
    
    def export_priority_classes(self):
        """Export all priority classes."""
        try:
            priority_classes = self.k8s_client['scheduling_v1'].list_priority_class()
            self.export_data['resources']['priority_classes'] = []
            
            for pc in priority_classes.items:
                pc_info = {
                    'name': pc.metadata.name,
                    'labels': pc.metadata.labels or {},
                    'annotations': pc.metadata.annotations or {},
                    'value': pc.value,
                    'global_default': pc.global_default or False,
                    'description': pc.description,
                    'creation_timestamp': pc.metadata.creation_timestamp.isoformat() if pc.metadata.creation_timestamp else None
                }
                
                self.export_data['resources']['priority_classes'].append(pc_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export priority classes: {e}")
    
    def export_horizontal_pod_autoscalers(self):
        """Export all horizontal pod autoscalers."""
        try:
            # Try v2 first, fall back to v1 if not available
            try:
                hpas = self.k8s_client['autoscaling_v2'].list_horizontal_pod_autoscaler_for_all_namespaces()
                api_version = 'v2'
            except:
                hpas = self.k8s_client['autoscaling_v1'].list_horizontal_pod_autoscaler_for_all_namespaces()
                api_version = 'v1'
                
            self.export_data['resources']['horizontal_pod_autoscalers'] = []
            
            for hpa in hpas.items:
                hpa_info = {
                    'name': hpa.metadata.name,
                    'namespace': hpa.metadata.namespace,
                    'labels': hpa.metadata.labels or {},
                    'annotations': hpa.metadata.annotations or {},
                    'api_version': api_version,
                    'scale_target_ref': {
                        'kind': hpa.spec.scale_target_ref.kind,
                        'name': hpa.spec.scale_target_ref.name,
                        'api_version': getattr(hpa.spec.scale_target_ref, 'api_version', None)
                    },
                    'min_replicas': hpa.spec.min_replicas,
                    'max_replicas': hpa.spec.max_replicas,
                    'current_replicas': hpa.status.current_replicas or 0,
                    'desired_replicas': hpa.status.desired_replicas or 0,
                    'creation_timestamp': hpa.metadata.creation_timestamp.isoformat() if hpa.metadata.creation_timestamp else None
                }
                
                # Add v2-specific fields if available
                if api_version == 'v2' and hasattr(hpa.spec, 'metrics') and hpa.spec.metrics:
                    hpa_info['metrics'] = len(hpa.spec.metrics)
                elif api_version == 'v1':
                    hpa_info['target_cpu_utilization_percentage'] = hpa.spec.target_cpu_utilization_percentage
                    hpa_info['current_cpu_utilization_percentage'] = hpa.status.current_cpu_utilization_percentage
                
                self.export_data['resources']['horizontal_pod_autoscalers'].append(hpa_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export horizontal pod autoscalers: {e}")
    
    def export_endpoints(self):
        """Export all endpoints."""
        try:
            endpoints = self.k8s_client['core_v1'].list_endpoints_for_all_namespaces()
            self.export_data['resources']['endpoints'] = []
            
            for ep in endpoints.items:
                ep_info = {
                    'name': ep.metadata.name,
                    'namespace': ep.metadata.namespace,
                    'labels': ep.metadata.labels or {},
                    'annotations': ep.metadata.annotations or {},
                    'subsets': [],
                    'creation_timestamp': ep.metadata.creation_timestamp.isoformat() if ep.metadata.creation_timestamp else None
                }
                
                if ep.subsets:
                    for subset in ep.subsets:
                        subset_info = {
                            'addresses': len(subset.addresses or []),
                            'not_ready_addresses': len(subset.not_ready_addresses or []),
                            'ports': [{'name': p.name, 'port': p.port, 'protocol': p.protocol} for p in (subset.ports or [])]
                        }
                        ep_info['subsets'].append(subset_info)
                
                self.export_data['resources']['endpoints'].append(ep_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export endpoints: {e}")
    
    def export_endpoint_slices(self):
        """Export all endpoint slices.

        Uses the raw JSON response instead of the typed client call. The
        generated V1EndpointSlice model treats 'endpoints' as a required,
        non-nullable field, but real clusters do produce EndpointSlices
        with endpoints=null (seen on headless/no-backend services). The
        typed call raises on the very first such object and the whole
        list is lost, not just the offending item; _preload_content=False
        sidesteps client-side model validation entirely.
        """
        try:
            raw = self.k8s_client['discovery_v1'].list_endpoint_slice_for_all_namespaces(
                _preload_content=False)
            payload = json.loads(raw.data)
            self.export_data['resources']['endpoint_slices'] = []
            
            for es in payload.get('items', []):
                metadata = es.get('metadata') or {}
                es_info = {
                    'name': metadata.get('name'),
                    'namespace': metadata.get('namespace'),
                    'labels': metadata.get('labels') or {},
                    'annotations': metadata.get('annotations') or {},
                    'address_type': es.get('addressType'),
                    'endpoints': len(es.get('endpoints') or []),
                    'ports': [{'name': p.get('name'), 'port': p.get('port'), 'protocol': p.get('protocol')}
                              for p in (es.get('ports') or [])],
                    'creation_timestamp': metadata.get('creationTimestamp')
                }
                
                self.export_data['resources']['endpoint_slices'].append(es_info)
                
        except Exception as e:
            self.logger.error(f"Failed to export endpoint slices: {e}")
    
    def export_ingress_classes(self):
        """Export all ingress classes."""
        try:
            ingress_classes = self.k8s_client['networking_v1'].list_ingress_class()
            self.export_data['resources']['ingress_classes'] = []
            
            for ic in ingress_classes.items:
                ic_info = {
                    'name': ic.metadata.name,
                    'labels': ic.metadata.labels or {},
                    'annotations': ic.metadata.annotations or {},
                    'controller': ic.spec.controller,
                    'parameters': ic.spec.parameters.to_dict() if ic.spec.parameters else None,
                    'creation_timestamp': ic.metadata.creation_timestamp.isoformat() if ic.metadata.creation_timestamp else None
                }
                
                self.export_data['resources']['ingress_classes'].append(ic_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export ingress classes: {e}")
    
    def export_configmaps(self):
        """Export all configmaps."""
        try:
            configmaps = self.k8s_client['core_v1'].list_config_map_for_all_namespaces()
            self.export_data['resources']['configmaps'] = []
            
            for cm in configmaps.items:
                cm_info = {
                    'name': cm.metadata.name,
                    'namespace': cm.metadata.namespace,
                    'labels': cm.metadata.labels or {},
                    'annotations': cm.metadata.annotations or {},
                    'data_keys': list(cm.data.keys()) if cm.data else [],
                    'creation_timestamp': cm.metadata.creation_timestamp.isoformat() if cm.metadata.creation_timestamp else None,
                    'describe_info': self._get_kubectl_describe_async('configmap', cm.metadata.name, cm.metadata.namespace)
                }
                
                self.export_data['resources']['configmaps'].append(cm_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export configmaps: {e}")
    
    def export_secrets(self):
        """Export all secrets (metadata only for security)."""
        try:
            secrets = self.k8s_client['core_v1'].list_secret_for_all_namespaces()
            self.export_data['resources']['secrets'] = []
            
            for secret in secrets.items:
                secret_info = {
                    'name': secret.metadata.name,
                    'namespace': secret.metadata.namespace,
                    'labels': secret.metadata.labels or {},
                    'annotations': secret.metadata.annotations or {},
                    'type': secret.type,
                    'data_keys': list(secret.data.keys()) if secret.data else [],
                    'creation_timestamp': secret.metadata.creation_timestamp.isoformat() if secret.metadata.creation_timestamp else None,
                    'describe_info': self._get_kubectl_describe_async('secret', secret.metadata.name, secret.metadata.namespace)
                }
                
                self.export_data['resources']['secrets'].append(secret_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export secrets: {e}")
    
    def export_ingresses(self):
        """Export all ingresses."""
        try:
            ingresses = self.k8s_client['networking_v1'].list_ingress_for_all_namespaces()
            self.export_data['resources']['ingresses'] = []
            
            for ingress in ingresses.items:
                ingress_info = {
                    'name': ingress.metadata.name,
                    'namespace': ingress.metadata.namespace,
                    'labels': ingress.metadata.labels or {},
                    'annotations': ingress.metadata.annotations or {},
                    'rules': [],
                    'tls': [],
                    'creation_timestamp': ingress.metadata.creation_timestamp.isoformat() if ingress.metadata.creation_timestamp else None
                }
                
                if ingress.spec.rules:
                    for rule in ingress.spec.rules:
                        rule_info = {
                            'host': rule.host,
                            'paths': []
                        }
                        if rule.http and rule.http.paths:
                            for path in rule.http.paths:
                                path_info = {
                                    'path': path.path,
                                    'path_type': path.path_type,
                                    'backend_service': path.backend.service.name if path.backend and path.backend.service else None,
                                    'backend_port': path.backend.service.port.number if path.backend and path.backend.service and path.backend.service.port else None
                                }
                                rule_info['paths'].append(path_info)
                        ingress_info['rules'].append(rule_info)
                
                if ingress.spec.tls:
                    for tls in ingress.spec.tls:
                        tls_info = {
                            'hosts': tls.hosts or [],
                            'secret_name': tls.secret_name
                        }
                        ingress_info['tls'].append(tls_info)
                
                self.export_data['resources']['ingresses'].append(ingress_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export ingresses: {e}")
    
    def export_persistent_volumes(self):
        """Export all persistent volumes."""
        try:
            pvs = self.k8s_client['core_v1'].list_persistent_volume()
            self.export_data['resources']['persistent_volumes'] = []
            
            for pv in pvs.items:
                pv_info = {
                    'name': pv.metadata.name,
                    'labels': pv.metadata.labels or {},
                    'annotations': pv.metadata.annotations or {},
                    'capacity': pv.spec.capacity,
                    'access_modes': pv.spec.access_modes,
                    'reclaim_policy': pv.spec.persistent_volume_reclaim_policy,
                    'status': pv.status.phase,
                    'claim': f"{pv.spec.claim_ref.namespace}/{pv.spec.claim_ref.name}" if pv.spec.claim_ref else None,
                    'storage_class': pv.spec.storage_class_name,
                    'creation_timestamp': pv.metadata.creation_timestamp.isoformat() if pv.metadata.creation_timestamp else None
                }
                
                self.export_data['resources']['persistent_volumes'].append(pv_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export persistent volumes: {e}")
    
    def export_persistent_volume_claims(self):
        """Export all persistent volume claims."""
        try:
            pvcs = self.k8s_client['core_v1'].list_persistent_volume_claim_for_all_namespaces()
            self.export_data['resources']['persistent_volume_claims'] = []
            
            for pvc in pvcs.items:
                pvc_info = {
                    'name': pvc.metadata.name,
                    'namespace': pvc.metadata.namespace,
                    'labels': pvc.metadata.labels or {},
                    'annotations': pvc.metadata.annotations or {},
                    'capacity': pvc.status.capacity or {},
                    'access_modes': pvc.spec.access_modes,
                    'storage_class': pvc.spec.storage_class_name,
                    'volume_name': pvc.spec.volume_name,
                    'status': pvc.status.phase,
                    'requested_storage': pvc.spec.resources.requests.get('storage') if pvc.spec.resources and pvc.spec.resources.requests else None,
                    'creation_timestamp': pvc.metadata.creation_timestamp.isoformat() if pvc.metadata.creation_timestamp else None
                }
                
                self.export_data['resources']['persistent_volume_claims'].append(pvc_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export persistent volume claims: {e}")
    
    def export_storage_classes(self):
        """Export all storage classes."""
        try:
            storage_classes = self.k8s_client['storage_v1'].list_storage_class()
            self.export_data['resources']['storage_classes'] = []
            
            for sc in storage_classes.items:
                sc_info = {
                    'name': sc.metadata.name,
                    'labels': sc.metadata.labels or {},
                    'annotations': sc.metadata.annotations or {},
                    'provisioner': sc.provisioner,
                    'parameters': sc.parameters or {},
                    'reclaim_policy': sc.reclaim_policy,
                    'allow_volume_expansion': sc.allow_volume_expansion,
                    'volume_binding_mode': sc.volume_binding_mode,
                    'allowed_topologies': len(sc.allowed_topologies or []),
                    'creation_timestamp': sc.metadata.creation_timestamp.isoformat() if sc.metadata.creation_timestamp else None
                }
                
                self.export_data['resources']['storage_classes'].append(sc_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export storage classes: {e}")
    
    def export_volume_attachments(self):
        """Export all volume attachments."""
        try:
            volume_attachments = self.k8s_client['storage_v1'].list_volume_attachment()
            self.export_data['resources']['volume_attachments'] = []
            
            for va in volume_attachments.items:
                va_info = {
                    'name': va.metadata.name,
                    'labels': va.metadata.labels or {},
                    'annotations': va.metadata.annotations or {},
                    'attacher': va.spec.attacher,
                    'node_name': va.spec.node_name,
                    'source': {
                        'persistent_volume_name': va.spec.source.persistent_volume_name if va.spec.source else None
                    },
                    'attached': va.status.attached if va.status else False,
                    'creation_timestamp': va.metadata.creation_timestamp.isoformat() if va.metadata.creation_timestamp else None
                }
                
                self.export_data['resources']['volume_attachments'].append(va_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export volume attachments: {e}")
    
    def export_csi_drivers(self):
        """Export all CSI drivers."""
        try:
            csi_drivers = self.k8s_client['storage_v1'].list_csi_driver()
            self.export_data['resources']['csi_drivers'] = []
            
            for driver in csi_drivers.items:
                driver_info = {
                    'name': driver.metadata.name,
                    'labels': driver.metadata.labels or {},
                    'annotations': driver.metadata.annotations or {},
                    'attach_required': driver.spec.attach_required,
                    'pod_info_on_mount': driver.spec.pod_info_on_mount,
                    'volume_lifecycle_modes': driver.spec.volume_lifecycle_modes or [],
                    'storage_capacity': driver.spec.storage_capacity,
                    'creation_timestamp': driver.metadata.creation_timestamp.isoformat() if driver.metadata.creation_timestamp else None
                }
                
                self.export_data['resources']['csi_drivers'].append(driver_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export CSI drivers: {e}")
    
    def export_csi_nodes(self):
        """Export all CSI nodes."""
        try:
            csi_nodes = self.k8s_client['storage_v1'].list_csi_node()
            self.export_data['resources']['csi_nodes'] = []
            
            for node in csi_nodes.items:
                node_info = {
                    'name': node.metadata.name,
                    'labels': node.metadata.labels or {},
                    'annotations': node.metadata.annotations or {},
                    'drivers': [],
                    'creation_timestamp': node.metadata.creation_timestamp.isoformat() if node.metadata.creation_timestamp else None
                }
                
                if node.spec.drivers:
                    for driver in node.spec.drivers:
                        driver_info = {
                            'name': driver.name,
                            'node_id': driver.node_id,
                            'topology_keys': driver.topology_keys or []
                        }
                        node_info['drivers'].append(driver_info)
                
                self.export_data['resources']['csi_nodes'].append(node_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export CSI nodes: {e}")
    
    def export_csi_storage_capacities(self):
        """Export all CSI storage capacities."""
        try:
            csi_capacities = self.k8s_client['storage_v1'].list_csi_storage_capacity_for_all_namespaces()
            self.export_data['resources']['csi_storage_capacities'] = []
            
            for capacity in csi_capacities.items:
                capacity_info = {
                    'name': capacity.metadata.name,
                    'namespace': capacity.metadata.namespace,
                    'labels': capacity.metadata.labels or {},
                    'annotations': capacity.metadata.annotations or {},
                    'storage_class_name': capacity.spec.storage_class_name,
                    'capacity': capacity.spec.capacity,
                    'maximum_volume_size': capacity.spec.maximum_volume_size,
                    'node_topology': capacity.spec.node_topology.to_dict() if capacity.spec.node_topology else {},
                    'creation_timestamp': capacity.metadata.creation_timestamp.isoformat() if capacity.metadata.creation_timestamp else None
                }
                
                self.export_data['resources']['csi_storage_capacities'].append(capacity_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export CSI storage capacities: {e}")
    
    def export_service_accounts(self):
        """Export all service accounts."""
        try:
            service_accounts = self.k8s_client['core_v1'].list_service_account_for_all_namespaces()
            self.export_data['resources']['service_accounts'] = []
            
            for sa in service_accounts.items:
                sa_info = {
                    'name': sa.metadata.name,
                    'namespace': sa.metadata.namespace,
                    'labels': sa.metadata.labels or {},
                    'annotations': sa.metadata.annotations or {},
                    'secrets': [{'name': secret.name} for secret in (sa.secrets or [])],
                    'image_pull_secrets': [{'name': ips.name} for ips in (sa.image_pull_secrets or [])],
                    'automount_service_account_token': sa.automount_service_account_token,
                    'creation_timestamp': sa.metadata.creation_timestamp.isoformat() if sa.metadata.creation_timestamp else None
                }
                
                self.export_data['resources']['service_accounts'].append(sa_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export service accounts: {e}")
    
    def export_cluster_roles(self):
        """Export all cluster roles."""
        try:
            cluster_roles = self.k8s_client['rbac_v1'].list_cluster_role()
            self.export_data['resources']['cluster_roles'] = []
            
            for cr in cluster_roles.items:
                cr_info = {
                    'name': cr.metadata.name,
                    'labels': cr.metadata.labels or {},
                    'annotations': cr.metadata.annotations or {},
                    'rules': [],
                    'aggregation_rule': cr.aggregation_rule.to_dict() if cr.aggregation_rule else None,
                    'creation_timestamp': cr.metadata.creation_timestamp.isoformat() if cr.metadata.creation_timestamp else None
                }
                
                if cr.rules:
                    for rule in cr.rules:
                        rule_info = {
                            'api_groups': rule.api_groups or [],
                            'resources': rule.resources or [],
                            'verbs': rule.verbs or [],
                            'resource_names': rule.resource_names or [],
                            'non_resource_urls': rule.non_resource_ur_ls or []
                        }
                        cr_info['rules'].append(rule_info)
                
                self.export_data['resources']['cluster_roles'].append(cr_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export cluster roles: {e}")
    
    def export_cluster_role_bindings(self):
        """Export all cluster role bindings."""
        try:
            cluster_role_bindings = self.k8s_client['rbac_v1'].list_cluster_role_binding()
            self.export_data['resources']['cluster_role_bindings'] = []
            
            for crb in cluster_role_bindings.items:
                crb_info = {
                    'name': crb.metadata.name,
                    'labels': crb.metadata.labels or {},
                    'annotations': crb.metadata.annotations or {},
                    'role_ref': {
                        'api_group': crb.role_ref.api_group,
                        'kind': crb.role_ref.kind,
                        'name': crb.role_ref.name
                    },
                    'subjects': [],
                    'creation_timestamp': crb.metadata.creation_timestamp.isoformat() if crb.metadata.creation_timestamp else None
                }
                
                if crb.subjects:
                    for subject in crb.subjects:
                        subject_info = {
                            'kind': subject.kind,
                            'name': subject.name,
                            'namespace': subject.namespace,
                            'api_group': subject.api_group
                        }
                        crb_info['subjects'].append(subject_info)
                
                self.export_data['resources']['cluster_role_bindings'].append(crb_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export cluster role bindings: {e}")
    
    def export_roles(self):
        """Export all roles."""
        try:
            roles = self.k8s_client['rbac_v1'].list_role_for_all_namespaces()
            self.export_data['resources']['roles'] = []
            
            for role in roles.items:
                role_info = {
                    'name': role.metadata.name,
                    'namespace': role.metadata.namespace,
                    'labels': role.metadata.labels or {},
                    'annotations': role.metadata.annotations or {},
                    'rules': [],
                    'creation_timestamp': role.metadata.creation_timestamp.isoformat() if role.metadata.creation_timestamp else None
                }
                
                if role.rules:
                    for rule in role.rules:
                        rule_info = {
                            'api_groups': rule.api_groups or [],
                            'resources': rule.resources or [],
                            'verbs': rule.verbs or [],
                            'resource_names': rule.resource_names or []
                        }
                        role_info['rules'].append(rule_info)
                
                self.export_data['resources']['roles'].append(role_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export roles: {e}")
    
    def export_role_bindings(self):
        """Export all role bindings."""
        try:
            role_bindings = self.k8s_client['rbac_v1'].list_role_binding_for_all_namespaces()
            self.export_data['resources']['role_bindings'] = []
            
            for rb in role_bindings.items:
                rb_info = {
                    'name': rb.metadata.name,
                    'namespace': rb.metadata.namespace,
                    'labels': rb.metadata.labels or {},
                    'annotations': rb.metadata.annotations or {},
                    'role_ref': {
                        'api_group': rb.role_ref.api_group,
                        'kind': rb.role_ref.kind,
                        'name': rb.role_ref.name
                    },
                    'subjects': [],
                    'creation_timestamp': rb.metadata.creation_timestamp.isoformat() if rb.metadata.creation_timestamp else None
                }
                
                if rb.subjects:
                    for subject in rb.subjects:
                        subject_info = {
                            'kind': subject.kind,
                            'name': subject.name,
                            'namespace': subject.namespace,
                            'api_group': subject.api_group
                        }
                        rb_info['subjects'].append(subject_info)
                
                self.export_data['resources']['role_bindings'].append(rb_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export role bindings: {e}")
    
    def export_limit_ranges(self):
        """Export all limit ranges."""
        try:
            limit_ranges = self.k8s_client['core_v1'].list_limit_range_for_all_namespaces()
            self.export_data['resources']['limit_ranges'] = []
            
            for lr in limit_ranges.items:
                lr_info = {
                    'name': lr.metadata.name,
                    'namespace': lr.metadata.namespace,
                    'labels': lr.metadata.labels or {},
                    'annotations': lr.metadata.annotations or {},
                    'limits': [{'type': limit.type, 'default': limit.default or {}, 'default_request': limit.default_request or {}, 'max': limit.max or {}, 'min': limit.min or {}} for limit in (lr.spec.limits or [])],
                    'creation_timestamp': lr.metadata.creation_timestamp.isoformat() if lr.metadata.creation_timestamp else None
                }
                
                self.export_data['resources']['limit_ranges'].append(lr_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export limit ranges: {e}")
    
    def export_resource_quotas(self):
        """Export all resource quotas."""
        try:
            resource_quotas = self.k8s_client['core_v1'].list_resource_quota_for_all_namespaces()
            self.export_data['resources']['resource_quotas'] = []
            
            for rq in resource_quotas.items:
                rq_info = {
                    'name': rq.metadata.name,
                    'namespace': rq.metadata.namespace,
                    'labels': rq.metadata.labels or {},
                    'annotations': rq.metadata.annotations or {},
                    'hard': rq.spec.hard or {},
                    'used': rq.status.used or {},
                    'scope_selector': rq.spec.scope_selector.to_dict() if rq.spec.scope_selector else None,
                    'creation_timestamp': rq.metadata.creation_timestamp.isoformat() if rq.metadata.creation_timestamp else None
                }
                
                self.export_data['resources']['resource_quotas'].append(rq_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export resource quotas: {e}")
    
    def export_network_policies(self):
        """Export all network policies."""
        try:
            network_policies = self.k8s_client['networking_v1'].list_network_policy_for_all_namespaces()
            self.export_data['resources']['network_policies'] = []
            
            for np in network_policies.items:
                np_info = {
                    'name': np.metadata.name,
                    'namespace': np.metadata.namespace,
                    'labels': np.metadata.labels or {},
                    'annotations': np.metadata.annotations or {},
                    'pod_selector': np.spec.pod_selector.to_dict() if np.spec.pod_selector else {},
                    'policy_types': np.spec.policy_types or [],
                    'ingress_rules': len(np.spec.ingress or []),
                    'egress_rules': len(np.spec.egress or []),
                    'creation_timestamp': np.metadata.creation_timestamp.isoformat() if np.metadata.creation_timestamp else None
                }
                
                self.export_data['resources']['network_policies'].append(np_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export network policies: {e}")
    
    def export_pod_disruption_budgets(self):
        """Export all pod disruption budgets."""
        try:
            pdbs = self.k8s_client['policy_v1'].list_pod_disruption_budget_for_all_namespaces()
            self.export_data['resources']['pod_disruption_budgets'] = []
            
            for pdb in pdbs.items:
                pdb_info = {
                    'name': pdb.metadata.name,
                    'namespace': pdb.metadata.namespace,
                    'labels': pdb.metadata.labels or {},
                    'annotations': pdb.metadata.annotations or {},
                    'min_available': str(pdb.spec.min_available) if pdb.spec.min_available else None,
                    'max_unavailable': str(pdb.spec.max_unavailable) if pdb.spec.max_unavailable else None,
                    'selector': pdb.spec.selector.to_dict() if pdb.spec.selector else {},
                    'current_healthy': pdb.status.current_healthy if pdb.status else 0,
                    'desired_healthy': pdb.status.desired_healthy if pdb.status else 0,
                    'disruptions_allowed': pdb.status.disruptions_allowed if pdb.status else 0,
                    'creation_timestamp': pdb.metadata.creation_timestamp.isoformat() if pdb.metadata.creation_timestamp else None
                }
                
                self.export_data['resources']['pod_disruption_budgets'].append(pdb_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export pod disruption budgets: {e}")
    
    def export_custom_resource_definitions(self):
        """Export all custom resource definitions."""
        try:
            crds = self.k8s_client['apiextensions_v1'].list_custom_resource_definition()
            self.export_data['resources']['custom_resource_definitions'] = []
            
            for crd in crds.items:
                crd_info = {
                    'name': crd.metadata.name,
                    'labels': crd.metadata.labels or {},
                    'annotations': crd.metadata.annotations or {},
                    'group': crd.spec.group,
                    'scope': crd.spec.scope,
                    'names': crd.spec.names.to_dict() if crd.spec.names else {},
                    'versions': [{'name': v.name, 'served': v.served, 'storage': v.storage} for v in (crd.spec.versions or [])],
                    'conversion': crd.spec.conversion.to_dict() if crd.spec.conversion else None,
                    'creation_timestamp': crd.metadata.creation_timestamp.isoformat() if crd.metadata.creation_timestamp else None
                }
                
                self.export_data['resources']['custom_resource_definitions'].append(crd_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export custom resource definitions: {e}")
    
    def export_eni_configs(self):
        """Export AWS VPC CNI ENIConfigs."""
        try:
            eni_configs = self.k8s_client['custom_objects'].list_cluster_custom_object(
                group='crd.k8s.amazonaws.com',
                version='v1alpha1',
                plural='eniconfigs'
            )
            self.export_data['resources']['eni_configs'] = []
            
            for eni_config in eni_configs.get('items', []):
                eni_info = {
                    'name': eni_config.get('metadata', {}).get('name', 'N/A'),
                    'labels': eni_config.get('metadata', {}).get('labels', {}),
                    'annotations': eni_config.get('metadata', {}).get('annotations', {}),
                    'spec': eni_config.get('spec', {}),
                    'creation_timestamp': eni_config.get('metadata', {}).get('creationTimestamp'),
                    'full_config': eni_config
                }
                self.export_data['resources']['eni_configs'].append(eni_info)
                
        except Exception as e:
            self.logger.warning(f"ENIConfigs not found or accessible: {e}")
            self.export_data['resources']['eni_configs'] = []
    
    def export_security_group_policies(self):
        """Export AWS VPC CNI SecurityGroupPolicies."""
        try:
            sg_policies = self.k8s_client['custom_objects'].list_namespaced_custom_object(
                group='vpcresources.k8s.aws',
                version='v1beta1', 
                namespace='',
                plural='securitygrouppolicies'
            )
            self.export_data['resources']['security_group_policies'] = []
            
            for sg_policy in sg_policies.get('items', []):
                sg_info = {
                    'name': sg_policy.get('metadata', {}).get('name', 'N/A'),
                    'namespace': sg_policy.get('metadata', {}).get('namespace', 'N/A'),
                    'labels': sg_policy.get('metadata', {}).get('labels', {}),
                    'annotations': sg_policy.get('metadata', {}).get('annotations', {}),
                    'spec': sg_policy.get('spec', {}),
                    'status': sg_policy.get('status', {}),
                    'creation_timestamp': sg_policy.get('metadata', {}).get('creationTimestamp'),
                    'full_config': sg_policy
                }
                self.export_data['resources']['security_group_policies'].append(sg_info)
                
        except Exception as e:
            self.logger.warning(f"SecurityGroupPolicies not found or accessible: {e}")
            self.export_data['resources']['security_group_policies'] = []
    
    def export_cni_nodes(self):
        """Export AWS VPC CNI CNINodes."""
        try:
            cni_nodes = self.k8s_client['custom_objects'].list_cluster_custom_object(
                group='vpcresources.k8s.aws',
                version='v1alpha1',
                plural='cninodes'
            )
            self.export_data['resources']['cni_nodes'] = []
            
            for cni_node in cni_nodes.get('items', []):
                cni_info = {
                    'name': cni_node.get('metadata', {}).get('name', 'N/A'),
                    'labels': cni_node.get('metadata', {}).get('labels', {}),
                    'annotations': cni_node.get('metadata', {}).get('annotations', {}),
                    'spec': cni_node.get('spec', {}),
                    'status': cni_node.get('status', {}),
                    'creation_timestamp': cni_node.get('metadata', {}).get('creationTimestamp'),
                    'full_config': cni_node
                }
                self.export_data['resources']['cni_nodes'].append(cni_info)
                
        except Exception as e:
            self.logger.warning(f"CNINodes not found or accessible: {e}")
            self.export_data['resources']['cni_nodes'] = []
    
    def export_network_attachment_definitions(self):
        """Export Multus CNI NetworkAttachmentDefinitions."""
        try:
            net_attachments = self.k8s_client['custom_objects'].list_namespaced_custom_object(
                group='k8s.cni.cncf.io',
                version='v1',
                namespace='',
                plural='network-attachment-definitions'
            )
            self.export_data['resources']['network_attachment_definitions'] = []
            
            for net_attach in net_attachments.get('items', []):
                net_info = {
                    'name': net_attach.get('metadata', {}).get('name', 'N/A'),
                    'namespace': net_attach.get('metadata', {}).get('namespace', 'N/A'),
                    'labels': net_attach.get('metadata', {}).get('labels', {}),
                    'annotations': net_attach.get('metadata', {}).get('annotations', {}),
                    'spec': net_attach.get('spec', {}),
                    'creation_timestamp': net_attach.get('metadata', {}).get('creationTimestamp'),
                    'full_config': net_attach
                }
                self.export_data['resources']['network_attachment_definitions'].append(net_info)
                
        except Exception as e:
            self.logger.warning(f"NetworkAttachmentDefinitions not found or accessible: {e}")
            self.export_data['resources']['network_attachment_definitions'] = []
    
    def export_karpenter_nodepools(self):
        """Export Karpenter NodePools."""
        try:
            nodepools = self.k8s_client['custom_objects'].list_cluster_custom_object(
                group='karpenter.sh',
                version='v1',
                plural='nodepools'
            )
            self.export_data['resources']['karpenter_nodepools'] = []
            
            for nodepool in nodepools.get('items', []):
                np_info = {
                    'name': nodepool.get('metadata', {}).get('name', 'N/A'),
                    'labels': nodepool.get('metadata', {}).get('labels', {}),
                    'annotations': nodepool.get('metadata', {}).get('annotations', {}),
                    'spec': nodepool.get('spec', {}),
                    'status': nodepool.get('status', {}),
                    'creation_timestamp': nodepool.get('metadata', {}).get('creationTimestamp'),
                    'full_config': nodepool
                }
                self.export_data['resources']['karpenter_nodepools'].append(np_info)
                
        except Exception as e:
            self.logger.warning(f"Karpenter NodePools not found or accessible: {e}")
            self.export_data['resources']['karpenter_nodepools'] = []
    
    def export_karpenter_nodeclasses(self):
        """Export Karpenter NodeClasses."""
        try:
            # Try EKS-specific NodeClasses first
            try:
                nodeclasses = self.k8s_client['custom_objects'].list_cluster_custom_object(
                    group='karpenter.k8s.aws',
                    version='v1',
                    plural='ec2nodeclasses'
                )
                nodeclass_type = 'ec2nodeclasses'
            except:
                # Fallback to generic NodeClasses
                nodeclasses = self.k8s_client['custom_objects'].list_cluster_custom_object(
                    group='karpenter.sh',
                    version='v1',
                    plural='nodeclasses'
                )
                nodeclass_type = 'nodeclasses'
            
            self.export_data['resources']['karpenter_nodeclasses'] = []
            
            for nodeclass in nodeclasses.get('items', []):
                nc_info = {
                    'name': nodeclass.get('metadata', {}).get('name', 'N/A'),
                    'type': nodeclass_type,
                    'labels': nodeclass.get('metadata', {}).get('labels', {}),
                    'annotations': nodeclass.get('metadata', {}).get('annotations', {}),
                    'spec': nodeclass.get('spec', {}),
                    'status': nodeclass.get('status', {}),
                    'creation_timestamp': nodeclass.get('metadata', {}).get('creationTimestamp'),
                    'full_config': nodeclass
                }
                self.export_data['resources']['karpenter_nodeclasses'].append(nc_info)
                
        except Exception as e:
            self.logger.warning(f"Karpenter NodeClasses not found or accessible: {e}")
            self.export_data['resources']['karpenter_nodeclasses'] = []
    
    def export_karpenter_nodeclaims(self):
        """Export Karpenter NodeClaims."""
        try:
            nodeclaims = self.k8s_client['custom_objects'].list_cluster_custom_object(
                group='karpenter.sh',
                version='v1',
                plural='nodeclaims'
            )
            self.export_data['resources']['karpenter_nodeclaims'] = []
            
            for nodeclaim in nodeclaims.get('items', []):
                nc_info = {
                    'name': nodeclaim.get('metadata', {}).get('name', 'N/A'),
                    'labels': nodeclaim.get('metadata', {}).get('labels', {}),
                    'annotations': nodeclaim.get('metadata', {}).get('annotations', {}),
                    'spec': nodeclaim.get('spec', {}),
                    'status': nodeclaim.get('status', {}),
                    'creation_timestamp': nodeclaim.get('metadata', {}).get('creationTimestamp'),
                    'full_config': nodeclaim
                }
                self.export_data['resources']['karpenter_nodeclaims'].append(nc_info)
                
        except Exception as e:
            self.logger.warning(f"Karpenter NodeClaims not found or accessible: {e}")
            self.export_data['resources']['karpenter_nodeclaims'] = []
    
    def export_custom_resources_dynamically(self):
        """Dynamically discover and export custom resources."""
        try:
            crds = self.k8s_client['apiextensions_v1'].list_custom_resource_definition()
            
            # Track additional custom resources not covered by specific methods
            aws_resources = ['eniconfigs', 'securitygrouppolicies', 'cninodes']
            cni_resources = ['network-attachment-definitions']
            karpenter_resources = ['nodepools', 'ec2nodeclasses', 'nodeclasses', 'nodeclaims']
            covered_resources = aws_resources + cni_resources + karpenter_resources
            
            self.export_data['resources']['additional_custom_resources'] = {}
            
            for crd in crds.items:
                plural_name = crd.spec.names.plural
                group = crd.spec.group
                
                # Skip resources we already handle specifically
                if plural_name in covered_resources:
                    continue
                    
                # Export instances of this CRD
                try:
                    for version in crd.spec.versions:
                        if version.served:
                            if crd.spec.scope == 'Namespaced':
                                custom_resources = self.k8s_client['custom_objects'].list_namespaced_custom_object(
                                    group=group,
                                    version=version.name,
                                    namespace='',
                                    plural=plural_name
                                )
                            else:
                                custom_resources = self.k8s_client['custom_objects'].list_cluster_custom_object(
                                    group=group,
                                    version=version.name,
                                    plural=plural_name
                                )
                            
                            resource_key = f"{group}_{plural_name}"
                            self.export_data['resources']['additional_custom_resources'][resource_key] = {
                                'crd_info': {
                                    'group': group,
                                    'version': version.name,
                                    'plural': plural_name,
                                    'scope': crd.spec.scope
                                },
                                'instances': custom_resources.get('items', [])
                            }
                            break  # Use first served version
                except Exception as e:
                    self.logger.debug(f"Could not export custom resource {plural_name}: {e}")
                    
        except Exception as e:
            self.logger.warning(f"Failed to dynamically discover custom resources: {e}")
            self.export_data['resources']['additional_custom_resources'] = {}
    
    def export_mutating_webhook_configurations(self):
        """Export all mutating webhook configurations."""
        try:
            mwcs = self.k8s_client['admissionregistration_v1'].list_mutating_webhook_configuration()
            self.export_data['resources']['mutating_webhook_configurations'] = []
            
            for mwc in mwcs.items:
                mwc_info = {
                    'name': mwc.metadata.name,
                    'labels': mwc.metadata.labels or {},
                    'annotations': mwc.metadata.annotations or {},
                    'webhooks': len(mwc.webhooks or []),
                    'creation_timestamp': mwc.metadata.creation_timestamp.isoformat() if mwc.metadata.creation_timestamp else None
                }
                
                self.export_data['resources']['mutating_webhook_configurations'].append(mwc_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export mutating webhook configurations: {e}")
    
    def export_validating_webhook_configurations(self):
        """Export all validating webhook configurations."""
        try:
            vwcs = self.k8s_client['admissionregistration_v1'].list_validating_webhook_configuration()
            self.export_data['resources']['validating_webhook_configurations'] = []
            
            for vwc in vwcs.items:
                vwc_info = {
                    'name': vwc.metadata.name,
                    'labels': vwc.metadata.labels or {},
                    'annotations': vwc.metadata.annotations or {},
                    'webhooks': len(vwc.webhooks or []),
                    'creation_timestamp': vwc.metadata.creation_timestamp.isoformat() if vwc.metadata.creation_timestamp else None
                }
                
                self.export_data['resources']['validating_webhook_configurations'].append(vwc_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export validating webhook configurations: {e}")
    
    def export_api_services(self):
        """Export all API services."""
        try:
            api_services = self.k8s_client['apiregistration_v1'].list_api_service()
            self.export_data['resources']['api_services'] = []
            
            for api_svc in api_services.items:
                api_svc_info = {
                    'name': api_svc.metadata.name,
                    'labels': api_svc.metadata.labels or {},
                    'annotations': api_svc.metadata.annotations or {},
                    'group': api_svc.spec.group,
                    'version': api_svc.spec.version,
                    'group_priority_minimum': api_svc.spec.group_priority_minimum,
                    'version_priority': api_svc.spec.version_priority,
                    'service': {'name': api_svc.spec.service.name, 'namespace': api_svc.spec.service.namespace} if api_svc.spec.service else None,
                    'creation_timestamp': api_svc.metadata.creation_timestamp.isoformat() if api_svc.metadata.creation_timestamp else None
                }
                
                self.export_data['resources']['api_services'].append(api_svc_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export API services: {e}")
    
    def export_leases(self):
        """Export all leases."""
        try:
            leases = self.k8s_client['coordination_v1'].list_lease_for_all_namespaces()
            self.export_data['resources']['leases'] = []
            
            for lease in leases.items:
                lease_info = {
                    'name': lease.metadata.name,
                    'namespace': lease.metadata.namespace,
                    'labels': lease.metadata.labels or {},
                    'annotations': lease.metadata.annotations or {},
                    'holder_identity': lease.spec.holder_identity if lease.spec else None,
                    'lease_duration_seconds': lease.spec.lease_duration_seconds if lease.spec else None,
                    'acquire_time': lease.spec.acquire_time.isoformat() if lease.spec and lease.spec.acquire_time else None,
                    'renew_time': lease.spec.renew_time.isoformat() if lease.spec and lease.spec.renew_time else None,
                    'creation_timestamp': lease.metadata.creation_timestamp.isoformat() if lease.metadata.creation_timestamp else None
                }
                
                self.export_data['resources']['leases'].append(lease_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export leases: {e}")
    
    def export_runtime_classes(self):
        """Export all runtime classes."""
        try:
            if 'node_v1' not in self.k8s_client or self.k8s_client['node_v1'] is None:
                self.logger.warning("NodeV1Api not available, skipping runtime classes export")
                self.export_data['resources']['runtime_classes'] = []
                return
                
            runtime_classes = self.k8s_client['node_v1'].list_runtime_class()
            self.export_data['resources']['runtime_classes'] = []
            
            for rc in runtime_classes.items:
                rc_info = {
                    'name': rc.metadata.name,
                    'labels': rc.metadata.labels or {},
                    'annotations': rc.metadata.annotations or {},
                    'handler': rc.handler,
                    'overhead': rc.overhead.to_dict() if rc.overhead else None,
                    'scheduling': rc.scheduling.to_dict() if rc.scheduling else None,
                    'creation_timestamp': rc.metadata.creation_timestamp.isoformat() if rc.metadata.creation_timestamp else None
                }
                
                self.export_data['resources']['runtime_classes'].append(rc_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export runtime classes: {e}")
    
    def export_flow_schemas(self):
        """Export all flow schemas."""
        try:
            if self.k8s_client.get('flowcontrol_v1beta3') is None:
                self.logger.warning("FlowcontrolV1beta3Api not available, skipping flow schemas export")
                self.export_data['resources']['flow_schemas'] = []
                return
                
            flow_schemas = self.k8s_client['flowcontrol_v1beta3'].list_flow_schema()
            self.export_data['resources']['flow_schemas'] = []
            
            for fs in flow_schemas.items:
                fs_info = {
                    'name': fs.metadata.name,
                    'labels': fs.metadata.labels or {},
                    'annotations': fs.metadata.annotations or {},
                    'priority_level_configuration': fs.spec.priority_level_configuration.name if fs.spec.priority_level_configuration else None,
                    'matching_precedence': fs.spec.matching_precedence,
                    'distinguisher_method': fs.spec.distinguisher_method.to_dict() if fs.spec.distinguisher_method else None,
                    'rules': len(fs.spec.rules or []),
                    'creation_timestamp': fs.metadata.creation_timestamp.isoformat() if fs.metadata.creation_timestamp else None
                }
                
                self.export_data['resources']['flow_schemas'].append(fs_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export flow schemas: {e}")
    
    def export_priority_level_configurations(self):
        """Export all priority level configurations."""
        try:
            if self.k8s_client.get('flowcontrol_v1beta3') is None:
                self.logger.warning("FlowcontrolV1beta3Api not available, skipping priority level configurations export")
                self.export_data['resources']['priority_level_configurations'] = []
                return
                
            plcs = self.k8s_client['flowcontrol_v1beta3'].list_priority_level_configuration()
            self.export_data['resources']['priority_level_configurations'] = []
            
            for plc in plcs.items:
                plc_info = {
                    'name': plc.metadata.name,
                    'labels': plc.metadata.labels or {},
                    'annotations': plc.metadata.annotations or {},
                    'type': plc.spec.type,
                    'limited': plc.spec.limited.to_dict() if plc.spec.limited else None,
                    'creation_timestamp': plc.metadata.creation_timestamp.isoformat() if plc.metadata.creation_timestamp else None
                }
                
                self.export_data['resources']['priority_level_configurations'].append(plc_info)
                
        except ApiException as e:
            self.logger.error(f"Failed to export priority level configurations: {e}")
    
    def export_all_resources(self):
        """Export all Kubernetes resources."""
        self.logger.info("Starting comprehensive resource export...")
        
        # EKS metadata first, outside the tolerant loop: a failure here means the
        # name/region/credentials are wrong and continuing would be misleading.
        self.export_cluster_info()

        export_methods = [
            ('namespaces', self.export_namespaces),
            ('nodes', self.export_nodes),
            ('pods', self.export_pods),
            ('services', self.export_services),
            ('deployments', self.export_deployments),
            ('replicasets', self.export_replicasets),
            ('statefulsets', self.export_statefulsets),
            ('daemonsets', self.export_daemonsets),
            ('jobs', self.export_jobs),
            ('cronjobs', self.export_cronjobs),
            ('pod_templates', self.export_pod_templates),
            ('priority_classes', self.export_priority_classes),
            ('horizontal_pod_autoscalers', self.export_horizontal_pod_autoscalers),
            ('endpoints', self.export_endpoints),
            ('endpoint_slices', self.export_endpoint_slices),
            ('ingresses', self.export_ingresses),
            ('ingress_classes', self.export_ingress_classes),
            ('configmaps', self.export_configmaps),
            ('secrets', self.export_secrets),
            ('persistent_volumes', self.export_persistent_volumes),
            ('persistent_volume_claims', self.export_persistent_volume_claims),
            ('storage_classes', self.export_storage_classes),
            ('volume_attachments', self.export_volume_attachments),
            ('csi_drivers', self.export_csi_drivers),
            ('csi_nodes', self.export_csi_nodes),
            ('csi_storage_capacities', self.export_csi_storage_capacities),
            ('service_accounts', self.export_service_accounts),
            ('cluster_roles', self.export_cluster_roles),
            ('cluster_role_bindings', self.export_cluster_role_bindings),
            ('roles', self.export_roles),
            ('role_bindings', self.export_role_bindings),
            ('limit_ranges', self.export_limit_ranges),
            ('resource_quotas', self.export_resource_quotas),
            ('network_policies', self.export_network_policies),
            ('pod_disruption_budgets', self.export_pod_disruption_budgets),
            ('custom_resource_definitions', self.export_custom_resource_definitions),
            ('eni_configs', self.export_eni_configs),
            ('security_group_policies', self.export_security_group_policies),
            ('cni_nodes', self.export_cni_nodes),
            ('network_attachment_definitions', self.export_network_attachment_definitions),
            ('karpenter_nodepools', self.export_karpenter_nodepools),
            ('karpenter_nodeclasses', self.export_karpenter_nodeclasses),
            ('karpenter_nodeclaims', self.export_karpenter_nodeclaims),
            ('additional_custom_resources', self.export_custom_resources_dynamically),
            ('mutating_webhook_configurations', self.export_mutating_webhook_configurations),
            ('validating_webhook_configurations', self.export_validating_webhook_configurations),
            ('api_services', self.export_api_services),
            ('leases', self.export_leases),
            ('runtime_classes', self.export_runtime_classes),
            ('flow_schemas', self.export_flow_schemas),
            ('priority_level_configurations', self.export_priority_level_configurations)
        ]
        
        for resource_name, export_method in export_methods:
            try:
                self.logger.info(f"Exporting {resource_name}...")
                export_method()
                count = len(self.export_data['resources'].get(resource_name, []))
                self.logger.info(f"Exported {count} {resource_name}")
            except Exception as e:
                self.logger.error(f"Failed to export {resource_name}: {e}")
        
        self.resolve_pending_describes()
        if self.rate_limiter.waited_total > 0:
            self.logger.info(f"Rate limiter throttled for {self.rate_limiter.waited_total:.1f}s in total")
        self.logger.info("Resource export completed")
    
    def save_to_file(self, output_file: str, format: str = 'json'):
        """Save exported data to file."""
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        if format.lower() == 'json':
            with open(output_file, 'w') as f:
                json.dump(self.export_data, f, indent=2, default=str)
        elif format.lower() == 'yaml':
            with open(output_file, 'w') as f:
                yaml.dump(self.export_data, f, default_flow_style=False)
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        self.logger.info(f"Export saved to {output_file}")
    
    def save_kubernetes_yaml_resources(self, output_dir: str):
        """Save resources as individual Kubernetes YAML files for restoration."""
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Create directory structure
        dirs = {
            'infrastructure': output_path / 'infrastructure',
            'networking': output_path / 'networking', 
            'workloads': output_path / 'workloads',
            'storage': output_path / 'storage',
            'rbac': output_path / 'rbac',
            'config': output_path / 'config',
            'custom-resources': output_path / 'custom-resources'
        }
        
        for dir_path in dirs.values():
            dir_path.mkdir(exist_ok=True)
        
        resources = self.export_data.get('resources', {})
        
        # Infrastructure resources
        self._save_yaml_resource_type(dirs['infrastructure'] / 'nodes.yaml', 'nodes', resources)
        self._save_yaml_resource_type(dirs['infrastructure'] / 'karpenter-nodepools.yaml', 'karpenter_nodepools', resources)
        self._save_yaml_resource_type(dirs['infrastructure'] / 'karpenter-nodeclasses.yaml', 'karpenter_nodeclasses', resources)
        self._save_yaml_resource_type(dirs['infrastructure'] / 'karpenter-nodeclaims.yaml', 'karpenter_nodeclaims', resources)
        
        # Networking resources
        self._save_yaml_resource_type(dirs['networking'] / 'services.yaml', 'services', resources)
        self._save_yaml_resource_type(dirs['networking'] / 'ingresses.yaml', 'ingresses', resources)
        self._save_yaml_resource_type(dirs['networking'] / 'network-policies.yaml', 'network_policies', resources)
        self._save_yaml_resource_type(dirs['networking'] / 'network-attachment-definitions.yaml', 'network_attachment_definitions', resources)
        self._save_yaml_resource_type(dirs['networking'] / 'eniconfigs.yaml', 'eni_configs', resources)
        self._save_yaml_resource_type(dirs['networking'] / 'security-group-policies.yaml', 'security_group_policies', resources)
        self._save_yaml_resource_type(dirs['networking'] / 'cni-nodes.yaml', 'cni_nodes', resources)
        
        # Workload resources
        self._save_yaml_resource_type(dirs['workloads'] / 'deployments.yaml', 'deployments', resources)
        self._save_yaml_resource_type(dirs['workloads'] / 'daemonsets.yaml', 'daemonsets', resources)
        self._save_yaml_resource_type(dirs['workloads'] / 'statefulsets.yaml', 'statefulsets', resources)
        self._save_yaml_resource_type(dirs['workloads'] / 'jobs.yaml', 'jobs', resources)
        self._save_yaml_resource_type(dirs['workloads'] / 'cronjobs.yaml', 'cronjobs', resources)
        self._save_yaml_resource_type(dirs['workloads'] / 'pods.yaml', 'pods', resources)
        
        # Storage resources
        self._save_yaml_resource_type(dirs['storage'] / 'persistent-volumes.yaml', 'persistent_volumes', resources)
        self._save_yaml_resource_type(dirs['storage'] / 'persistent-volume-claims.yaml', 'persistent_volume_claims', resources)
        self._save_yaml_resource_type(dirs['storage'] / 'storage-classes.yaml', 'storage_classes', resources)
        
        # RBAC resources
        self._save_yaml_resource_type(dirs['rbac'] / 'service-accounts.yaml', 'service_accounts', resources)
        self._save_yaml_resource_type(dirs['rbac'] / 'cluster-roles.yaml', 'cluster_roles', resources)
        self._save_yaml_resource_type(dirs['rbac'] / 'cluster-role-bindings.yaml', 'cluster_role_bindings', resources)
        self._save_yaml_resource_type(dirs['rbac'] / 'roles.yaml', 'roles', resources)
        self._save_yaml_resource_type(dirs['rbac'] / 'role-bindings.yaml', 'role_bindings', resources)
        
        # Config resources
        self._save_yaml_resource_type(dirs['config'] / 'namespaces.yaml', 'namespaces', resources)
        self._save_yaml_resource_type(dirs['config'] / 'configmaps.yaml', 'configmaps', resources)
        self._save_yaml_resource_type(dirs['config'] / 'secrets.yaml', 'secrets', resources)
        
        # Custom resources
        self._save_yaml_resource_type(dirs['custom-resources'] / 'custom-resource-definitions.yaml', 'custom_resource_definitions', resources)
        self._save_additional_custom_resources(dirs['custom-resources'], resources)
        
        # Generate restoration guide
        self._generate_restoration_guide(output_path / 'restore-guide.md')
        
        # Generate restoration script
        self._generate_restoration_script(output_path / 'restore-cluster.sh')
        
        self.logger.info(f"Kubernetes YAML resources saved to {output_dir}")
    
    def _save_yaml_resource_type(self, file_path: Path, resource_type: str, resources: dict):
        """Save a specific resource type as YAML."""
        resource_list = resources.get(resource_type, [])
        
        if not resource_list:
            return
            
        yaml_docs = []
        
        for resource in resource_list:
            # Convert to Kubernetes API format
            if 'full_config' in resource:
                yaml_docs.append(resource['full_config'])
            else:
                # Reconstruct basic Kubernetes resource format
                k8s_resource = self._convert_to_kubernetes_format(resource, resource_type)
                if k8s_resource:
                    yaml_docs.append(k8s_resource)
        
        if yaml_docs:
            with open(file_path, 'w') as f:
                for i, doc in enumerate(yaml_docs):
                    if i > 0:
                        f.write('---\n')
                    yaml.dump(doc, f, default_flow_style=False)
    
    def _save_additional_custom_resources(self, custom_dir: Path, resources: dict):
        """Save additional custom resources."""
        additional_crs = resources.get('additional_custom_resources', {})
        
        for resource_key, resource_data in additional_crs.items():
            instances = resource_data.get('instances', [])
            if instances:
                file_path = custom_dir / f"{resource_key}.yaml"
                with open(file_path, 'w') as f:
                    for i, instance in enumerate(instances):
                        if i > 0:
                            f.write('---\n')
                        yaml.dump(instance, f, default_flow_style=False)
    
    def _convert_to_kubernetes_format(self, resource: dict, resource_type: str) -> dict:
        """Convert exported resource back to Kubernetes API format."""
        # This is a simplified conversion - in a real implementation,
        # you'd need more sophisticated mapping for each resource type
        
        api_versions = {
            'pods': 'v1',
            'services': 'v1', 
            'deployments': 'apps/v1',
            'daemonsets': 'apps/v1',
            'statefulsets': 'apps/v1',
            'configmaps': 'v1',
            'secrets': 'v1',
            'namespaces': 'v1',
            'nodes': 'v1',
            'persistent_volumes': 'v1',
            'persistent_volume_claims': 'v1',
            'storage_classes': 'storage.k8s.io/v1',
            'service_accounts': 'v1',
            'cluster_roles': 'rbac.authorization.k8s.io/v1',
            'cluster_role_bindings': 'rbac.authorization.k8s.io/v1',
            'roles': 'rbac.authorization.k8s.io/v1',
            'role_bindings': 'rbac.authorization.k8s.io/v1',
        }
        
        kinds = {
            'pods': 'Pod',
            'services': 'Service',
            'deployments': 'Deployment',
            'daemonsets': 'DaemonSet',
            'statefulsets': 'StatefulSet',
            'configmaps': 'ConfigMap',
            'secrets': 'Secret',
            'namespaces': 'Namespace',
            'nodes': 'Node',
            'persistent_volumes': 'PersistentVolume',
            'persistent_volume_claims': 'PersistentVolumeClaim',
            'storage_classes': 'StorageClass',
            'service_accounts': 'ServiceAccount',
            'cluster_roles': 'ClusterRole',
            'cluster_role_bindings': 'ClusterRoleBinding',
            'roles': 'Role',
            'role_bindings': 'RoleBinding',
        }
        
        if resource_type not in api_versions:
            return None
            
        k8s_resource = {
            'apiVersion': api_versions[resource_type],
            'kind': kinds[resource_type],
            'metadata': {
                'name': resource.get('name', ''),
                'labels': resource.get('labels', {}),
                'annotations': resource.get('annotations', {})
            }
        }
        
        if 'namespace' in resource and resource['namespace']:
            k8s_resource['metadata']['namespace'] = resource['namespace']
            
        return k8s_resource
    
    def _generate_restoration_guide(self, file_path: Path):
        """Generate a restoration guide."""
        guide_content = f"""
# EKS Cluster Restoration Guide

## Overview
This guide provides step-by-step instructions to restore your EKS cluster configuration from the exported YAML files.

## Prerequisites
- kubectl configured for target cluster
- Appropriate RBAC permissions
- Custom Resource Definitions (CRDs) installed if needed

## Restoration Order

### 1. Infrastructure Resources
```bash
# Apply CRDs first
kubectl apply -f custom-resources/custom-resource-definitions.yaml

# Wait for CRDs to be ready
kubectl wait --for condition=established --timeout=60s crd --all

# Apply infrastructure resources
kubectl apply -f infrastructure/
```

### 2. Configuration Resources
```bash
# Apply namespaces first
kubectl apply -f config/namespaces.yaml

# Apply other config resources
kubectl apply -f config/configmaps.yaml
kubectl apply -f config/secrets.yaml
```

### 3. RBAC Resources
```bash
kubectl apply -f rbac/
```

### 4. Storage Resources
```bash
kubectl apply -f storage/
```

### 5. Networking Resources
```bash
# Apply ENI configs and networking policies
kubectl apply -f networking/eniconfigs.yaml
kubectl apply -f networking/network-attachment-definitions.yaml
kubectl apply -f networking/cni-nodes.yaml
kubectl apply -f networking/security-group-policies.yaml

# Apply services and ingresses
kubectl apply -f networking/services.yaml
kubectl apply -f networking/ingresses.yaml
kubectl apply -f networking/network-policies.yaml
```

### 6. Workload Resources
```bash
# Apply workloads (order matters for some resources)
kubectl apply -f workloads/deployments.yaml
kubectl apply -f workloads/daemonsets.yaml
kubectl apply -f workloads/statefulsets.yaml
kubectl apply -f workloads/jobs.yaml
kubectl apply -f workloads/cronjobs.yaml
```

### 7. Custom Resources
```bash
# Apply additional custom resources
kubectl apply -f custom-resources/
```

## Verification

### Check cluster status
```bash
kubectl get nodes
kubectl get pods --all-namespaces
kubectl get services --all-namespaces
```

### Verify custom resources
```bash
# Check ENIConfigs
kubectl get eniconfigs

# Check NetworkAttachmentDefinitions 
kubectl get network-attachment-definitions --all-namespaces

# Check Karpenter resources
kubectl get nodepools
kubectl get ec2nodeclasses
```

### Check multi-interface pod configurations
```bash
# Verify pods with network annotations
kubectl get pods -o jsonpath='{{range .items[*]}}{{.metadata.name}}{{"\\t"}}{{.metadata.annotations.k8s\\.v1\\.cni\\.cncf\\.io/networks}}{{"\\n"}}{{end}}'
```

## Troubleshooting

- If CRDs fail to apply, check if they already exist
- If pods fail to start, check for missing dependencies
- For networking issues, verify CNI plugins are installed
- Check AWS VPC CNI and Multus CNI configurations

## Generated on
{self.export_data['metadata']['export_timestamp']}
Cluster: {self.cluster_name}
Region: {self.region}
"""
        
        with open(file_path, 'w') as f:
            f.write(guide_content)
    
    def _generate_restoration_script(self, file_path: Path):
        """Generate an automated restoration script."""
        script_content = '''
#!/bin/bash

# EKS Cluster Restoration Script
# This script automatically applies resources in the correct order

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

print_info() { echo -e "${YELLOW}[INFO]${NC} $1"; }
print_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Parse command line arguments
DRY_RUN=false
VALIDATE=false
NAMESPACE_FILTER=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --validate-dependencies)
            VALIDATE=true
            shift
            ;;
        --namespace)
            NAMESPACE_FILTER="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --dry-run                 Show what would be applied without actually applying"
            echo "  --validate-dependencies   Validate prerequisites before applying"
            echo "  --namespace NAMESPACE     Only apply resources in specified namespace"
            echo "  -h, --help               Show this help message"
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Function to apply resources
apply_resources() {
    local file=$1
    local description=$2
    
    if [ ! -f "$file" ]; then
        print_info "$file not found, skipping $description"
        return
    fi
    
    print_info "Applying $description..."
    
    if [ "$DRY_RUN" = true ]; then
        kubectl apply -f "$file" --dry-run=client
    else
        kubectl apply -f "$file"
    fi
    
    print_success "$description applied successfully"
}

# Function to wait for resources
wait_for_resources() {
    local resource_type=$1
    local timeout=${2:-60}
    
    print_info "Waiting for $resource_type to be ready..."
    if kubectl wait --for=condition=established --timeout="${timeout}s" "$resource_type" --all 2>/dev/null; then
        print_success "$resource_type are ready"
    else
        print_error "Timeout waiting for $resource_type"
    fi
}

# Validation
if [ "$VALIDATE" = true ]; then
    print_info "Validating prerequisites..."
    
    if ! command -v kubectl &> /dev/null; then
        print_error "kubectl is required but not installed"
        exit 1
    fi
    
    if ! kubectl cluster-info &> /dev/null; then
        print_error "Cannot connect to Kubernetes cluster"
        exit 1
    fi
    
    print_success "Prerequisites validated"
fi

print_info "Starting EKS cluster restoration..."

# Step 1: Apply CRDs
apply_resources "custom-resources/custom-resource-definitions.yaml" "Custom Resource Definitions"
wait_for_resources "crd"

# Step 2: Apply namespaces
apply_resources "config/namespaces.yaml" "Namespaces"

# Step 3: Apply infrastructure
apply_resources "infrastructure/karpenter-nodeclasses.yaml" "Karpenter NodeClasses" 
apply_resources "infrastructure/karpenter-nodepools.yaml" "Karpenter NodePools"
apply_resources "infrastructure/karpenter-nodeclaims.yaml" "Karpenter NodeClaims"

# Step 4: Apply RBAC
apply_resources "rbac/service-accounts.yaml" "Service Accounts"
apply_resources "rbac/cluster-roles.yaml" "Cluster Roles"
apply_resources "rbac/cluster-role-bindings.yaml" "Cluster Role Bindings"
apply_resources "rbac/roles.yaml" "Roles"
apply_resources "rbac/role-bindings.yaml" "Role Bindings"

# Step 5: Apply config
apply_resources "config/configmaps.yaml" "ConfigMaps"
apply_resources "config/secrets.yaml" "Secrets"

# Step 6: Apply storage
apply_resources "storage/storage-classes.yaml" "Storage Classes"
apply_resources "storage/persistent-volumes.yaml" "Persistent Volumes"
apply_resources "storage/persistent-volume-claims.yaml" "Persistent Volume Claims"

# Step 7: Apply networking (critical for multi-interface pods)
apply_resources "networking/eniconfigs.yaml" "ENI Configs"
apply_resources "networking/network-attachment-definitions.yaml" "Network Attachment Definitions"
apply_resources "networking/cni-nodes.yaml" "CNI Nodes"
apply_resources "networking/security-group-policies.yaml" "Security Group Policies"
apply_resources "networking/services.yaml" "Services"
apply_resources "networking/ingresses.yaml" "Ingresses"
apply_resources "networking/network-policies.yaml" "Network Policies"

# Step 8: Apply workloads
apply_resources "workloads/deployments.yaml" "Deployments"
apply_resources "workloads/daemonsets.yaml" "DaemonSets"
apply_resources "workloads/statefulsets.yaml" "StatefulSets"
apply_resources "workloads/jobs.yaml" "Jobs"
apply_resources "workloads/cronjobs.yaml" "CronJobs"

# Step 9: Apply additional custom resources
for cr_file in custom-resources/*.yaml; do
    if [ "$cr_file" != "custom-resources/custom-resource-definitions.yaml" ] && [ -f "$cr_file" ]; then
        basename_file=$(basename "$cr_file" .yaml)
        apply_resources "$cr_file" "Custom Resource: $basename_file"
    fi
done

print_success "EKS cluster restoration completed!"

if [ "$DRY_RUN" = false ]; then
    print_info "Verifying cluster status..."
    kubectl get nodes
    kubectl get pods --all-namespaces | head -10
    print_info "Run 'kubectl get pods --all-namespaces' to see all pods"
fi
'''
        
        with open(file_path, 'w') as f:
            f.write(script_content)
        
        # Make script executable
        os.chmod(file_path, 0o755)
    
    def generate_summary_report(self) -> str:
        """Generate a summary report of the exported resources."""
        report = []
        report.append(f"EKS Cluster Export Summary")
        report.append(f"=" * 50)
        report.append(f"Cluster: {self.cluster_name}")
        report.append(f"Region: {self.region}")
        report.append(f"Export Time: {self.export_data['metadata']['export_timestamp']}")
        report.append("")
        
        # Resource counts
        report.append("Resource Summary:")
        report.append("-" * 20)
        for resource_type, resources in self.export_data.get('resources', {}).items():
            if isinstance(resources, list):
                count = len(resources)
                if count > 0:  # Only show resources that exist
                    # Format resource type names nicely
                    formatted_name = resource_type.replace('_', ' ').title()
                    if resource_type == 'eni_configs':
                        formatted_name = 'ENI Configs'
                    elif resource_type == 'network_attachment_definitions':
                        formatted_name = 'Network Attachment Definitions'
                    elif resource_type == 'security_group_policies':
                        formatted_name = 'Security Group Policies'
                    elif resource_type == 'cni_nodes':
                        formatted_name = 'CNI Nodes'
                    elif resource_type.startswith('karpenter_'):
                        formatted_name = 'Karpenter ' + resource_type.replace('karpenter_', '').replace('_', ' ').title()
                    
                    report.append(f"{formatted_name}: {count}")
            elif isinstance(resources, dict) and resource_type == 'additional_custom_resources':
                total_instances = sum(len(data.get('instances', [])) for data in resources.values())
                if total_instances > 0:
                    report.append(f"Additional Custom Resources: {len(resources)} types, {total_instances} instances")
        
        report.append("")
        
        # Namespace breakdown
        namespaces = self.export_data.get('resources', {}).get('namespaces', [])
        if namespaces:
            report.append("Namespaces:")
            report.append("-" * 15)
            for ns in namespaces:
                report.append(f"  - {ns['name']} (Status: {ns['status']})")
        
        report.append("")
        
        # Node information
        nodes = self.export_data.get('resources', {}).get('nodes', [])
        if nodes:
            report.append("Nodes:")
            report.append("-" * 10)
            for node in nodes:
                report.append(f"  - {node['name']}")
                if 'capacity' in node:
                    cpu = node['capacity'].get('cpu', 'N/A')
                    memory = node['capacity'].get('memory', 'N/A')
                    report.append(f"    CPU: {cpu}, Memory: {memory}")
        
        return "\n".join(report)


def main():
    parser = argparse.ArgumentParser(
        description='Export EKS cluster configuration with support for AWS-specific and CNI resources',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic export with all enhancements
  %(prog)s my-cluster --include-aws-resources --include-custom-crds
  
  # Export for restoration with YAML format
  %(prog)s my-cluster --output-format yaml --split-by-type --generate-restore-scripts
  
  # Export with filtering
  %(prog)s my-cluster --exclude-secrets-data --namespace-filter=default,kube-system
"""
    )
    
    # Required arguments
    parser.add_argument('cluster_name', nargs='?', default=None,
                       help='EKS cluster name (default: derived from the kubeconfig context)')
    
    # Optional arguments
    parser.add_argument('--region', default=None,
                       help='AWS region (default: derived from the kubeconfig context, then AWS config)')
    parser.add_argument('--kubeconfig', help='Path to kubeconfig file (default: $KUBECONFIG or ~/.kube/config)',
                       default=None)
    parser.add_argument('--context', help='kubeconfig context to use (default: current-context)',
                       default=None)
    parser.add_argument('--all-contexts', action='store_true',
                       help='Export every context in the kubeconfig (one output per unique cluster; '
                            'output filenames get a -<context> suffix or fill a {context} placeholder)')
    parser.add_argument('--list-contexts', action='store_true',
                       help='List kubeconfig contexts with their derived EKS cluster/region and exit')
    parser.add_argument('--qps', type=float, default=ratelimit.DEFAULT_QPS,
                       help=f'Max Kubernetes API requests per second, shared by API calls and kubectl describe '
                            f'(default: {ratelimit.DEFAULT_QPS:g}; 0 disables)')
    parser.add_argument('--burst', type=int, default=ratelimit.DEFAULT_BURST,
                       help=f'Requests allowed above --qps before throttling (default: {ratelimit.DEFAULT_BURST})')
    parser.add_argument('--describe-workers', type=int, default=DEFAULT_DESCRIBE_WORKERS,
                       help=f'Parallel "kubectl describe" calls (default: {DEFAULT_DESCRIBE_WORKERS}; 1 = sequential)')
    parser.add_argument('--skip-aws', action='store_true',
                       help='Do not call the EKS API (cluster/nodegroup metadata); export Kubernetes resources only')
    parser.add_argument('--output', '-o', help='Output file path', 
                       default='eks-export-{timestamp}.json')
    parser.add_argument('--format', choices=['json', 'yaml'], default='json',
                       help='Output format for main export file')
    parser.add_argument('--summary', action='store_true',
                       help='Generate summary report')
    
    # Enhanced export options
    parser.add_argument('--include-aws-resources', action='store_true',
                       help='Include AWS-specific resources (ENIConfigs, SecurityGroupPolicies, etc.)')
    parser.add_argument('--include-custom-crds', action='store_true', 
                       help='Include custom resource instances from all CRDs')
    parser.add_argument('--output-format', choices=['json', 'yaml', 'both'], default='json',
                       help='Output format(s)')
    parser.add_argument('--split-by-type', action='store_true',
                       help='Generate individual YAML files by resource type for kubectl restore')
    parser.add_argument('--generate-restore-scripts', action='store_true',
                       help='Generate restoration scripts and guides')
    parser.add_argument('--exclude-secrets-data', action='store_true',
                       help='Exclude secret data values (keep only metadata)')
    parser.add_argument('--namespace-filter', 
                       help='Comma-separated list of namespaces to include')
    parser.add_argument('--resource-type', 
                       help='Comma-separated list of resource types to export')
    parser.add_argument('--output-dir', 
                       help='Output directory for split files (default: same as main output file)')
    
    args = parser.parse_args()
    
    # Replace timestamp placeholder
    if '{timestamp}' in args.output:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        args.output = args.output.replace('{timestamp}', timestamp)
    
    # Determine output directory
    output_dir = args.output_dir or (os.path.dirname(args.output) or '.')

    if args.list_contexts:
        sys.exit(list_contexts(args.kubeconfig))

    if args.all_contexts:
        sys.exit(run_all_contexts(args, output_dir))

    try:
        run_export(args, args.context, args.output, output_dir)
    except (ExportError, ValueError, kubeconfig_utils.KubeconfigError) as e:
        print(f"\u274c Export failed: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\u274c Export failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


def list_contexts(kubeconfig: str) -> int:
    """Print each context with its derived EKS identity. Returns an exit code."""
    try:
        cfg = kubeconfig_utils.load_kubeconfig(kubeconfig)
    except kubeconfig_utils.KubeconfigError as e:
        print(f"\u274c {e}", file=sys.stderr)
        return 1
    current = kubeconfig_utils.current_context_name(cfg)
    print(f"{'':2}{'CONTEXT':<70} {'EKS CLUSTER':<40} {'REGION':<16} ACCOUNT")
    for name in kubeconfig_utils.list_context_names(cfg):
        try:
            ident = kubeconfig_utils.eks_identity_for_context(cfg, name)
        except kubeconfig_utils.KubeconfigError as e:
            print(f"{'*' if name == current else ' ':2}{name:<70} <error: {e}>")
            continue
        mark = '*' if name == current else ' '
        if ident:
            print(f"{mark:2}{name:<70} {ident.cluster_name:<40} {ident.region or '-':<16} {ident.account or '-'}")
        else:
            print(f"{mark:2}{name:<70} {'(not an EKS cluster)':<40} {'-':<16} -")
    return 0


def safe_context_slug(context: str) -> str:
    """Filesystem-safe form of a context name. ARN-named contexts contain ':' and '/'."""
    return re.sub(r'[^A-Za-z0-9._-]+', '_', context).strip('_') or 'context'


def output_path_for_context(output: str, context: str) -> str:
    """'x.json' + 'fr5' -> 'x-fr5.json'; honours a literal {context} placeholder."""
    slug = safe_context_slug(context)
    if '{context}' in output:
        return output.replace('{context}', slug)
    root, ext = os.path.splitext(output)
    return f"{root}-{slug}{ext}"


def run_all_contexts(args, output_dir: str) -> int:
    """Export every context in the kubeconfig, once per unique cluster. Returns an exit code."""
    if args.cluster_name or args.region:
        print("\u274c --all-contexts derives cluster name and region per context; "
              "do not pass them explicitly", file=sys.stderr)
        return 1
    try:
        cfg = kubeconfig_utils.load_kubeconfig(args.kubeconfig)
    except kubeconfig_utils.KubeconfigError as e:
        print(f"\u274c {e}", file=sys.stderr)
        return 1

    # One export per unique cluster. When several contexts point at the same
    # cluster (e.g. 'dc2' and an ARN-named context), keep the shortest name.
    by_cluster = {}
    skipped = []
    for name in kubeconfig_utils.list_context_names(cfg):
        try:
            ctx = kubeconfig_utils.resolve_context(cfg, name)
        except kubeconfig_utils.KubeconfigError as e:
            print(f"\u26a0\ufe0f  Skipping context '{name}': {e}")
            continue
        best = by_cluster.get(ctx.cluster)
        if best is None:
            by_cluster[ctx.cluster] = name
        elif len(name) < len(best):
            skipped.append((best, name))
            by_cluster[ctx.cluster] = name
        else:
            skipped.append((name, best))
    for dup, kept in skipped:
        print(f"\u2139\ufe0f  Skipping context '{dup}': same cluster as '{kept}'")
    plan = list(by_cluster.values())

    print(f"Exporting {len(plan)} cluster(s) from {len(kubeconfig_utils.list_context_names(cfg))} context(s): "
          f"{', '.join(plan)}")
    failures = {}
    outputs = []
    for name in plan:
        print("\n" + "#" * 70 + f"\n# Context: {name}\n" + "#" * 70)
        out = output_path_for_context(args.output, name)
        try:
            run_export(args, name, out, output_dir)
            outputs.append(out)
        except Exception as e:  # keep going; report at the end
            failures[name] = str(e)
            print(f"\u274c Context '{name}' failed: {e}", file=sys.stderr)

    print("\n" + "=" * 70)
    print(f"Completed {len(outputs)}/{len(plan)} context(s)")
    for out in outputs:
        print(f"  \u2705 {out}")
    for name, err in failures.items():
        print(f"  \u274c {name}: {err}")
    return 1 if failures else 0


def run_export(args, context: str, output: str, output_dir: str) -> None:
    """Export one cluster (the given kubeconfig context) to `output`. Raises on failure."""
    exporter = EKSConfigExporter(
        cluster_name=args.cluster_name,
        region=args.region,
        kubeconfig=args.kubeconfig,
        context=context,
        skip_aws=args.skip_aws,
        qps=args.qps,
        burst=args.burst,
        describe_workers=args.describe_workers
    )
    
    print(f"Exporting EKS cluster '{exporter.cluster_name}' (region {exporter.region}, "
          f"context {exporter.export_data['metadata']['kube_context'] or 'default'})...")
    if args.include_aws_resources:
        print("- Including AWS-specific resources (ENIConfigs, SecurityGroupPolicies, etc.)")
    if args.include_custom_crds:
        print("- Including custom resource instances")
    if args.split_by_type:
        print("- Generating individual YAML files by resource type")
    
    exporter.export_all_resources()
    
    # Save main export file
    main_format = args.format if args.output_format == 'json' else args.output_format
    if main_format == 'both':
        json_file = output if output.endswith('.json') else output.rsplit('.', 1)[0] + '.json'
        yaml_file = output.rsplit('.', 1)[0] + '.yaml'
        exporter.save_to_file(json_file, 'json')
        exporter.save_to_file(yaml_file, 'yaml')
        print(f"Export saved to: {json_file} and {yaml_file}")
    else:
        exporter.save_to_file(output, main_format)
        print(f"Export saved to: {output}")
    
    # Generate split YAML files if requested
    yaml_output_dir = None
    if args.split_by_type or args.generate_restore_scripts:
        yaml_output_dir = os.path.join(output_dir, f"kubectl-restore-{exporter.cluster_name}-{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        exporter.save_kubernetes_yaml_resources(yaml_output_dir)
        print(f"Kubernetes YAML resources saved to: {yaml_output_dir}")
    
    # Generate summary report
    if args.summary:
        summary = exporter.generate_summary_report()
        print("\n" + "=" * 60)
        print(summary)
        print("=" * 60)
        
        summary_file = output.rsplit('.', 1)[0] + '_summary.txt'
        with open(summary_file, 'w') as f:
            f.write(summary)
        print(f"Summary report saved to: {summary_file}")
    
    # Show resource counts for new resources
    resources = exporter.export_data.get('resources', {})
    new_resource_counts = {
        'ENI Configs': len(resources.get('eni_configs', [])),
        'Network Attachment Definitions': len(resources.get('network_attachment_definitions', [])),
        'Security Group Policies': len(resources.get('security_group_policies', [])),
        'CNI Nodes': len(resources.get('cni_nodes', [])),
        'Karpenter NodePools': len(resources.get('karpenter_nodepools', [])),
        'Karpenter NodeClasses': len(resources.get('karpenter_nodeclasses', [])),
        'Karpenter NodeClaims': len(resources.get('karpenter_nodeclaims', [])),
        'Additional Custom Resources': len(resources.get('additional_custom_resources', {}))
    }
    
    print("\nEnhanced Resource Summary:")
    for resource_type, count in new_resource_counts.items():
        if count > 0:
            print(f"  {resource_type}: {count}")
    
    print(f"\n\u2705 Export completed successfully!")
    
    if yaml_output_dir and args.split_by_type:
        print(f"\n\U0001f680 To restore this cluster configuration:")
        print(f"   cd {yaml_output_dir}")
        print(f"   ./restore-cluster.sh --validate-dependencies")

if __name__ == '__main__':
    main()