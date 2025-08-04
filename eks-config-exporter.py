#!/usr/bin/env python3

import json
import yaml
import argparse
import sys
import os
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
import subprocess
import boto3
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import logging

class EKSConfigExporter:
    """
    Comprehensive EKS cluster configuration exporter and visualizer.
    Exports all Kubernetes resources including pods, services, deployments, 
    daemonsets, configmaps, secrets, namespaces, and EKS-specific configurations.
    """
    
    def __init__(self, cluster_name: str, region: str = None, kubeconfig: str = None):
        self.cluster_name = cluster_name
        self.region = region or os.environ.get('AWS_DEFAULT_REGION', 'us-west-2')
        self.kubeconfig = kubeconfig
        self.export_data = {
            'metadata': {
                'cluster_name': cluster_name,
                'region': self.region,
                'export_timestamp': datetime.now(timezone.utc).isoformat(),
                'exporter_version': '1.0.0'
            },
            'cluster_info': {},
            'resources': {}
        }
        
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)
        
        # Initialize clients
        self.aws_client = None
        self.k8s_client = None
        self._init_clients()
    
    def _get_kubectl_describe(self, resource_type: str, resource_name: str, namespace: str = None) -> str:
        """Get kubectl describe output for a resource."""
        try:
            cmd = ['kubectl', 'describe', resource_type, resource_name]
            if namespace:
                cmd.extend(['-n', namespace])
            
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
    
    def _init_clients(self):
        """Initialize AWS and Kubernetes clients."""
        try:
            # AWS client
            self.aws_client = boto3.client('eks', region_name=self.region)
            
            # Kubernetes client
            if self.kubeconfig:
                config.load_kube_config(config_file=self.kubeconfig)
            else:
                try:
                    config.load_incluster_config()
                except:
                    config.load_kube_config()
            
            self.k8s_client = {
                'core_v1': client.CoreV1Api(),
                'apps_v1': client.AppsV1Api(),
                'networking_v1': client.NetworkingV1Api(),
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
            
        except Exception as e:
            self.logger.error(f"Failed to initialize clients: {e}")
            raise
    
    def export_cluster_info(self):
        """Export EKS cluster basic information."""
        try:
            cluster_info = self.aws_client.describe_cluster(name=self.cluster_name)
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
                
        except Exception as e:
            self.logger.error(f"Failed to export cluster info: {e}")
    
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
                    'describe_info': self._get_kubectl_describe('namespace', ns.metadata.name)
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
                    'describe_info': self._get_kubectl_describe('node', node.metadata.name)
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
                    'describe_info': self._get_kubectl_describe('pod', pod.metadata.name, pod.metadata.namespace)
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
                    'describe_info': self._get_kubectl_describe('service', service.metadata.name, service.metadata.namespace)
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
                    'describe_info': self._get_kubectl_describe('deployment', deployment.metadata.name, deployment.metadata.namespace)
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
                    'describe_info': self._get_kubectl_describe('daemonset', ds.metadata.name, ds.metadata.namespace)
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
        """Export all endpoint slices."""
        try:
            endpoint_slices = self.k8s_client['networking_v1'].list_endpoint_slice_for_all_namespaces()
            self.export_data['resources']['endpoint_slices'] = []
            
            for es in endpoint_slices.items:
                es_info = {
                    'name': es.metadata.name,
                    'namespace': es.metadata.namespace,
                    'labels': es.metadata.labels or {},
                    'annotations': es.metadata.annotations or {},
                    'address_type': es.address_type,
                    'endpoints': len(es.endpoints or []),
                    'ports': [{'name': p.name, 'port': p.port, 'protocol': p.protocol} for p in (es.ports or [])],
                    'creation_timestamp': es.metadata.creation_timestamp.isoformat() if es.metadata.creation_timestamp else None
                }
                
                self.export_data['resources']['endpoint_slices'].append(es_info)
                
        except ApiException as e:
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
                    'describe_info': self._get_kubectl_describe('configmap', cm.metadata.name, cm.metadata.namespace)
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
                    'describe_info': self._get_kubectl_describe('secret', secret.metadata.name, secret.metadata.namespace)
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
        
        export_methods = [
            ('cluster_info', self.export_cluster_info),
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
                report.append(f"{resource_type.capitalize()}: {len(resources)}")
        
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
    parser = argparse.ArgumentParser(description='Export EKS cluster configuration')
    parser.add_argument('cluster_name', help='EKS cluster name')
    parser.add_argument('--region', help='AWS region', default=None)
    parser.add_argument('--kubeconfig', help='Path to kubeconfig file', default=None)
    parser.add_argument('--output', '-o', help='Output file path', 
                       default='eks-export-{timestamp}.json')
    parser.add_argument('--format', choices=['json', 'yaml'], default='json',
                       help='Output format')
    parser.add_argument('--summary', action='store_true',
                       help='Generate summary report')
    
    args = parser.parse_args()
    
    # Replace timestamp placeholder
    if '{timestamp}' in args.output:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        args.output = args.output.replace('{timestamp}', timestamp)
    
    try:
        exporter = EKSConfigExporter(
            cluster_name=args.cluster_name,
            region=args.region,
            kubeconfig=args.kubeconfig
        )
        
        exporter.export_all_resources()
        exporter.save_to_file(args.output, args.format)
        
        if args.summary:
            summary = exporter.generate_summary_report()
            print(summary)
            
            summary_file = args.output.rsplit('.', 1)[0] + '_summary.txt'
            with open(summary_file, 'w') as f:
                f.write(summary)
            print(f"\nSummary report saved to: {summary_file}")
        
        print(f"\nExport completed successfully!")
        print(f"Output file: {args.output}")
        
    except Exception as e:
        print(f"Export failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()