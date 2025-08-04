#!/usr/bin/env python3

import json
import yaml
import argparse
import os
from datetime import datetime
from typing import Dict, List, Any

class EKSVisualizationGenerator:
    """
    Generates interactive HTML visualization dashboard for EKS cluster exports.
    Creates comprehensive visualizations including resource summaries, network topology,
    and detailed resource listings with search and filtering capabilities.
    """
    
    def __init__(self, export_data: Dict[str, Any]):
        self.data = export_data
        self.cluster_name = export_data.get('metadata', {}).get('cluster_name', 'Unknown')
        self.region = export_data.get('metadata', {}).get('region', 'Unknown')
        self.export_time = export_data.get('metadata', {}).get('export_timestamp', 'Unknown')
    
    def generate_html_dashboard(self) -> str:
        """Generate complete HTML dashboard."""
        html_template = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>EKS Cluster Dashboard - {self.cluster_name}</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background-color: #f5f5f5;
            color: #333;
        }}
        
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 2rem;
            text-align: center;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            padding: 2rem;
        }}
        
        .summary-cards {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2rem;
        }}
        
        .card {{
            background: white;
            padding: 1.5rem;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            transition: transform 0.3s ease;
        }}
        
        .card:hover {{
            transform: translateY(-5px);
        }}
        
        .clickable-card {{
            cursor: pointer;
            transition: all 0.3s ease;
        }}
        
        .clickable-card:hover {{
            transform: translateY(-5px) scale(1.02);
            box-shadow: 0 6px 12px rgba(0,0,0,0.15);
        }}
        
        .card-title {{
            font-size: 1.2rem;
            font-weight: bold;
            color: #667eea;
            margin-bottom: 0.5rem;
        }}
        
        .card-value {{
            font-size: 2rem;
            font-weight: bold;
            color: #333;
        }}
        
        .tabs {{
            display: flex;
            background: white;
            border-radius: 10px 10px 0 0;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 0;
        }}
        
        .tab {{
            flex: 1;
            padding: 1rem;
            text-align: center;
            cursor: pointer;
            background: #f8f9fa;
            border: none;
            font-size: 1rem;
            font-weight: 500;
            color: #666;
            transition: all 0.3s ease;
        }}
        
        .tab.active {{
            background: #667eea;
            color: white;
        }}
        
        .tab:first-child {{
            border-radius: 10px 0 0 0;
        }}
        
        .tab:last-child {{
            border-radius: 0 10px 0 0;
        }}
        
        .tab-content {{
            background: white;
            padding: 2rem;
            border-radius: 0 0 10px 10px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            display: none;
        }}
        
        .tab-content.active {{
            display: block;
        }}
        
        .search-filter {{
            margin-bottom: 1rem;
            display: flex;
            gap: 1rem;
            align-items: center;
        }}
        
        .search-input {{
            flex: 1;
            padding: 0.75rem;
            border: 2px solid #e9ecef;
            border-radius: 5px;
            font-size: 1rem;
        }}
        
        .filter-select {{
            padding: 0.75rem;
            border: 2px solid #e9ecef;
            border-radius: 5px;
            font-size: 1rem;
            background: white;
        }}
        
        .resource-table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 1rem;
        }}
        
        .resource-table th,
        .resource-table td {{
            padding: 0.75rem;
            text-align: left;
            border-bottom: 1px solid #e9ecef;
        }}
        
        .resource-table th {{
            background: #f8f9fa;
            font-weight: 600;
            color: #495057;
        }}
        
        .resource-table tr:hover {{
            background: #f8f9fa;
        }}
        
        .status-badge {{
            padding: 0.25rem 0.75rem;
            border-radius: 20px;
            font-size: 0.875rem;
            font-weight: 500;
        }}
        
        .status-running {{
            background: #d4edda;
            color: #155724;
        }}
        
        .status-pending {{
            background: #fff3cd;
            color: #856404;
        }}
        
        .status-failed {{
            background: #f8d7da;
            color: #721c24;
        }}
        
        .namespace-filter {{
            display: flex;
            flex-wrap: wrap;
            gap: 0.5rem;
            margin-bottom: 1rem;
        }}
        
        .namespace-tag {{
            padding: 0.25rem 0.75rem;
            background: #e9ecef;
            border-radius: 20px;
            cursor: pointer;
            transition: all 0.3s ease;
        }}
        
        .namespace-tag.active {{
            background: #667eea;
            color: white;
        }}
        
        .metric-chart {{
            height: 300px;
            background: #f8f9fa;
            border-radius: 5px;
            display: flex;
            align-items: center;
            justify-content: center;
            margin: 1rem 0;
        }}
        
        .expandable {{
            cursor: pointer;
        }}
        
        .expandable:hover {{
            background: #f0f0f0;
        }}
        
        .details {{
            display: none;
            background: #f8f9fa;
            padding: 1rem;
            margin-top: 0.5rem;
            border-radius: 5px;
        }}
        
        .details.expanded {{
            display: block;
        }}
        
        .view-content {{
            display: none;
            margin-top: 1rem;
        }}
        
        .view-content.active {{
            display: block;
        }}
        
        .btn-details {{
            background-color: #667eea;
            color: white;
            border: none;
            padding: 0.25rem 0.5rem;
            border-radius: 4px;
            cursor: pointer;
            font-size: 0.8rem;
        }}
        
        .btn-details:hover {{
            background-color: #5a6fd8;
        }}
        
        .resource-modal {{
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background-color: rgba(0, 0, 0, 0.5);
            z-index: 1000;
            display: flex;
            justify-content: center;
            align-items: center;
        }}
        
        .modal-content {{
            background: white;
            border-radius: 10px;
            width: 90%;
            max-width: 800px;
            max-height: 90%;
            overflow-y: auto;
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
        }}
        
        .modal-header {{
            padding: 1.5rem;
            border-bottom: 1px solid #dee2e6;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        
        .modal-body {{
            padding: 1.5rem;
        }}
        
        .close-btn {{
            background: none;
            border: none;
            font-size: 2rem;
            cursor: pointer;
            color: #666;
        }}
        
        .close-btn:hover {{
            color: #333;
        }}
        
        .modal-content-area {{
            margin-top: 1rem;
        }}
        
        .json-view {{
            background: #2d3748;
            color: #e2e8f0;
            padding: 1rem;
            border-radius: 5px;
            font-family: 'Courier New', monospace;
            font-size: 0.875rem;
            overflow-x: auto;
            max-height: 400px;
        }}
        
        .yaml-view {{
            background: #1a202c;
            color: #e2e8f0;
            padding: 1rem;
            border-radius: 5px;
            font-family: 'Courier New', monospace;
            font-size: 0.875rem;
            overflow-x: auto;
            max-height: 500px;
            white-space: pre-wrap;
            border: 1px solid #4a5568;
        }}
        
        .view-toggle {{
            margin-bottom: 1rem;
        }}
        
        .toggle-btn {{
            background: #667eea;
            color: white;
            border: none;
            padding: 0.5rem 1rem;
            border-radius: 5px;
            cursor: pointer;
            margin-right: 0.5rem;
            font-size: 0.875rem;
        }}
        
        .toggle-btn.active {{
            background: #5a67d8;
        }}
        
        .toggle-btn:hover {{
            background: #5a67d8;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>EKS Cluster Dashboard</h1>
        <p>Cluster: <strong>{self.cluster_name}</strong> | Region: <strong>{self.region}</strong></p>
        <p>Exported: {self.export_time}</p>
    </div>
    
    <div class="container">
        {self._generate_summary_cards()}
        
        <div class="tabs">
            <button class="tab active" onclick="showTab('overview')">Overview</button>
            <button class="tab" onclick="showTab('pods')">Pods</button>
            <button class="tab" onclick="showTab('services')">Services</button>
            <button class="tab" onclick="showTab('deployments')">Deployments</button>
            <button class="tab" onclick="showTab('nodes')">Nodes</button>
            <button class="tab" onclick="showTab('namespaces')">Namespaces</button>
            <button class="tab" onclick="showTab('daemonsets')">DaemonSets</button>
            <button class="tab" onclick="showTab('configmaps')">ConfigMaps</button>
            <button class="tab" onclick="showTab('secrets')">Secrets</button>
            <button class="tab" onclick="showTab('storage')">Storage</button>
        </div>
        
        <div id="overview" class="tab-content active">
            {self._generate_overview_content()}
        </div>
        
        <div id="pods" class="tab-content">
            {self._generate_pods_content()}
        </div>
        
        <div id="services" class="tab-content">
            {self._generate_services_content()}
        </div>
        
        <div id="deployments" class="tab-content">
            {self._generate_deployments_content()}
        </div>
        
        <div id="nodes" class="tab-content">
            {self._generate_nodes_content()}
        </div>
        
        <div id="namespaces" class="tab-content">
            {self._generate_namespaces_content()}
        </div>
        
        <div id="daemonsets" class="tab-content">
            {self._generate_daemonsets_content()}
        </div>
        
        <div id="configmaps" class="tab-content">
            {self._generate_configmaps_content()}
        </div>
        
        <div id="secrets" class="tab-content">
            {self._generate_secrets_content()}
        </div>
        
        <div id="storage" class="tab-content">
            {self._generate_storage_content()}
        </div>
    </div>
    
    <script>
        {self._generate_javascript()}
    </script>
</body>
</html>"""
        return html_template
    
    def _generate_summary_cards(self) -> str:
        """Generate summary cards for the dashboard."""
        resources = self.data.get('resources', {})
        
        cards_html = '<div class="summary-cards">'
        
        # Resource counts
        resource_counts = {
            'Pods': len(resources.get('pods', [])),
            'Services': len(resources.get('services', [])),
            'Deployments': len(resources.get('deployments', [])),
            'Nodes': len(resources.get('nodes', [])),
            'Namespaces': len(resources.get('namespaces', [])),
            'DaemonSets': len(resources.get('daemonsets', [])),
            'ConfigMaps': len(resources.get('configmaps', [])),
            'Secrets': len(resources.get('secrets', []))
        }
        
        for resource_type, count in resource_counts.items():
            # Map display names to tab names
            tab_mapping = {
                'Pods': 'pods',
                'Services': 'services', 
                'Deployments': 'deployments',
                'Nodes': 'nodes',
                'Namespaces': 'namespaces',
                'DaemonSets': 'daemonsets',
                'ConfigMaps': 'configmaps',
                'Secrets': 'secrets'
            }
            tab_name = tab_mapping.get(resource_type, resource_type.lower())
            
            cards_html += f'''
            <div class="card clickable-card" onclick="showTab('{tab_name}')">
                <div class="card-title">{resource_type}</div>
                <div class="card-value">{count}</div>
            </div>
            '''
        
        cards_html += '</div>'
        return cards_html
    
    def _generate_overview_content(self) -> str:
        """Generate overview tab content."""
        cluster_info = self.data.get('cluster_info', {})
        
        overview_html = f'''
        <h2>Cluster Information</h2>
        <div class="card">
            <h3>Basic Details</h3>
            <p><strong>Name:</strong> {cluster_info.get('name', 'N/A')}</p>
            <p><strong>Version:</strong> {cluster_info.get('version', 'N/A')}</p>
            <p><strong>Status:</strong> <span class="status-badge status-running">{cluster_info.get('status', 'N/A')}</span></p>
            <p><strong>Endpoint:</strong> {cluster_info.get('endpoint', 'N/A')}</p>
            <p><strong>Platform Version:</strong> {cluster_info.get('platformVersion', 'N/A')}</p>
            <p><strong>Created:</strong> {cluster_info.get('createdAt', 'N/A')}</p>
        </div>
        
        <h3>Network Configuration</h3>
        <div class="card">
            <p><strong>VPC ID:</strong> {cluster_info.get('resourcesVpcConfig', {}).get('vpcId', 'N/A')}</p>
            <p><strong>Subnet IDs:</strong> {', '.join(cluster_info.get('resourcesVpcConfig', {}).get('subnetIds', []))}</p>
            <p><strong>Security Group IDs:</strong> {', '.join(cluster_info.get('resourcesVpcConfig', {}).get('securityGroupIds', []))}</p>
            <p><strong>Endpoint Private Access:</strong> {cluster_info.get('resourcesVpcConfig', {}).get('endpointPrivateAccess', 'N/A')}</p>
            <p><strong>Endpoint Public Access:</strong> {cluster_info.get('resourcesVpcConfig', {}).get('endpointPublicAccess', 'N/A')}</p>
        </div>
        '''
        
        # Node groups information
        nodegroups = cluster_info.get('nodegroups', [])
        if nodegroups:
            overview_html += '<h3>Node Groups</h3>'
            for ng in nodegroups:
                overview_html += f'''
                <div class="card">
                    <h4>{ng.get('nodegroupName', 'Unknown')}</h4>
                    <p><strong>Status:</strong> <span class="status-badge status-running">{ng.get('status', 'N/A')}</span></p>
                    <p><strong>Instance Types:</strong> {', '.join(ng.get('instanceTypes', []))}</p>
                    <p><strong>AMI Type:</strong> {ng.get('amiType', 'N/A')}</p>
                    <p><strong>Capacity Type:</strong> {ng.get('capacityType', 'N/A')}</p>
                    <p><strong>Desired Size:</strong> {ng.get('scalingConfig', {}).get('desiredSize', 'N/A')}</p>
                    <p><strong>Min Size:</strong> {ng.get('scalingConfig', {}).get('minSize', 'N/A')}</p>
                    <p><strong>Max Size:</strong> {ng.get('scalingConfig', {}).get('maxSize', 'N/A')}</p>
                </div>
                '''
        
        return overview_html
    
    def _generate_pods_content(self) -> str:
        """Generate pods tab content."""
        pods = self.data.get('resources', {}).get('pods', [])
        
        # Get unique namespaces
        namespaces = list(set(pod.get('namespace', 'default') for pod in pods))
        
        content = f'''
        <h2>Pods ({len(pods)})</h2>
        <div class="search-filter">
            <input type="text" class="search-input" id="podSearch" placeholder="Search pods..." onkeyup="filterTable('podTable', this.value)">
            <select class="filter-select" id="podNamespaceFilter" onchange="filterPodsByNamespace()">
                <option value="">All Namespaces</option>
                {''.join(f'<option value="{ns}">{ns}</option>' for ns in sorted(namespaces))}
            </select>
        </div>
        
        <table class="resource-table" id="podTable">
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Namespace</th>
                    <th>Status</th>
                    <th>Node</th>
                    <th>Pod IP</th>
                    <th>Containers</th>
                    <th>Age</th>
                </tr>
            </thead>
            <tbody>
        '''
        
        for pod in pods:
            status_class = 'status-running' if pod.get('phase') == 'Running' else 'status-pending'
            if pod.get('phase') in ['Failed', 'Error']:
                status_class = 'status-failed'
            
            container_count = len(pod.get('containers', []))
            age = self._calculate_age(pod.get('creation_timestamp'))
            
            content += f'''
                <tr class="expandable" onclick="toggleDetails(this)">
                    <td>{pod.get('name', 'N/A')}</td>
                    <td>{pod.get('namespace', 'N/A')}</td>
                    <td><span class="status-badge {status_class}">{pod.get('phase', 'N/A')}</span></td>
                    <td>{pod.get('node_name', 'N/A')}</td>
                    <td>{pod.get('pod_ip', 'N/A')}</td>
                    <td>{container_count}</td>
                    <td>{age}</td>
                </tr>
                <tr>
                    <td colspan="7">
                        <div class="details">
                            <h4>Pod Details</h4>
                            <p><strong>Host IP:</strong> {pod.get('host_ip', 'N/A')}</p>
                            <p><strong>Labels:</strong> {self._format_dict(pod.get('labels', {}))}</p>
                            <p><strong>Containers:</strong></p>
                            <ul>
                                {''.join(f'<li>{c.get("name", "Unknown")}: {c.get("image", "N/A")}</li>' for c in pod.get('containers', []))}
                            </ul>
                            <p><strong>Volumes:</strong> {len(pod.get('volumes', []))}</p>
                            
                            <div class="view-toggle">
                                <button class="toggle-btn active" onclick="showView(this, 'summary')">Summary</button>
                                <button class="toggle-btn" onclick="showView(this, 'json')">JSON</button>
                                <button class="toggle-btn" onclick="showView(this, 'yaml')">YAML</button>
                            </div>
                            
                            <div class="view-content summary-view">
                                <!-- Summary already shown above -->
                            </div>
                            
                            <div class="view-content json-view" style="display: none;">
                                <pre>{json.dumps(pod, indent=2)}</pre>
                            </div>
                            
                            <div class="view-content yaml-view" style="display: none;">
{self._to_yaml(pod)}</div>
                        </div>
                    </td>
                </tr>
            '''
        
        content += '''
            </tbody>
        </table>
        '''
        
        return content
    
    def _generate_services_content(self) -> str:
        """Generate services tab content."""
        services = self.data.get('resources', {}).get('services', [])
        
        content = f'''
        <h2>Services ({len(services)})</h2>
        <div class="search-filter">
            <input type="text" class="search-input" id="serviceSearch" placeholder="Search services..." onkeyup="filterTable('serviceTable', this.value)">
        </div>
        
        <table class="resource-table" id="serviceTable">
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Namespace</th>
                    <th>Type</th>
                    <th>Cluster IP</th>
                    <th>Ports</th>
                    <th>Age</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>
        '''
        
        for service in services:
            ports = ', '.join(f"{p.get('port', 'N/A')}/{p.get('protocol', 'TCP')}" for p in service.get('ports', []))
            age = self._calculate_age(service.get('creation_timestamp'))
            
            content += f'''
                <tr class="expandable" onclick="toggleDetails(this)">
                    <td>{service.get('name', 'N/A')}</td>
                    <td>{service.get('namespace', 'N/A')}</td>
                    <td>{service.get('type', 'N/A')}</td>
                    <td>{service.get('cluster_ip', 'N/A')}</td>
                    <td>{ports}</td>
                    <td>{age}</td>
                    <td><button class="btn-details" onclick="event.stopPropagation(); showResourceDetails('{service.get('name', 'N/A')}', 'service', {json.dumps(service, default=str).replace('"', '&quot;')})">View Details</button></td>
                </tr>
                <tr>
                    <td colspan="7">
                        <div class="details">
                            <h4>Service Details</h4>
                            <p><strong>Selector:</strong> {self._format_dict(service.get('selector', {}))}</p>
                            <p><strong>Labels:</strong> {self._format_dict(service.get('labels', {}))}</p>
                            <p><strong>Annotations:</strong> {self._format_dict(service.get('annotations', {}))}</p>
                            
                            <div class="view-toggle">
                                <button class="toggle-btn active" onclick="showView(this, 'summary')">Summary</button>
                                <button class="toggle-btn" onclick="showView(this, 'json')">JSON</button>
                                <button class="toggle-btn" onclick="showView(this, 'yaml')">YAML</button>
                                <button class="toggle-btn" onclick="showView(this, 'describe')">Describe</button>
                            </div>
                            
                            <div class="view-content summary active">
                                <p><strong>Service Summary:</strong></p>
                                <ul>
                                    <li>Type: {service.get('type', 'N/A')}</li>
                                    <li>Cluster IP: {service.get('cluster_ip', 'N/A')}</li>
                                    <li>Ports: {len(service.get('ports', []))}</li>
                                    <li>Selector: {self._format_dict(service.get('selector', {}))}</li>
                                </ul>
                            </div>
                            
                            <div class="view-content json">
                                <pre><code>{json.dumps(service, indent=2, default=str)}</code></pre>
                            </div>
                            
                            <div class="view-content yaml">
                                <pre><code>{yaml.dump(service, default_flow_style=False)}</code></pre>
                            </div>
                            
                            <div class="view-content describe">
                                <pre><code>{service.get('describe_info', 'Describe information not available')}</code></pre>
                            </div>
                        </div>
                    </td>
                </tr>
            '''
        
        content += '''
            </tbody>
        </table>
        '''
        
        return content
    
    def _generate_deployments_content(self) -> str:
        """Generate deployments tab content."""
        deployments = self.data.get('resources', {}).get('deployments', [])
        
        content = f'''
        <h2>Deployments ({len(deployments)})</h2>
        <div class="search-filter">
            <input type="text" class="search-input" id="deploymentSearch" placeholder="Search deployments..." onkeyup="filterTable('deploymentTable', this.value)">
        </div>
        
        <table class="resource-table" id="deploymentTable">
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Namespace</th>
                    <th>Ready</th>
                    <th>Up-to-date</th>
                    <th>Available</th>
                    <th>Age</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>
        '''
        
        for deployment in deployments:
            ready_replicas = deployment.get('ready_replicas', 0)
            replicas = deployment.get('replicas', 0)
            available_replicas = deployment.get('available_replicas', 0)
            age = self._calculate_age(deployment.get('creation_timestamp'))
            
            content += f'''
                <tr class="expandable" onclick="toggleDetails(this)">
                    <td>{deployment.get('name', 'N/A')}</td>
                    <td>{deployment.get('namespace', 'N/A')}</td>
                    <td>{ready_replicas}/{replicas}</td>
                    <td>{replicas}</td>
                    <td>{available_replicas}</td>
                    <td>{age}</td>
                    <td><button class="btn-details" onclick="event.stopPropagation(); showResourceDetails('{deployment.get('name', 'N/A')}', 'deployment', {json.dumps(deployment, default=str).replace('"', '&quot;')})">View Details</button></td>
                </tr>
                <tr>
                    <td colspan="7">
                        <div class="details">
                            <h4>Deployment Details</h4>
                            <p><strong>Labels:</strong> {self._format_dict(deployment.get('labels', {}))}</p>
                            <p><strong>Annotations:</strong> {self._format_dict(deployment.get('annotations', {}))}</p>
                            <p><strong>Selector:</strong> {self._format_dict(deployment.get('selector', {}))}</p>
                            <p><strong>Strategy:</strong> {deployment.get('strategy', 'N/A')}</p>
                            
                            <div class="view-toggle">
                                <button class="toggle-btn active" onclick="showView(this, 'summary')">Summary</button>
                                <button class="toggle-btn" onclick="showView(this, 'json')">JSON</button>
                                <button class="toggle-btn" onclick="showView(this, 'yaml')">YAML</button>
                                <button class="toggle-btn" onclick="showView(this, 'describe')">Describe</button>
                            </div>
                            
                            <div class="view-content summary active">
                                <p><strong>Replica Status:</strong></p>
                                <ul>
                                    <li>Desired Replicas: {replicas}</li>
                                    <li>Ready Replicas: {ready_replicas}</li>
                                    <li>Available Replicas: {available_replicas}</li>
                                </ul>
                            </div>
                            
                            <div class="view-content json">
                                <pre><code>{json.dumps(deployment, indent=2, default=str)}</code></pre>
                            </div>
                            
                            <div class="view-content yaml">
                                <pre><code>{yaml.dump(deployment, default_flow_style=False)}</code></pre>
                            </div>
                            
                            <div class="view-content describe">
                                <pre><code>{deployment.get('describe_info', 'Describe information not available')}</code></pre>
                            </div>
                        </div>
                    </td>
                </tr>
            '''
        
        content += '''
            </tbody>
        </table>
        '''
        
        return content
    
    def _generate_nodes_content(self) -> str:
        """Generate nodes tab content."""
        nodes = self.data.get('resources', {}).get('nodes', [])
        
        content = f'''
        <h2>Nodes ({len(nodes)})</h2>
        
        <table class="resource-table" id="nodeTable">
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Status</th>
                    <th>CPU</th>
                    <th>Memory</th>
                    <th>OS</th>
                    <th>Kernel</th>
                    <th>Age</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>
        '''
        
        for node in nodes:
            # Find Ready condition
            ready_condition = next(
                (c for c in node.get('conditions', []) if c.get('type') == 'Ready'),
                {'status': 'Unknown'}
            )
            status = 'Ready' if ready_condition.get('status') == 'True' else 'NotReady'
            status_class = 'status-running' if status == 'Ready' else 'status-failed'
            
            capacity = node.get('capacity', {})
            node_info = node.get('node_info', {})
            age = self._calculate_age(node.get('creation_timestamp'))
            
            content += f'''
                <tr class="expandable" onclick="toggleDetails(this)">
                    <td>{node.get('name', 'N/A')}</td>
                    <td><span class="status-badge {status_class}">{status}</span></td>
                    <td>{capacity.get('cpu', 'N/A')}</td>
                    <td>{capacity.get('memory', 'N/A')}</td>
                    <td>{node_info.get('osImage', 'N/A')}</td>
                    <td>{node_info.get('kernelVersion', 'N/A')}</td>
                    <td>{age}</td>
                    <td><button class="btn-details" onclick="event.stopPropagation(); showResourceDetails('{node.get('name', 'N/A')}', 'node', {json.dumps(node, default=str).replace('"', '&quot;')})">View Details</button></td>
                </tr>
                <tr>
                    <td colspan="8">
                        <div class="details">
                            <h4>Node Details</h4>
                            <p><strong>Labels:</strong> {self._format_dict(node.get('labels', {}))}</p>
                            <p><strong>Annotations:</strong> {self._format_dict(node.get('annotations', {}))}</p>
                            <p><strong>Allocatable Resources:</strong> CPU: {node.get('allocatable', {}).get('cpu', 'N/A')}, Memory: {node.get('allocatable', {}).get('memory', 'N/A')}</p>
                            
                            <div class="view-toggle">
                                <button class="toggle-btn active" onclick="showView(this, 'summary')">Summary</button>
                                <button class="toggle-btn" onclick="showView(this, 'json')">JSON</button>
                                <button class="toggle-btn" onclick="showView(this, 'yaml')">YAML</button>
                                <button class="toggle-btn" onclick="showView(this, 'describe')">Describe</button>
                            </div>
                            
                            <div class="view-content summary active">
                                <p><strong>Node Summary:</strong></p>
                                <ul>
                                    <li>Status: {status}</li>
                                    <li>CPU Capacity: {capacity.get('cpu', 'N/A')}</li>
                                    <li>Memory Capacity: {capacity.get('memory', 'N/A')}</li>
                                    <li>OS: {node_info.get('osImage', 'N/A')}</li>
                                    <li>Container Runtime: {node_info.get('containerRuntimeVersion', 'N/A')}</li>
                                    <li>Kubelet Version: {node_info.get('kubeletVersion', 'N/A')}</li>
                                </ul>
                            </div>
                            
                            <div class="view-content json">
                                <pre><code>{json.dumps(node, indent=2, default=str)}</code></pre>
                            </div>
                            
                            <div class="view-content yaml">
                                <pre><code>{yaml.dump(node, default_flow_style=False)}</code></pre>
                            </div>
                            
                            <div class="view-content describe">
                                <pre><code>{node.get('describe_info', 'Describe information not available')}</code></pre>
                            </div>
                        </div>
                    </td>
                </tr>
            '''
        
        content += '''
            </tbody>
        </table>
        '''
        
        return content
    
    def _generate_namespaces_content(self) -> str:
        """Generate namespaces tab content."""
        namespaces = self.data.get('resources', {}).get('namespaces', [])
        
        content = f'''
        <h2>Namespaces ({len(namespaces)})</h2>
        
        <table class="resource-table" id="namespaceTable">
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Status</th>
                    <th>Labels</th>
                    <th>Age</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>
        '''
        
        for namespace in namespaces:
            status_class = 'status-running' if namespace.get('status') == 'Active' else 'status-pending'
            age = self._calculate_age(namespace.get('creation_timestamp'))
            
            content += f'''
                <tr class="expandable" onclick="toggleDetails(this)">
                    <td>{namespace.get('name', 'N/A')}</td>
                    <td><span class="status-badge {status_class}">{namespace.get('status', 'N/A')}</span></td>
                    <td>{self._format_dict(namespace.get('labels', {}))}</td>
                    <td>{age}</td>
                    <td><button class="btn-details" onclick="event.stopPropagation(); showResourceDetails('{namespace.get('name', 'N/A')}', 'namespace', {json.dumps(namespace, default=str).replace('"', '&quot;')})">View Details</button></td>
                </tr>
                <tr>
                    <td colspan="5">
                        <div class="details">
                            <h4>Namespace Details</h4>
                            <p><strong>Status:</strong> {namespace.get('status', 'N/A')}</p>
                            <p><strong>Labels:</strong> {self._format_dict(namespace.get('labels', {}))}</p>
                            <p><strong>Annotations:</strong> {self._format_dict(namespace.get('annotations', {}))}</p>
                            
                            <div class="view-toggle">
                                <button class="toggle-btn active" onclick="showView(this, 'summary')">Summary</button>
                                <button class="toggle-btn" onclick="showView(this, 'json')">JSON</button>
                                <button class="toggle-btn" onclick="showView(this, 'yaml')">YAML</button>
                                <button class="toggle-btn" onclick="showView(this, 'describe')">Describe</button>
                            </div>
                            
                            <div class="view-content summary active">
                                <p><strong>Namespace Summary:</strong></p>
                                <ul>
                                    <li>Status: {namespace.get('status', 'N/A')}</li>
                                    <li>Labels: {len(namespace.get('labels', {}))}</li>
                                    <li>Annotations: {len(namespace.get('annotations', {}))}</li>
                                </ul>
                            </div>
                            
                            <div class="view-content json">
                                <pre><code>{json.dumps(namespace, indent=2, default=str)}</code></pre>
                            </div>
                            
                            <div class="view-content yaml">
                                <pre><code>{yaml.dump(namespace, default_flow_style=False)}</code></pre>
                            </div>
                            
                            <div class="view-content describe">
                                <pre><code>{namespace.get('describe_info', 'Describe information not available')}</code></pre>
                            </div>
                        </div>
                    </td>
                </tr>
            '''
        
        content += '''
            </tbody>
        </table>
        '''
        
        return content
    
    def _generate_storage_content(self) -> str:
        """Generate storage tab content."""
        pvs = self.data.get('resources', {}).get('persistent_volumes', [])
        
        content = f'''
        <h2>Persistent Volumes ({len(pvs)})</h2>
        
        <table class="resource-table">
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Status</th>
                    <th>Capacity</th>
                    <th>Access Modes</th>
                    <th>Reclaim Policy</th>
                    <th>Claim</th>
                    <th>Age</th>
                </tr>
            </thead>
            <tbody>
        '''
        
        for pv in pvs:
            status_class = 'status-running' if pv.get('status') == 'Bound' else 'status-pending'
            capacity = pv.get('capacity', {}).get('storage', 'N/A')
            access_modes = ', '.join(pv.get('access_modes', []))
            age = self._calculate_age(pv.get('creation_timestamp'))
            
            content += f'''
                <tr>
                    <td>{pv.get('name', 'N/A')}</td>
                    <td><span class="status-badge {status_class}">{pv.get('status', 'N/A')}</span></td>
                    <td>{capacity}</td>
                    <td>{access_modes}</td>
                    <td>{pv.get('reclaim_policy', 'N/A')}</td>
                    <td>{pv.get('claim', 'N/A')}</td>
                    <td>{age}</td>
                </tr>
            '''
        
        content += '''
            </tbody>
        </table>
        '''
        
        return content
    
    def _generate_daemonsets_content(self) -> str:
        """Generate daemonsets tab content."""
        daemonsets = self.data.get('resources', {}).get('daemonsets', [])
        
        content = f'''
        <h2>DaemonSets ({len(daemonsets)})</h2>
        <div class="search-filter">
            <input type="text" class="search-input" id="daemonsetSearch" placeholder="Search daemonsets..." onkeyup="filterTable('daemonsetTable', this.value)">
        </div>
        
        <table class="resource-table" id="daemonsetTable">
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Namespace</th>
                    <th>Desired</th>
                    <th>Current</th>
                    <th>Ready</th>
                    <th>Node Selector</th>
                    <th>Age</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>
        '''
        
        for daemonset in daemonsets:
            desired = daemonset.get('desired_number_scheduled', 0)
            current = daemonset.get('current_number_scheduled', 0)
            ready = daemonset.get('number_ready', 0)
            node_selector = self._format_dict(daemonset.get('selector', {}))
            age = self._calculate_age(daemonset.get('creation_timestamp'))
            
            content += f'''
                <tr class="expandable" onclick="toggleDetails(this)">
                    <td>{daemonset.get('name', 'N/A')}</td>
                    <td>{daemonset.get('namespace', 'N/A')}</td>
                    <td>{desired}</td>
                    <td>{current}</td>
                    <td>{ready}</td>
                    <td>{node_selector}</td>
                    <td>{age}</td>
                    <td><button class="btn-details" onclick="event.stopPropagation(); showResourceDetails('{daemonset.get('name', 'N/A')}', 'daemonset', {json.dumps(daemonset, default=str).replace('"', '&quot;')})">View Details</button></td>
                </tr>
                <tr>
                    <td colspan="8">
                        <div class="details">
                            <h4>DaemonSet Details</h4>
                            <p><strong>Labels:</strong> {self._format_dict(daemonset.get('labels', {}))}</p>
                            <p><strong>Annotations:</strong> {self._format_dict(daemonset.get('annotations', {}))}</p>
                            <p><strong>Selector:</strong> {self._format_dict(daemonset.get('selector', {}))}</p>
                            
                            <div class="view-toggle">
                                <button class="toggle-btn active" onclick="showView(this, 'summary')">Summary</button>
                                <button class="toggle-btn" onclick="showView(this, 'json')">JSON</button>
                                <button class="toggle-btn" onclick="showView(this, 'yaml')">YAML</button>
                                <button class="toggle-btn" onclick="showView(this, 'describe')">Describe</button>
                            </div>
                            
                            <div class="view-content summary active">
                                <p><strong>Status Summary:</strong></p>
                                <ul>
                                    <li>Desired Scheduled: {desired}</li>
                                    <li>Current Scheduled: {current}</li>
                                    <li>Number Ready: {ready}</li>
                                </ul>
                            </div>
                            
                            <div class="view-content json">
                                <pre><code>{json.dumps(daemonset, indent=2, default=str)}</code></pre>
                            </div>
                            
                            <div class="view-content yaml">
                                <pre><code>{yaml.dump(daemonset, default_flow_style=False)}</code></pre>
                            </div>
                            
                            <div class="view-content describe">
                                <pre><code>{daemonset.get('describe_info', 'Describe information not available')}</code></pre>
                            </div>
                        </div>
                    </td>
                </tr>
            '''
        
        content += '''
            </tbody>
        </table>
        '''
        
        return content
    
    def _generate_configmaps_content(self) -> str:
        """Generate configmaps tab content."""
        configmaps = self.data.get('resources', {}).get('configmaps', [])
        
        content = f'''
        <h2>ConfigMaps ({len(configmaps)})</h2>
        <div class="search-filter">
            <input type="text" class="search-input" id="configmapSearch" placeholder="Search configmaps..." onkeyup="filterTable('configmapTable', this.value)">
        </div>
        
        <table class="resource-table" id="configmapTable">
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Namespace</th>
                    <th>Data Keys</th>
                    <th>Age</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>
        '''
        
        for configmap in configmaps:
            data_keys = ', '.join(configmap.get('data_keys', []))
            age = self._calculate_age(configmap.get('creation_timestamp'))
            
            content += f'''
                <tr class="expandable" onclick="toggleDetails(this)">
                    <td>{configmap.get('name', 'N/A')}</td>
                    <td>{configmap.get('namespace', 'N/A')}</td>
                    <td>{data_keys}</td>
                    <td>{age}</td>
                    <td><button class="btn-details" onclick="event.stopPropagation(); showResourceDetails('{configmap.get('name', 'N/A')}', 'configmap', {json.dumps(configmap, default=str).replace('"', '&quot;')})">View Details</button></td>
                </tr>
                <tr>
                    <td colspan="5">
                        <div class="details">
                            <h4>ConfigMap Details</h4>
                            <p><strong>Labels:</strong> {self._format_dict(configmap.get('labels', {}))}</p>
                            <p><strong>Annotations:</strong> {self._format_dict(configmap.get('annotations', {}))}</p>
                            <p><strong>Data Keys:</strong> {len(configmap.get('data_keys', []))} keys</p>
                            
                            <div class="view-toggle">
                                <button class="toggle-btn active" onclick="showView(this, 'summary')">Summary</button>
                                <button class="toggle-btn" onclick="showView(this, 'json')">JSON</button>
                                <button class="toggle-btn" onclick="showView(this, 'yaml')">YAML</button>
                                <button class="toggle-btn" onclick="showView(this, 'describe')">Describe</button>
                            </div>
                            
                            <div class="view-content summary active">
                                <p><strong>Configuration Summary:</strong></p>
                                <ul>
                                    <li>Total Data Keys: {len(configmap.get('data_keys', []))}</li>
                                    <li>Keys: {', '.join(configmap.get('data_keys', [])[:5])}{'...' if len(configmap.get('data_keys', [])) > 5 else ''}</li>
                                </ul>
                            </div>
                            
                            <div class="view-content json">
                                <pre><code>{json.dumps(configmap, indent=2, default=str)}</code></pre>
                            </div>
                            
                            <div class="view-content yaml">
                                <pre><code>{yaml.dump(configmap, default_flow_style=False)}</code></pre>
                            </div>
                            
                            <div class="view-content describe">
                                <pre><code>{configmap.get('describe_info', 'Describe information not available')}</code></pre>
                            </div>
                        </div>
                    </td>
                </tr>
            '''
        
        content += '''
            </tbody>
        </table>
        '''
        
        return content
    
    def _generate_secrets_content(self) -> str:
        """Generate secrets tab content."""
        secrets = self.data.get('resources', {}).get('secrets', [])
        
        content = f'''
        <h2>Secrets ({len(secrets)})</h2>
        <div class="search-filter">
            <input type="text" class="search-input" id="secretSearch" placeholder="Search secrets..." onkeyup="filterTable('secretTable', this.value)">
        </div>
        
        <table class="resource-table" id="secretTable">
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Namespace</th>
                    <th>Type</th>
                    <th>Data Keys</th>
                    <th>Age</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>
        '''
        
        for secret in secrets:
            data_keys = ', '.join(secret.get('data_keys', []))
            age = self._calculate_age(secret.get('creation_timestamp'))
            
            content += f'''
                <tr class="expandable" onclick="toggleDetails(this)">
                    <td>{secret.get('name', 'N/A')}</td>
                    <td>{secret.get('namespace', 'N/A')}</td>
                    <td>{secret.get('type', 'N/A')}</td>
                    <td>{data_keys}</td>
                    <td>{age}</td>
                    <td><button class="btn-details" onclick="event.stopPropagation(); showResourceDetails('{secret.get('name', 'N/A')}', 'secret', {json.dumps(secret, default=str).replace('"', '&quot;')})">View Details</button></td>
                </tr>
                <tr>
                    <td colspan="6">
                        <div class="details">
                            <h4>Secret Details</h4>
                            <p><strong>Labels:</strong> {self._format_dict(secret.get('labels', {}))}</p>
                            <p><strong>Annotations:</strong> {self._format_dict(secret.get('annotations', {}))}</p>
                            <p><strong>Type:</strong> {secret.get('type', 'N/A')}</p>
                            <p><strong>Data Keys:</strong> {len(secret.get('data_keys', []))} keys (data hidden for security)</p>
                            
                            <div class="view-toggle">
                                <button class="toggle-btn active" onclick="showView(this, 'summary')">Summary</button>
                                <button class="toggle-btn" onclick="showView(this, 'json')">JSON</button>
                                <button class="toggle-btn" onclick="showView(this, 'yaml')">YAML</button>
                                <button class="toggle-btn" onclick="showView(this, 'describe')">Describe</button>
                            </div>
                            
                            <div class="view-content summary active">
                                <p><strong>Secret Summary:</strong></p>
                                <ul>
                                    <li>Type: {secret.get('type', 'N/A')}</li>
                                    <li>Total Data Keys: {len(secret.get('data_keys', []))}</li>
                                    <li>Keys: {', '.join(secret.get('data_keys', [])[:5])}{'...' if len(secret.get('data_keys', [])) > 5 else ''}</li>
                                </ul>
                                <p><em>Note: Secret data values are not displayed for security reasons</em></p>
                            </div>
                            
                            <div class="view-content json">
                                <pre><code>{json.dumps(secret, indent=2, default=str)}</code></pre>
                            </div>
                            
                            <div class="view-content yaml">
                                <pre><code>{yaml.dump(secret, default_flow_style=False)}</code></pre>
                            </div>
                            
                            <div class="view-content describe">
                                <pre><code>{secret.get('describe_info', 'Describe information not available')}</code></pre>
                            </div>
                        </div>
                    </td>
                </tr>
            '''
        
        content += '''
            </tbody>
        </table>
        '''
        
        return content
    
    def _generate_javascript(self) -> str:
        """Generate JavaScript for interactivity."""
        return '''
        function showTab(tabName) {
            // Hide all tab contents
            const contents = document.querySelectorAll('.tab-content');
            contents.forEach(content => content.classList.remove('active'));
            
            // Remove active class from all tabs
            const tabs = document.querySelectorAll('.tab');
            tabs.forEach(tab => tab.classList.remove('active'));
            
            // Show selected tab content
            document.getElementById(tabName).classList.add('active');
            
            // Add active class to clicked tab
            event.target.classList.add('active');
        }
        
        function filterTable(tableId, searchValue) {
            const table = document.getElementById(tableId);
            const rows = table.getElementsByTagName('tr');
            
            for (let i = 1; i < rows.length; i += 2) { // Skip header and detail rows
                const row = rows[i];
                const text = row.textContent.toLowerCase();
                const shouldShow = text.includes(searchValue.toLowerCase());
                row.style.display = shouldShow ? '' : 'none';
                if (rows[i + 1]) { // Hide corresponding detail row
                    rows[i + 1].style.display = shouldShow ? '' : 'none';
                }
            }
        }
        
        function filterPodsByNamespace() {
            const select = document.getElementById('podNamespaceFilter');
            const selectedNamespace = select.value;
            const table = document.getElementById('podTable');
            const rows = table.getElementsByTagName('tr');
            
            for (let i = 1; i < rows.length; i += 2) { // Skip header
                const row = rows[i];
                const namespaceCell = row.cells[1]; // Namespace is the second column
                const shouldShow = !selectedNamespace || namespaceCell.textContent === selectedNamespace;
                row.style.display = shouldShow ? '' : 'none';
                if (rows[i + 1]) { // Hide corresponding detail row
                    rows[i + 1].style.display = shouldShow ? '' : 'none';
                }
            }
        }
        
        function toggleDetails(row) {
            const detailsRow = row.nextElementSibling;
            const details = detailsRow.querySelector('.details');
            details.classList.toggle('expanded');
        }
        
        function showView(button, viewType) {
            const detailsDiv = button.closest('.details');
            const allButtons = detailsDiv.querySelectorAll('.toggle-btn');
            const allViews = detailsDiv.querySelectorAll('.view-content');
            
            // Remove active class from all buttons
            allButtons.forEach(btn => btn.classList.remove('active'));
            
            // Hide all views
            allViews.forEach(view => view.classList.remove('active'));
            
            // Activate clicked button
            button.classList.add('active');
            
            // Show selected view
            const targetView = detailsDiv.querySelector(`.view-content.${viewType}`);
            if (targetView) {
                targetView.classList.add('active');
            }
        }
        
        function showResourceDetails(name, type, data) {
            // Create a modal or detailed view
            const modal = document.createElement('div');
            modal.className = 'resource-modal';
            modal.innerHTML = `
                <div class="modal-content">
                    <div class="modal-header">
                        <h3>${type}: ${name}</h3>
                        <button class="close-btn" onclick="closeModal()">&times;</button>
                    </div>
                    <div class="modal-body">
                        <div class="view-toggle">
                            <button class="toggle-btn active" onclick="showModalView(this, 'summary')">Summary</button>
                            <button class="toggle-btn" onclick="showModalView(this, 'json')">JSON</button>
                            <button class="toggle-btn" onclick="showModalView(this, 'yaml')">YAML</button>
                        </div>
                        <div class="modal-content-area">
                            <div class="view-content summary active">
                                <h4>Resource Summary</h4>
                                <p>Detailed information for ${type}: ${name}</p>
                            </div>
                            <div class="view-content json">
                                <pre><code>${JSON.stringify(JSON.parse(data), null, 2)}</code></pre>
                            </div>
                            <div class="view-content yaml">
                                <pre><code>${jsyaml.dump(JSON.parse(data))}</code></pre>
                            </div>
                        </div>
                    </div>
                </div>
            `;
            document.body.appendChild(modal);
        }
        
        function closeModal() {
            const modal = document.querySelector('.resource-modal');
            if (modal) {
                document.body.removeChild(modal);
            }
        }
        
        function showModalView(button, viewType) {
            const modalBody = button.closest('.modal-body');
            const allButtons = modalBody.querySelectorAll('.toggle-btn');
            const allViews = modalBody.querySelectorAll('.view-content');
            
            // Remove active class from all buttons
            allButtons.forEach(btn => btn.classList.remove('active'));
            
            // Hide all views
            allViews.forEach(view => view.classList.remove('active'));
            
            // Activate clicked button
            button.classList.add('active');
            
            // Show selected view
            const targetView = modalBody.querySelector(`.view-content.${viewType}`);
            if (targetView) {
                targetView.classList.add('active');
            }
        }
        '''
    
    def _calculate_age(self, timestamp_str: str) -> str:
        """Calculate age from timestamp string."""
        if not timestamp_str:
            return 'N/A'
        
        try:
            timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            now = datetime.utcnow().replace(tzinfo=timestamp.tzinfo)
            delta = now - timestamp
            
            days = delta.days
            hours = delta.seconds // 3600
            minutes = (delta.seconds % 3600) // 60
            
            if days > 0:
                return f"{days}d"
            elif hours > 0:
                return f"{hours}h"
            else:
                return f"{minutes}m"
        except:
            return 'N/A'
    
    def _format_dict(self, d: Dict[str, Any]) -> str:
        """Format dictionary for display."""
        if not d:
            return 'None'
        
        items = []
        for k, v in d.items():
            if len(str(v)) > 20:
                v = str(v)[:20] + '...'
            items.append(f"{k}={v}")
        
        result = ', '.join(items)
        return result[:100] + '...' if len(result) > 100 else result
    
    def _to_yaml(self, data: Dict[str, Any]) -> str:
        """Convert data to YAML format."""
        try:
            return yaml.dump(data, default_flow_style=False, sort_keys=False, indent=2)
        except Exception:
            return "Error generating YAML"
    
    def save_html(self, output_file: str):
        """Save HTML dashboard to file."""
        html_content = self.generate_html_dashboard()
        
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"HTML dashboard saved to: {output_file}")


def main():
    parser = argparse.ArgumentParser(description='Generate HTML visualization for EKS export data')
    parser.add_argument('export_file', help='Path to the JSON export file')
    parser.add_argument('--output', '-o', help='Output HTML file path',
                       default='eks-dashboard-{timestamp}.html')
    
    args = parser.parse_args()
    
    # Replace timestamp placeholder
    if '{timestamp}' in args.output:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        args.output = args.output.replace('{timestamp}', timestamp)
    
    try:
        with open(args.export_file, 'r') as f:
            export_data = json.load(f)
        
        visualizer = EKSVisualizationGenerator(export_data)
        visualizer.save_html(args.output)
        
        print(f"\\nVisualization completed successfully!")
        print(f"Open {args.output} in your web browser to view the dashboard.")
        
    except Exception as e:
        print(f"Visualization failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()