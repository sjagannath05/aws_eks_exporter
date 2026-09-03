# EKS Configuration Export and Visualization Tool

A comprehensive tool for exporting and visualizing AWS EKS cluster configurations. This advanced tool captures **all** Kubernetes resources across **40+ resource types** including workloads, networking, storage, security, and EKS-specific metadata, then generates a fully interactive HTML dashboard with rich operational insights.

## Features

### ✅ Complete Resource Export (40+ Resource Types)

#### **Workloads**
- **Pods**: Complete pod information including containers, volumes, status, and resource usage
- **PodTemplates**: Pod template configurations and specifications
- **ReplicaSets**: Replica set status, desired vs actual replicas, and owner references
- **Deployments**: Deployment status, replica counts, rolling update strategies
- **StatefulSets**: StatefulSet configurations, persistent volume claims, and scaling
- **DaemonSets**: DaemonSet configurations, node scheduling, and status
- **Jobs**: Job completion status, parallelism, and failure policies
- **CronJobs**: Scheduled job configurations, schedules, and execution history
- **PriorityClasses**: Pod priority configurations and global defaults
- **HorizontalPodAutoscalers**: HPA metrics, scaling policies, and current status

#### **Cluster Resources**
- **Nodes**: Node capacity, conditions, system information, and describe details
- **Namespaces**: Namespace metadata, labels, resource quotas, and status
- **APIServices**: Kubernetes API service registrations and availability
- **Leases**: Coordination and leader election lease information
- **RuntimeClasses**: Container runtime configurations and scheduling constraints
- **FlowSchemas**: API priority and fairness flow control (when available)
- **PriorityLevelConfigurations**: API request priority levels and queuing

#### **Service and Networking**
- **Services**: Service configurations, endpoints, load balancer details
- **Endpoints**: Service endpoint addresses and port mappings
- **EndpointSlices**: Modern endpoint discovery and load balancing
- **Ingresses**: Ingress rules, TLS configurations, and routing
- **IngressClasses**: Ingress controller configurations and parameters

#### **Config and Secrets**
- **ConfigMaps**: Configuration data keys, metadata, and describe information
- **Secrets**: Secret metadata and keys (data values hidden for security)

#### **Storage**
- **PersistentVolumeClaims**: PVC status, storage requests, and binding information
- **PersistentVolumes**: PV configurations, capacity, and reclaim policies
- **StorageClasses**: Dynamic provisioning configurations and parameters
- **VolumeAttachments**: Volume attachment status and node assignments
- **CSIDrivers**: Container Storage Interface driver configurations
- **CSINodes**: CSI node driver information and topology
- **CSIStorageCapacities**: Storage capacity information for CSI drivers

#### **Authentication & Authorization**
- **ServiceAccounts**: Service account configurations and token automounting
- **ClusterRoles**: Cluster-wide RBAC role definitions and permissions
- **ClusterRoleBindings**: Cluster role bindings to users and groups
- **Roles**: Namespace-scoped RBAC role definitions
- **RoleBindings**: Role bindings within namespaces

#### **Policy**
- **LimitRanges**: Resource limit and request constraints
- **ResourceQuotas**: Namespace resource consumption limits
- **NetworkPolicies**: Network traffic ingress/egress rules
- **PodDisruptionBudgets**: Voluntary disruption protection policies

#### **Extensions**
- **CustomResourceDefinitions**: Custom resource type definitions
- **MutatingWebhookConfigurations**: Admission controller webhook configurations
- **ValidatingWebhookConfigurations**: Validation webhook configurations

### ✅ EKS-Specific Information
- Cluster basic information (version, status, endpoint)
- Node groups configuration and scaling settings
- VPC and networking configuration
- Security group associations
- IAM roles and policies (metadata)

### ✅ Advanced Interactive HTML Dashboard

#### **🎯 Enhanced Navigation & UX**
- **Clickable Dashboard Cards**: Click resource summary cards to jump directly to resource sections
- **Responsive Design**: Optimized for desktop, tablet, and mobile devices
- **Comprehensive Tabbed Interface**: 10+ organized tabs for different resource types
- **Real-time Search & Filter**: Instant search across all resource names and properties
- **Advanced Filtering**: Namespace-based filtering and custom search capabilities

#### **📊 Rich Resource Detail Views** 
Every resource now includes a **4-tab detail system**:
- **📋 Summary Tab**: Key metrics, status overview, and quick insights
- **💾 JSON Tab**: Complete raw API response data in formatted JSON
- **📝 YAML Tab**: Human-readable YAML resource definitions
- **🔍 Describe Tab**: Full `kubectl describe` output with operational details and events

#### **🎨 Visual Enhancements**
- **Interactive Elements**: Expandable rows, modal detail views, and hover effects
- **Status Indicators**: Color-coded badges for health, readiness, and operational status
- **Action Buttons**: Direct access to detailed views and resource-specific actions
- **Responsive Tables**: Auto-sizing columns and mobile-friendly layouts

#### **⚡ Operational Intelligence**
- **kubectl Integration**: Rich operational data from `kubectl describe` commands
- **Event Information**: Resource events, status changes, and system messages
- **Capacity Insights**: Node resource utilization, storage capacity, and limits
- **Health Monitoring**: Resource conditions, readiness probes, and failure states

### ✅ Multiple Output Formats
- **JSON**: Structured data for programmatic processing
- **YAML**: Human-readable configuration format
- **HTML**: Interactive dashboard for visual exploration
- **Summary Report**: Text-based cluster overview

## Prerequisites

- **AWS CLI**: Configured with appropriate permissions
- **kubectl**: Configured for the target EKS cluster
- **Python 3.7+**: With required packages (see requirements.txt)
- **EKS Permissions**: Read access to EKS clusters and Kubernetes resources

### Required AWS Permissions
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "eks:DescribeCluster",
                "eks:ListNodegroups",
                "eks:DescribeNodegroup",
                "eks:ListClusters"
            ],
            "Resource": "*"
        }
    ]
}
```

### Required Kubernetes Permissions
The tool requires read access to all Kubernetes resources. A cluster-admin role or equivalent read permissions are recommended.

## Installation

1. **Clone or download the tool files**:
   ```bash
   # Files needed:
   # - eks-config-exporter.py
   # - eks-visualizer.py  
   # - eks-export-and-visualize.sh
   # - requirements.txt
   ```

2. **Install Python dependencies**:
   ```bash
   pip3 install -r requirements.txt
   ```

3. **Make the shell script executable**:
   ```bash
   chmod +x eks-export-and-visualize.sh
   ```

## Usage

### Quick Start

**Export the current kubeconfig context** (cluster name, region and account are derived from it):
```bash
export KUBECONFIG=~/.kube/my-clusters.kubeconfig
./eks-export-and-visualize.sh
```

**Export by cluster name** (writes a kubeconfig via `aws eks update-kubeconfig` only if none exists yet):
```bash
./eks-export-and-visualize.sh -c my-eks-cluster -r us-west-2
```

### Multi-Cluster Kubeconfigs

A single kubeconfig with several contexts (for example `ch1`, `dc2`, `fr5`, `sy1`) is fully supported:

```bash
# See what each context resolves to (current-context marked with *)
./eks-export-and-visualize.sh -k ~/.kube/sites.kubeconfig --list-contexts

# Export one site
./eks-export-and-visualize.sh -k ~/.kube/sites.kubeconfig --context fr5

# Export every unique cluster in the file (one JSON + dashboard per context)
./eks-export-and-visualize.sh -k ~/.kube/sites.kubeconfig --all-contexts
```

Rules:
- `--context` selects the kubeconfig context; the EKS cluster name, region and account come from its cluster ARN (or the `aws eks get-token` exec args). Passing `-c`/`-r` values that contradict the context is an error.
- `--all-contexts` exports each unique cluster once. Contexts pointing at the same cluster are collapsed to the shortest name. Output files get a `-<context>` suffix; one failing context does not stop the rest.
- The kubeconfig you point at (via `-k` or `$KUBECONFIG`) is never modified.
- `kubectl describe` output uses the same kubeconfig and context as the API export.
- If the EKS API lookup fails (wrong region, no permission, no credentials) the export stops with an error instead of producing a file with empty cluster metadata. Use `--skip-aws` to export Kubernetes resources without any EKS API calls.

### Command Line Options

```bash
Usage: ./eks-export-and-visualize.sh [-c CLUSTER_NAME] [OPTIONS]

Cluster selection (one of):
  -c, --cluster CLUSTER_NAME    EKS cluster name (optional when the kubeconfig context identifies it)
  --context NAME                kubeconfig context to export (default: current-context)
  --all-contexts                export every unique cluster in the kubeconfig
  --list-contexts               list kubeconfig contexts with derived EKS identity and exit

Options:
  -r, --region REGION          AWS region (default: from kubeconfig context, then AWS config)
  -o, --output OUTPUT_DIR      Output directory (default: exported)
  -k, --kubeconfig PATH        Path to kubeconfig file (default: $KUBECONFIG, then ~/.kube/config)
  --skip-aws                   Do not call the EKS API; export Kubernetes resources only
  -f, --format FORMAT          Export format: json or yaml (default: json)
  --no-html                    Skip HTML dashboard generation
  --no-summary                 Skip summary report generation
  -h, --help                   Show this help message
```

### Usage Examples

**Export with custom region and output directory**:
```bash
./eks-export-and-visualize.sh -c production-cluster -r us-west-2 -o /tmp/eks-export
```

**Export in YAML format without HTML dashboard**:
```bash
./eks-export-and-visualize.sh -c dev-cluster -f yaml --no-html
```

**Export using specific kubeconfig**:
```bash
./eks-export-and-visualize.sh -c staging-cluster -k ~/.kube/staging-config
```

**Production export with all features**:
```bash
./eks-export-and-visualize.sh -c prod-eks-cluster -r us-east-1 -o ./production-export
```

### Direct Python Usage

**Export cluster configuration**:
```bash
# Name/region derived from the kubeconfig context
python3 eks-config-exporter.py --kubeconfig ~/.kube/sites.kubeconfig --context fr5 --output fr5.json --summary

# Explicit name/region (validated against the kubeconfig if it is an EKS context)
python3 eks-config-exporter.py my-cluster --region us-west-2 --output cluster-export.json --summary

# All contexts; {context} placeholder is optional (default: -<context> suffix)
python3 eks-config-exporter.py --kubeconfig ~/.kube/sites.kubeconfig --all-contexts --output 'exports/{context}.json'

# Inspect contexts
python3 eks-config-exporter.py --kubeconfig ~/.kube/sites.kubeconfig --list-contexts
```

**Generate HTML dashboard**:
```bash
python3 eks-visualizer.py cluster-export.json --output dashboard.html
```

## Output Files

The tool generates several output files:

### 1. Export Data File
- **Format**: JSON or YAML
- **Contains**: Complete cluster configuration and resource data
- **Example**: `eks-export-my-cluster-20240104_143022.json`

### 2. HTML Dashboard
- **Format**: Self-contained HTML file
- **Contains**: Interactive web dashboard
- **Example**: `eks-dashboard-my-cluster-20240104_143022.html`

### 3. Summary Report
- **Format**: Plain text
- **Contains**: Quick cluster overview and resource counts
- **Example**: `eks-export-my-cluster-20240104_143022_summary.txt`

## Dashboard Features

### Navigation
- **Overview Tab**: Cluster information and node groups
- **Pods Tab**: All pods with filtering and search
- **Services Tab**: Service configurations and endpoints
- **Deployments Tab**: Deployment status and replicas
- **Nodes Tab**: Node capacity and conditions
- **Namespaces Tab**: Namespace information
- **Storage Tab**: Persistent volumes and claims

### Interactive Features
- **Search**: Real-time text search across resource names
- **Namespace Filter**: Filter pods by namespace
- **Expandable Details**: Click rows to see detailed information
- **Status Badges**: Color-coded status indicators
- **Responsive Design**: Works on desktop and mobile

### Dashboard Screenshot Examples

The dashboard provides:
- 📊 **Summary Cards**: Quick resource counts
- 🔍 **Search & Filter**: Find resources quickly  
- 📱 **Responsive Design**: Works on all devices
- 🎨 **Status Indicators**: Visual status representation
- 📋 **Detailed Views**: Complete resource information

## Security Considerations

- **Secrets**: Only metadata and keys are exported, not secret values
- **Sensitive Data**: Review output files before sharing
- **Access Control**: Ensure appropriate RBAC permissions
- **Local Storage**: Export files contain cluster configuration data

## Troubleshooting

### Common Issues

**AWS Authentication Error**:
```bash
# Check AWS credentials
aws sts get-caller-identity

# Update AWS credentials
aws configure
```

**kubectl Access Error**:
```bash
# Check which context/cluster the tool will use
python3 eks-config-exporter.py --list-contexts

# Test access for that context
kubectl --context <name> get nodes

# Or fetch a fresh kubeconfig for a cluster
aws eks update-kubeconfig --region us-west-2 --name my-cluster
```

**"EKS API lookup failed" / "does not match kubeconfig context"**:
The cluster name or region you passed disagrees with the kubeconfig context, or your AWS credentials are for a different account than the one in the cluster ARN. Drop `-c`/`-r` and let them be derived, pick the right `--context`, or use `--skip-aws` to export without EKS metadata.

**Python Dependencies Error**:
```bash
# Install dependencies
pip3 install -r requirements.txt --user

# Or install individually
pip3 install boto3 kubernetes PyYAML
```

**Permission Denied Error**:
```bash
# Make script executable
chmod +x eks-export-and-visualize.sh

# Check file permissions
ls -la eks-export-and-visualize.sh
```

### Debugging

Enable verbose output for troubleshooting:
```bash
# Run with detailed logging
python3 eks-config-exporter.py my-cluster --region us-west-2 -v
```

## Advanced Usage

### Custom Resource Types

The tool automatically discovers and exports custom resources. To add specific CRDs:

1. Modify `eks-config-exporter.py`
2. Add custom resource export methods
3. Update the `export_all_resources()` method

### Automation

**Scheduled Exports**:
```bash
# Add to cron for daily exports
0 2 * * * /path/to/eks-export-and-visualize.sh -c prod-cluster -o /backup/eks-exports
```

**CI/CD Integration**:
```yaml
# GitHub Actions example
- name: Export EKS Configuration
  run: |
    ./eks-export-and-visualize.sh -c ${{ env.CLUSTER_NAME }} -r ${{ env.AWS_REGION }}
    
- name: Archive Export
  uses: actions/upload-artifact@v3
  with:
    name: eks-export
    path: exported/
```

## File Structure

```
eks-tool/
├── eks-config-exporter.py      # Main export script
├── eks-visualizer.py           # HTML dashboard generator
├── eks-export-and-visualize.sh # Wrapper script
├── kubeconfig_utils.py         # Kubeconfig parsing: contexts, EKS ARN -> name/region/account
├── tests/                      # pytest unit tests (python -m pytest tests)
├── requirements.txt            # Python dependencies
├── README.md                   # This documentation
└── exported/                   # Output directory (created)
    ├── eks-export-*.json       # Export data
    ├── eks-dashboard-*.html     # HTML dashboard
    └── *_summary.txt           # Summary reports
```

## Contributing

To enhance the tool:

1. **Add Resource Types**: Extend the exporter for new Kubernetes resources
2. **Improve Visualization**: Enhance the HTML dashboard with new features
3. **Add Export Formats**: Support additional output formats
4. **Performance**: Optimize for large clusters

## License

This tool is provided as-is for defensive security and cluster management purposes. Use in accordance with your organization's security policies.

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review AWS and Kubernetes permissions
3. Verify prerequisites are installed
4. Test with a smaller cluster first

---

**Note**: This tool is designed for defensive security analysis and cluster management. It exports comprehensive cluster configuration data for visualization and analysis purposes.