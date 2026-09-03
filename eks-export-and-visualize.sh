#!/bin/bash

# EKS Configuration Export and Visualization Tool
# This script exports an EKS cluster configuration and generates an interactive HTML dashboard

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
CLUSTER_NAME=""
REGION=""
OUTPUT_DIR="exported"
KUBECONFIG_PATH=""          # from -k; $KUBECONFIG env is honoured when this is empty
KUBE_CONTEXT=""
ALL_CONTEXTS=false
SKIP_AWS=false
LIST_CONTEXTS=false
EXPORT_FORMAT="json"
GENERATE_HTML=true
GENERATE_SUMMARY=true
VENV_DIR=""
INCLUDE_AWS_RESOURCES=false
INCLUDE_CUSTOM_CRDS=false
SPLIT_BY_TYPE=false
GENERATE_RESTORE_SCRIPTS=false
EXCLUDE_SECRETS_DATA=false
NAMESPACE_FILTER=""
RESOURCE_TYPE_FILTER=""

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show usage
show_usage() {
    cat << EOF
EKS Configuration Export and Visualization Tool

Usage: $0 [-c CLUSTER_NAME] [OPTIONS]

Cluster selection (one of):
  -c, --cluster CLUSTER_NAME    EKS cluster name. Optional when a kubeconfig context
                                points at an EKS cluster: name, region and account are
                                derived from it. Required to run 'aws eks update-kubeconfig'
                                when no kubeconfig is available.
  --context NAME                kubeconfig context to export (default: current-context)
  --all-contexts                export every unique cluster in the kubeconfig
  --list-contexts               list kubeconfig contexts with derived EKS identity and exit

Options:
  -r, --region REGION          AWS region (default: from AWS config)
  -o, --output OUTPUT_DIR      Output directory (default: exported)
  -k, --kubeconfig PATH        Path to kubeconfig file (default: \$KUBECONFIG, then ~/.kube/config)
  --skip-aws                   Do not call the EKS API; export Kubernetes resources only
  -f, --format FORMAT          Export format: json or yaml (default: json)
  -v, --venv VENV_DIR          Path to virtual environment directory
  --include-aws-resources      Include AWS-specific resources (ENIConfigs, etc.)
  --include-custom-crds        Include all custom resource instances
  --split-by-type              Generate individual YAML files by resource type
  --generate-restore-scripts   Generate restoration scripts and guides
  --exclude-secrets-data       Exclude secret data values (metadata only)
  --namespace-filter FILTER    Comma-separated list of namespaces to include
  --resource-type FILTER       Comma-separated list of resource types
  --no-html                    Skip HTML dashboard generation
  --no-summary                 Skip summary report generation
  -h, --help                   Show this help message

Examples:
  # Basic export
  $0 -c my-eks-cluster

  # Export with AWS resources and custom CRDs
  $0 -c my-eks-cluster --include-aws-resources --include-custom-crds

  # Export for complete restoration
  $0 -c my-eks-cluster --include-aws-resources --split-by-type --generate-restore-scripts

  # Export in YAML format without HTML dashboard
  $0 -c my-eks-cluster -f yaml --no-html

  # Export specific namespaces only
  $0 -c my-eks-cluster --namespace-filter=default,kube-system

  # Export using virtual environment
  $0 -c my-eks-cluster -v /path/to/venv

Prerequisites:
  - AWS CLI configured with appropriate permissions
  - kubectl configured for the target cluster
  - Python 3.7+ with required packages (see requirements.txt)
  - EKS cluster access permissions

EOF
}

# Function to activate virtual environment if specified
activate_venv() {
    if [ -n "$VENV_DIR" ]; then
        if [ -f "$VENV_DIR/bin/activate" ]; then
            print_info "Activating virtual environment: $VENV_DIR"
            source "$VENV_DIR/bin/activate"
            print_success "Virtual environment activated"
        else
            print_error "Virtual environment not found at: $VENV_DIR"
            print_error "Make sure the path points to a valid Python virtual environment"
            exit 1
        fi
    fi
}

# Function to check prerequisites
check_prerequisites() {
    print_info "Checking prerequisites..."
    
    # Activate virtual environment if specified
    activate_venv
    
    # Check if Python 3 is installed
    if ! command -v python3 &> /dev/null; then
        print_error "Python 3 is required but not installed"
        exit 1
    fi
    
    # Check if AWS CLI is installed and configured
    if ! command -v aws &> /dev/null; then
        print_warning "AWS CLI not found. Make sure AWS credentials are configured"
    else
        if ! aws sts get-caller-identity &> /dev/null; then
            print_error "AWS CLI not configured or no valid credentials"
            exit 1
        fi
    fi
    
    # Check if kubectl is installed
    if ! command -v kubectl &> /dev/null; then
        print_error "kubectl is required but not installed"
        exit 1
    fi
    
    # Check if required Python packages are installed
    if ! python3 -c "import boto3, kubernetes, yaml" 2>/dev/null; then
        print_info "Installing required Python packages..."
        if [ -n "$VENV_DIR" ]; then
            # Install in virtual environment
            if [ -f "requirements.txt" ]; then
                pip install -r requirements.txt
            else
                pip install boto3 kubernetes PyYAML
            fi
        else
            # Install with --user flag if no venv
            if [ -f "requirements.txt" ]; then
                pip3 install -r requirements.txt --user
            else
                pip3 install boto3 kubernetes PyYAML --user
            fi
        fi
    fi
    
    print_success "Prerequisites check completed"
}

# Function to validate cluster access
validate_cluster_access() {
    print_info "Validating cluster access..."

    # Refresh ~/.kube/config for -c CLUSTER_NAME (previous behaviour) unless the user
    # pointed us at a kubeconfig (-k / $KUBECONFIG) or a context; those are never modified.
    if [ -z "$KUBECONFIG" ] && [ -z "$KUBE_CONTEXT" ] && [ "$ALL_CONTEXTS" = false ]; then
        if [ -z "$CLUSTER_NAME" ] && [ ! -f "$HOME/.kube/config" ]; then
            print_error "No kubeconfig found and no cluster name given; pass -c CLUSTER_NAME or -k PATH"
            exit 1
        fi
    fi
    if [ -n "$CLUSTER_NAME" ] && [ -z "$KUBECONFIG" ] && [ -z "$KUBE_CONTEXT" ]; then
        print_info "Updating ~/.kube/config for cluster: $CLUSTER_NAME"
        if [ -n "$REGION" ]; then
            aws eks update-kubeconfig --region "$REGION" --name "$CLUSTER_NAME"
        else
            aws eks update-kubeconfig --name "$CLUSTER_NAME"
        fi
    fi

    if [ "$ALL_CONTEXTS" = true ]; then
        if [ -z "$(kubectl config get-contexts -o name 2>/dev/null)" ]; then
            print_error "kubeconfig has no contexts"
            exit 1
        fi
        print_success "Kubeconfig contexts found (access is validated per context during export)"
        return
    fi

    local ctx_args=()
    [ -n "$KUBE_CONTEXT" ] && ctx_args=(--context "$KUBE_CONTEXT")
    if ! kubectl "${ctx_args[@]}" get nodes &> /dev/null; then
        print_error "Cannot access cluster${KUBE_CONTEXT:+ (context: $KUBE_CONTEXT)}. Check your kubeconfig and permissions"
        exit 1
    fi

    print_success "Cluster access validated"
}

# Function to export cluster configuration
export_cluster_config() {
    print_info "Exporting EKS cluster configuration..."
    
    # Create output directory
    mkdir -p "$OUTPUT_DIR"
    
    # Generate timestamp for unique filenames
    TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
    if [ "$ALL_CONTEXTS" = true ]; then
        EXPORT_LABEL="all"
    else
        EXPORT_LABEL="${CLUSTER_NAME:-${KUBE_CONTEXT:-cluster}}"
    fi
    EXPORT_FILE="$OUTPUT_DIR/eks-export-${EXPORT_LABEL}-${TIMESTAMP}.$EXPORT_FORMAT"
    
    # Build export command
    EXPORT_CMD="python3 eks-config-exporter.py"
    
    if [ -n "$CLUSTER_NAME" ]; then
        EXPORT_CMD="$EXPORT_CMD $CLUSTER_NAME"
    fi
    
    if [ -n "$REGION" ]; then
        EXPORT_CMD="$EXPORT_CMD --region $REGION"
    fi
    
    if [ -n "$KUBECONFIG_PATH" ]; then
        EXPORT_CMD="$EXPORT_CMD --kubeconfig $KUBECONFIG_PATH"
    fi
    
    if [ -n "$KUBE_CONTEXT" ]; then
        EXPORT_CMD="$EXPORT_CMD --context $KUBE_CONTEXT"
    fi
    
    if [ "$ALL_CONTEXTS" = true ]; then
        EXPORT_CMD="$EXPORT_CMD --all-contexts"
    fi
    
    if [ "$SKIP_AWS" = true ]; then
        EXPORT_CMD="$EXPORT_CMD --skip-aws"
    fi
    
    EXPORT_CMD="$EXPORT_CMD --output $EXPORT_FILE --format $EXPORT_FORMAT"
    
    if [ "$GENERATE_SUMMARY" = true ]; then
        EXPORT_CMD="$EXPORT_CMD --summary"
    fi
    
    # Add enhanced export options
    if [ "$INCLUDE_AWS_RESOURCES" = true ]; then
        EXPORT_CMD="$EXPORT_CMD --include-aws-resources"
    fi
    
    if [ "$INCLUDE_CUSTOM_CRDS" = true ]; then
        EXPORT_CMD="$EXPORT_CMD --include-custom-crds"
    fi
    
    if [ "$SPLIT_BY_TYPE" = true ]; then
        EXPORT_CMD="$EXPORT_CMD --split-by-type"
    fi
    
    if [ "$GENERATE_RESTORE_SCRIPTS" = true ]; then
        EXPORT_CMD="$EXPORT_CMD --generate-restore-scripts"
    fi
    
    if [ "$EXCLUDE_SECRETS_DATA" = true ]; then
        EXPORT_CMD="$EXPORT_CMD --exclude-secrets-data"
    fi
    
    if [ -n "$NAMESPACE_FILTER" ]; then
        EXPORT_CMD="$EXPORT_CMD --namespace-filter $NAMESPACE_FILTER"
    fi
    
    if [ -n "$RESOURCE_TYPE_FILTER" ]; then
        EXPORT_CMD="$EXPORT_CMD --resource-type $RESOURCE_TYPE_FILTER"
    fi
    
    EXPORT_CMD="$EXPORT_CMD --output-dir $OUTPUT_DIR"
    
    # Execute export
    if eval "$EXPORT_CMD"; then
        if [ "$ALL_CONTEXTS" = true ]; then
            # Python wrote one file per context: eks-export-all-<ts>-<context>.<fmt>
            ls "$OUTPUT_DIR"/eks-export-"${EXPORT_LABEL}"-"${TIMESTAMP}"-*."$EXPORT_FORMAT" > "$OUTPUT_DIR/.last_export"
            print_success "Configuration exported: $(wc -l < "$OUTPUT_DIR/.last_export" | tr -d ' ') file(s)"
        else
            print_success "Configuration exported to: $EXPORT_FILE"
            echo "$EXPORT_FILE" > "$OUTPUT_DIR/.last_export"
        fi
    else
        print_error "Failed to export cluster configuration"
        exit 1
    fi
}

# Function to generate HTML dashboard (one per export file)
generate_html_dashboard() {
    if [ "$GENERATE_HTML" = false ]; then
        return
    fi
    
    print_info "Generating HTML dashboard(s)..."
    
    if [ ! -s "$OUTPUT_DIR/.last_export" ]; then
        print_error "No export file found"
        return
    fi
    
    : > "$OUTPUT_DIR/.last_dashboard"
    local export_file html_file base
    while IFS= read -r export_file; do
        [ -z "$export_file" ] && continue
        base=$(basename "$export_file")
        base="${base%.*}"
        html_file="$OUTPUT_DIR/${base/#eks-export-/eks-dashboard-}.html"
        if python3 eks-visualizer.py "$export_file" --output "$html_file"; then
            print_success "HTML dashboard generated: $html_file"
            echo "$html_file" >> "$OUTPUT_DIR/.last_dashboard"
        else
            print_error "Failed to generate HTML dashboard for $export_file"
        fi
    done < "$OUTPUT_DIR/.last_export"
    
    # Open in browser only for a single dashboard; with --all-contexts just list them
    if [ "$(wc -l < "$OUTPUT_DIR/.last_dashboard" | tr -d ' ')" = "1" ]; then
        html_file=$(cat "$OUTPUT_DIR/.last_dashboard")
        if command -v open &> /dev/null; then
            print_info "Opening dashboard in browser..."
            open "$html_file" || true
        elif command -v xdg-open &> /dev/null; then
            print_info "Opening dashboard in browser..."
            xdg-open "$html_file" || true
        fi
    fi
}

# Function to show export summary
show_export_summary() {
    print_success "EKS Export Completed Successfully!"
    echo
    print_info "Export Details:"
    if [ "$ALL_CONTEXTS" = true ]; then
        echo "  Clusters: all contexts in kubeconfig"
    else
        echo "  Cluster: ${CLUSTER_NAME:-(derived from kubeconfig context${KUBE_CONTEXT:+ $KUBE_CONTEXT})}"
        echo "  Region: ${REGION:-(derived from kubeconfig context)}"
    fi
    echo "  Kubeconfig: ${KUBECONFIG:-~/.kube/config}"
    echo "  Output Directory: $OUTPUT_DIR"
    echo "  Format: $EXPORT_FORMAT"
    
    # Show enhanced options used
    if [ "$INCLUDE_AWS_RESOURCES" = true ]; then
        echo "  AWS Resources: Included"
    fi
    
    if [ "$INCLUDE_CUSTOM_CRDS" = true ]; then
        echo "  Custom CRDs: Included"
    fi
    
    if [ "$SPLIT_BY_TYPE" = true ]; then
        echo "  YAML Split: Enabled"
    fi
    
    if [ "$GENERATE_RESTORE_SCRIPTS" = true ]; then
        echo "  Restore Scripts: Generated"
    fi
    
    echo
    
    if [ -s "$OUTPUT_DIR/.last_export" ]; then
        while IFS= read -r f; do
            [ -n "$f" ] && echo "  Export File: $f ($(du -h "$f" | cut -f1))"
        done < "$OUTPUT_DIR/.last_export"
    fi
    
    if [ -s "$OUTPUT_DIR/.last_dashboard" ]; then
        while IFS= read -r f; do
            [ -n "$f" ] && echo "  HTML Dashboard: $f"
        done < "$OUTPUT_DIR/.last_dashboard"
    fi
    
    echo
    print_info "Next Steps:"
    echo "  1. Review the exported data in the JSON/YAML file"
    echo "  2. Open the HTML dashboard in your web browser"
    echo "  3. Use the dashboard to explore your cluster configuration"
    
    if ls "$OUTPUT_DIR"/*summary*.txt >/dev/null 2>&1; then
        echo "  4. Check the summary report for quick insights"
    fi
    
    # Check for kubectl restore directory
    if ls "$OUTPUT_DIR"/kubectl-restore-* >/dev/null 2>&1; then
        RESTORE_DIR=$(ls -d "$OUTPUT_DIR"/kubectl-restore-* | head -1)
        echo "  5. For cluster restoration, see: $RESTORE_DIR"
        echo "     cd \"$RESTORE_DIR\" && ./restore-cluster.sh --validate-dependencies"
    fi
    
    # Show if AWS/CNI resources were found
    if [ "$INCLUDE_AWS_RESOURCES" = true ]; then
        echo ""
        print_info "Enhanced Features:"
        echo "  ✅ AWS VPC CNI resources (ENIConfigs, SecurityGroupPolicies)"
        echo "  ✅ Multus CNI resources (NetworkAttachmentDefinitions)"
        echo "  ✅ Karpenter resources (NodePools, NodeClasses, NodeClaims)"
        echo "  ✅ Dynamic custom resource discovery"
        echo "  ✅ Multi-interface pod configuration support"
    fi
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--cluster)
            CLUSTER_NAME="$2"
            shift 2
            ;;
        -r|--region)
            REGION="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -k|--kubeconfig)
            KUBECONFIG_PATH="$2"
            shift 2
            ;;
        --context)
            KUBE_CONTEXT="$2"
            shift 2
            ;;
        --all-contexts)
            ALL_CONTEXTS=true
            shift
            ;;
        --list-contexts)
            LIST_CONTEXTS=true
            shift
            ;;
        --skip-aws)
            SKIP_AWS=true
            shift
            ;;
        -f|--format)
            EXPORT_FORMAT="$2"
            shift 2
            ;;
        -v|--venv)
            VENV_DIR="$2"
            shift 2
            ;;
        --include-aws-resources)
            INCLUDE_AWS_RESOURCES=true
            shift
            ;;
        --include-custom-crds)
            INCLUDE_CUSTOM_CRDS=true
            shift
            ;;
        --split-by-type)
            SPLIT_BY_TYPE=true
            shift
            ;;
        --generate-restore-scripts)
            GENERATE_RESTORE_SCRIPTS=true
            shift
            ;;
        --exclude-secrets-data)
            EXCLUDE_SECRETS_DATA=true
            shift
            ;;
        --namespace-filter)
            NAMESPACE_FILTER="$2"
            shift 2
            ;;
        --resource-type)
            RESOURCE_TYPE_FILTER="$2"
            shift 2
            ;;
        --no-html)
            GENERATE_HTML=false
            shift
            ;;
        --no-summary)
            GENERATE_SUMMARY=false
            shift
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Make an explicit -k visible to kubectl, aws and python alike.
if [ -n "$KUBECONFIG_PATH" ]; then
    if [ ! -f "$KUBECONFIG_PATH" ]; then
        print_error "Kubeconfig file not found: $KUBECONFIG_PATH"
        exit 1
    fi
    export KUBECONFIG="$KUBECONFIG_PATH"
fi

if [ "$ALL_CONTEXTS" = true ] && { [ -n "$CLUSTER_NAME" ] || [ -n "$REGION" ] || [ -n "$KUBE_CONTEXT" ]; }; then
    print_error "--all-contexts derives cluster, region and context per kubeconfig entry; do not combine with -c, -r or --context"
    exit 1
fi

# Validate format
if [[ "$EXPORT_FORMAT" != "json" && "$EXPORT_FORMAT" != "yaml" ]]; then
    print_error "Export format must be 'json' or 'yaml'"
    exit 1
fi

# Main execution
main() {
    if [ "$LIST_CONTEXTS" = true ]; then
        activate_venv
        exec python3 eks-config-exporter.py --list-contexts ${KUBECONFIG_PATH:+--kubeconfig "$KUBECONFIG_PATH"}
    fi

    if [ "$ALL_CONTEXTS" = true ]; then
        print_info "Starting EKS Configuration Export for all kubeconfig contexts"
    else
        print_info "Starting EKS Configuration Export for cluster: ${CLUSTER_NAME:-(from kubeconfig context ${KUBE_CONTEXT:-current})}"
    fi
    echo
    
    check_prerequisites
    validate_cluster_access
    export_cluster_config
    generate_html_dashboard
    show_export_summary
}

# Run main function
main