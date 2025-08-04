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
KUBECONFIG=""
EXPORT_FORMAT="json"
GENERATE_HTML=true
GENERATE_SUMMARY=true
VENV_DIR=""

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

Usage: $0 -c CLUSTER_NAME [OPTIONS]

Required:
  -c, --cluster CLUSTER_NAME    EKS cluster name

Options:
  -r, --region REGION          AWS region (default: from AWS config)
  -o, --output OUTPUT_DIR      Output directory (default: exported)
  -k, --kubeconfig PATH        Path to kubeconfig file
  -f, --format FORMAT          Export format: json or yaml (default: json)
  -v, --venv VENV_DIR          Path to virtual environment directory
  --no-html                    Skip HTML dashboard generation
  --no-summary                 Skip summary report generation
  -h, --help                   Show this help message

Examples:
  # Basic export
  $0 -c my-eks-cluster

  # Export with custom region and output directory
  $0 -c my-eks-cluster -r us-west-2 -o /tmp/eks-export

  # Export in YAML format without HTML dashboard
  $0 -c my-eks-cluster -f yaml --no-html

  # Export using specific kubeconfig
  $0 -c my-eks-cluster -k ~/.kube/my-cluster-config

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
    
    # Update kubeconfig if needed
    if [ -z "$KUBECONFIG" ]; then
        print_info "Updating kubeconfig for cluster: $CLUSTER_NAME"
        if [ -n "$REGION" ]; then
            aws eks update-kubeconfig --region "$REGION" --name "$CLUSTER_NAME"
        else
            aws eks update-kubeconfig --name "$CLUSTER_NAME"
        fi
    fi
    
    # Test kubectl access
    if ! kubectl get nodes &> /dev/null; then
        print_error "Cannot access cluster. Please check your kubeconfig and cluster permissions"
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
    EXPORT_FILE="$OUTPUT_DIR/eks-export-${CLUSTER_NAME}-${TIMESTAMP}.$EXPORT_FORMAT"
    
    # Build export command
    EXPORT_CMD="python3 eks-config-exporter.py $CLUSTER_NAME"
    
    if [ -n "$REGION" ]; then
        EXPORT_CMD="$EXPORT_CMD --region $REGION"
    fi
    
    if [ -n "$KUBECONFIG" ]; then
        EXPORT_CMD="$EXPORT_CMD --kubeconfig $KUBECONFIG"
    fi
    
    EXPORT_CMD="$EXPORT_CMD --output $EXPORT_FILE --format $EXPORT_FORMAT"
    
    if [ "$GENERATE_SUMMARY" = true ]; then
        EXPORT_CMD="$EXPORT_CMD --summary"
    fi
    
    # Execute export
    if eval "$EXPORT_CMD"; then
        print_success "Configuration exported to: $EXPORT_FILE"
        echo "$EXPORT_FILE" > "$OUTPUT_DIR/.last_export"
    else
        print_error "Failed to export cluster configuration"
        exit 1
    fi
}

# Function to generate HTML dashboard
generate_html_dashboard() {
    if [ "$GENERATE_HTML" = false ]; then
        return
    fi
    
    print_info "Generating HTML dashboard..."
    
    # Get the last export file
    if [ -f "$OUTPUT_DIR/.last_export" ]; then
        EXPORT_FILE=$(cat "$OUTPUT_DIR/.last_export")
    else
        print_error "No export file found"
        return
    fi
    
    # Generate HTML dashboard
    TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
    HTML_FILE="$OUTPUT_DIR/eks-dashboard-${CLUSTER_NAME}-${TIMESTAMP}.html"
    
    if python3 eks-visualizer.py "$EXPORT_FILE" --output "$HTML_FILE"; then
        print_success "HTML dashboard generated: $HTML_FILE"
        echo "$HTML_FILE" > "$OUTPUT_DIR/.last_dashboard"
        
        # Try to open in browser (if available)
        if command -v open &> /dev/null; then
            print_info "Opening dashboard in browser..."
            open "$HTML_FILE" || true
        elif command -v xdg-open &> /dev/null; then
            print_info "Opening dashboard in browser..."
            xdg-open "$HTML_FILE" || true
        fi
    else
        print_error "Failed to generate HTML dashboard"
    fi
}

# Function to show export summary
show_export_summary() {
    print_success "EKS Export Completed Successfully!"
    echo
    print_info "Export Details:"
    echo "  Cluster: $CLUSTER_NAME"
    echo "  Region: ${REGION:-$(aws configure get region)}"
    echo "  Output Directory: $OUTPUT_DIR"
    echo "  Format: $EXPORT_FORMAT"
    echo
    
    if [ -f "$OUTPUT_DIR/.last_export" ]; then
        EXPORT_FILE=$(cat "$OUTPUT_DIR/.last_export")
        echo "  Export File: $EXPORT_FILE"
        echo "  File Size: $(du -h "$EXPORT_FILE" | cut -f1)"
    fi
    
    if [ -f "$OUTPUT_DIR/.last_dashboard" ]; then
        HTML_FILE=$(cat "$OUTPUT_DIR/.last_dashboard")
        echo "  HTML Dashboard: $HTML_FILE"
    fi
    
    echo
    print_info "Next Steps:"
    echo "  1. Review the exported data in the JSON/YAML file"
    echo "  2. Open the HTML dashboard in your web browser"
    echo "  3. Use the dashboard to explore your cluster configuration"
    
    if [ -f "$OUTPUT_DIR"/*summary*.txt ]; then
        echo "  4. Check the summary report for quick insights"
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
            KUBECONFIG="$2"
            shift 2
            ;;
        -f|--format)
            EXPORT_FORMAT="$2"
            shift 2
            ;;
        -v|--venv)
            VENV_DIR="$2"
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

# Validate required arguments
if [ -z "$CLUSTER_NAME" ]; then
    print_error "Cluster name is required"
    show_usage
    exit 1
fi

# Validate format
if [[ "$EXPORT_FORMAT" != "json" && "$EXPORT_FORMAT" != "yaml" ]]; then
    print_error "Export format must be 'json' or 'yaml'"
    exit 1
fi

# Main execution
main() {
    print_info "Starting EKS Configuration Export for cluster: $CLUSTER_NAME"
    echo
    
    check_prerequisites
    validate_cluster_access
    export_cluster_config
    generate_html_dashboard
    show_export_summary
}

# Run main function
main