#!/bin/bash

# Exit on error
set -e

# Default values
MAX_RESULTS=100
ISSUES_RESULTS=15
FORCE_UPDATE=false
# By default run a consolidated search across all engines (arXiv + CrossRef,
# ACM, OpenAlex, Semantic Scholar, IEEE, DVCon) for verification topics.
SOURCE="all"      # valid values: arxiv, crossref, acm, openalex, semanticscholar, ieee, all, dvcon
PROFILE="verification"  # valid values: general, verification
LOG_DIR="logs"
# This must match the filename used in logging.basicConfig in main.py.
LOG_FILE="daily_papers.log"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --max-results)
            MAX_RESULTS="$2"
            shift 2
            ;;
        --issues-results)
            ISSUES_RESULTS="$2"
            shift 2
            ;;
        --force-update)
            FORCE_UPDATE=true
            shift
            ;;
        --source)
            SOURCE="$2"
            shift 2
            ;;
        --profile)
            PROFILE="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "Starting Daily Papers Update Script..."
echo "Selected source: $SOURCE"
echo "Selected profile: $PROFILE"

# Determine flags for main.py based on additional sources
INCLUDE_FLAGS=""

case "$SOURCE" in
    arxiv)
        INCLUDE_FLAGS=""
        ;;
    crossref)
        INCLUDE_FLAGS="--include-crossref"
        ;;
    acm)
        INCLUDE_FLAGS="--include-acm"
        ;;
    openalex)
        INCLUDE_FLAGS="--include-openalex"
        ;;
    semanticscholar)
        INCLUDE_FLAGS="--include-semanticscholar"
        ;;
    ieee)
        INCLUDE_FLAGS="--include-ieee"
        ;;
    dvcon)
        INCLUDE_FLAGS="--include-dvcon --download-dvcon-assets"
        ;;
    all)
        # For verification-centric runs, prefer DV-focused sources (DVCon, IEEE,
        # ACM, Semantic Scholar) and skip broad aggregators like CrossRef and
        # OpenAlex unless explicitly requested.
        if [[ "$PROFILE" == "verification" ]]; then
            INCLUDE_FLAGS="--include-acm --include-semanticscholar --include-ieee --include-dvcon --download-dvcon-assets"
        else
            INCLUDE_FLAGS="--include-crossref --include-acm --include-openalex --include-semanticscholar --include-ieee --include-dvcon --download-dvcon-assets"
        fi
        ;;
    *)
        echo "Invalid source: $SOURCE. Valid options are: arxiv, crossref, acm, openalex, semanticscholar, ieee, all."
        exit 1
        ;;
esac

# Create log directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Execute the main script with arguments
echo "Running main.py..."
python3 main.py \
    --max-results "$MAX_RESULTS" \
    --issues-results "$ISSUES_RESULTS" \
    --profile "$PROFILE" \
    --source "$SOURCE" \
    ${FORCE_UPDATE:+--force-update} \
    $INCLUDE_FLAGS

# If DVCon assets were downloaded, also build DVCON_README.md with OCR abstracts.
if [[ "$SOURCE" == "dvcon" || "$SOURCE" == "all" ]]; then
    echo "Building DVCON_README.md from downloaded DVCon PDFs..."
    python3 -c "from utils import build_dvcon_readme_from_pdfs; build_dvcon_readme_from_pdfs()"
fi

# Check if the script executed successfully
if [ $? -eq 0 ]; then
    echo "Script completed successfully!"
    # Archive the log file with timestamp if it exists
    if [ -f "$LOG_FILE" ]; then
        TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
        mv "$LOG_FILE" "${LOG_DIR}/${TIMESTAMP}_${LOG_FILE}"
    else
        echo "No log file '$LOG_FILE' found to archive."
    fi
else
    echo "Script failed with error code $?"
    exit 1
fi