#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Space Guardian Pipeline ==="
echo ""

# Check R is available
if ! command -v Rscript &> /dev/null; then
    echo "ERROR: Rscript not found. Install R from https://cran.r-project.org/"
    echo "Then install required packages in R:"
    echo '  install.packages(c("arrow", "isotree", "asteRisk", "sets", "jsonlite"))'
    exit 1
fi

# Run the ingestion script first (handles strictness/caching internally)
echo "Ensuring fresh satellite data..."
python3 ingestion.py

# Run the master pipeline
echo "Running fuzzy.R (Isolation Forest + SGP4 + Fuzzy Logic)..."
Rscript "$SCRIPT_DIR/isolation/fuzzy.R"

# Copy output to visualisation folder
echo "Copying threat data to visualisation/..."
cp "$SCRIPT_DIR/isolation/space_guardian_threats.json" "$SCRIPT_DIR/visualisation/space_guardian_threats.json"

echo ""
echo "=== Done! ==="
echo "To view, serve the visualisation/ folder with a local HTTP server:"
echo "  cd $SCRIPT_DIR/visualisation && python3 -m http.server 8000"
echo "Then open http://localhost:8000 in your browser."
