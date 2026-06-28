#!/bin/bash
# Ablation Study: Raw XML vs. Synthesized Markdown
# This script runs the evaluation in 'xml' mode on a 50-filing subset.

ACC_FILE="sec_research/evaluation/test_set_accs_mini.txt"
if [ ! -f "$ACC_FILE" ]; then
    head -n 50 sec_research/evaluation/test_set_2000.txt > "$ACC_FILE"
fi

echo "Running Raw XML Baseline Evaluation..."
../python manage.py evaluate_ownership_llm \
    --holdout "$ACC_FILE" \
    --mode xml \
    --summary-out scratch/ablation_xml_summary.json \
    --limit 50

echo "Running Markdown Evaluation (for comparison)..."
../python manage.py evaluate_ownership_llm \
    --holdout "$ACC_FILE" \
    --mode markdown \
    --summary-out scratch/ablation_markdown_summary.json \
    --limit 50
