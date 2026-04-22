#!/bin/bash
# run_30b_hardened.sh
# Rerun the 500-filing Qwen-30B evaluation with the new H2/H3 markdown structure.

PROJECT_ROOT="/home/ralbright/projects/openedgar"
APP_ROOT="$PROJECT_ROOT/sec_openedgar"
VENV_BIN="$PROJECT_ROOT/venv/bin"

echo "Starting Hardened 500-filing Qwen-30B Baseline Evaluation..."
cd "$APP_ROOT" || exit 1

"$VENV_BIN/uv" run python manage.py evaluate_ownership_llm \
    --holdout sec_research/evaluation/test_set_final_500.txt \
    --model qwen3-coder-30b \
    --cache-dir sec_research/evaluation/hp_cache_qwen3-30b-hardened \
    --summary-out sec_research/evaluation/holdout_summary_Qwen3_30B_Hardened.json \
    --limit 500

echo "Evaluation complete. Results written to sec_research/evaluation/holdout_summary_Qwen3_30B_Hardened.json"
