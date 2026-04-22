#!/bin/bash
# Fine-tuning Qwen3-Coder-30B on 1,500 SEC filings
# Using Unsloth + LoRA on RTX 3090

export CUDA_VISIBLE_DEVICES=0
export PYTHONPATH=$PYTHONPATH:$(pwd)

echo "Starting Qwen3-Coder-30B Fine-Tuning..."
/home/ralbright/projects/openedgar/venv/bin/uv run python sec_research/scripts/train_lora.py 2>&1 | tee sec_research/finetuning/training_qwen3_30b.log
