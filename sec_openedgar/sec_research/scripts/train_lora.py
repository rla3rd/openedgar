"""
Standalone Unsloth Fine-Tuning Script for OpenEDGAR SEC Extraction
Model: Qwen3-Coder-30B
Target: High-Fidelity Form 4 Extraction
"""
import torch
import os
from datasets import load_dataset
from unsloth import FastLanguageModel
from trl import SFTTrainer
from transformers import TrainingArguments
from unsloth.chat_templates import get_chat_template

def main():
    max_seq_length = 4096 
    dtype = None # Auto detection
    load_in_4bit = True 

    # 1. Load Base Model
    model, tokenizer = FastLanguageModel.from_pretrained(
        model_name = "Qwen/Qwen3-Coder-30B-A3B-Instruct",
        max_seq_length = max_seq_length,
        dtype = dtype,
        load_in_4bit = load_in_4bit,
    )

    # 2. Add LoRA Adapters
    model = FastLanguageModel.get_peft_model(
        model,
        r = 16, 
        target_modules = ["q_proj", "k_proj", "v_proj", "o_proj",
                          "gate_proj", "up_proj", "down_proj"],
        lora_alpha = 16,
        lora_dropout = 0,
        bias = "none",
        use_gradient_checkpointing = "unsloth",
        random_state = 3407,
        use_rslora = False,
    )

    # 3. Apply ChatML Template
    tokenizer = get_chat_template(
        tokenizer,
        chat_template = "chatml",
    )

    def formatting_prompts_func(examples):
        texts = []
        for messages in examples["messages"]:
            text = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=False)
            texts.append(text)
        return { "text" : texts }

    # 4. Load Dataset
    dataset_path = "sec_research/finetuning/qwen_30b_train_1500.jsonl"
    print(f"Loading {dataset_path}...")
    dataset = load_dataset("json", data_files=dataset_path, split="train")
    dataset = dataset.map(formatting_prompts_func, batched=True)

    # 5. Initialize SFT Trainer
    trainer = SFTTrainer(
        model = model,
        tokenizer = tokenizer,
        train_dataset = dataset,
        dataset_text_field = "text",
        max_seq_length = max_seq_length,
        dataset_num_proc = 2,
        packing = False,
        args = TrainingArguments(
            per_device_train_batch_size = 2,
            gradient_accumulation_steps = 4,
            warmup_steps = 10,
            num_train_epochs = 1,
            learning_rate = 2e-4,
            fp16 = not torch.cuda.is_bf16_supported(),
            bf16 = torch.cuda.is_bf16_supported(),
            logging_steps = 1,
            optim = "adamw_8bit",
            weight_decay = 0.01,
            lr_scheduler_type = "linear",
            seed = 3407,
            output_dir = "sec_research/finetuning/outputs_qwen3_30b",
            report_to = "none",
        ),
    )

    # 6. Train
    print("Starting Fine-Tuning on 1,500 filings...")
    trainer_stats = trainer.train()

    # 7. Export to GGUF
    print("Exporting model to GGUF (q4_k_m)...")
    model.save_pretrained_gguf("sec_research/finetuning/Qwen3-Coder-30B-OpenEDGAR", tokenizer, quantization_method = "q4_k_m")
    
    print("Done! Model saved to sec_research/finetuning/Qwen3-Coder-30B-OpenEDGAR")

if __name__ == "__main__":
    main()
