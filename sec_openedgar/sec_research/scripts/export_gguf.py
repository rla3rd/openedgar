import torch
from unsloth import FastLanguageModel

def main():
    # We use the existing directory name as source
    model_path = "finetuned_qwen_7b"
    print(f"Loading merged model from {model_path} for GGUF export...")
    
    model, tokenizer = FastLanguageModel.from_pretrained(
        model_name = model_path,
        max_seq_length = 3072,
        dtype = None,
        load_in_4bit = True,
    )
    
    print("Starting GGUF conversion (q4_k_m) to Qwen3_14B_GGUF...")
    # This will now find the dependencies in the venv
    model.save_pretrained_gguf("Qwen3_14B_GGUF", tokenizer, quantization_method = "q4_k_m")
    print("Export complete!")

if __name__ == "__main__":
    main()
