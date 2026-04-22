import json
import os
import random

def convert_to_mlx():
    # Use absolute-ish paths or check local existence
    input_file = "sec_openedgar/sec_research/finetuning/qwen_30b_train_1500.jsonl"
    if not os.path.exists(input_file):
        input_file = "sec_research/finetuning/qwen_30b_train_1500.jsonl"
        
    output_dir = os.path.join(os.path.dirname(input_file), "mlx_data")
    os.makedirs(output_dir, exist_ok=True)
    
    with open(input_file, 'r') as f:
        lines = f.readlines()
    
    random.shuffle(lines)
    
    # MLX-LM lora expects "text" field or chat format
    # We will use the chat format as it matches our Instruct model training
    def format_line(line):
        data = json.loads(line)
        # Assuming messages format: [{"role": "user", "content": "..."}, {"role": "assistant", "content": "..."}]
        return json.dumps(data) + "\n"

    train_split = int(len(lines) * 0.9)
    
    with open(os.path.join(output_dir, "train.jsonl"), "w") as f:
        for line in lines[:train_split]:
            f.write(format_line(line))
            
    with open(os.path.join(output_dir, "valid.jsonl"), "w") as f:
        for line in lines[train_split:]:
            f.write(format_line(line))

    print(f"MLX Data prepared in {output_dir}")

if __name__ == "__main__":
    convert_to_mlx()
