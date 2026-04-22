import os
import json
import pathlib
import argparse
from rouge_score import rouge_scorer
import numpy as np

# Try to import sentence_transformers, fallback to sklearn if needed
try:
    import torch
    from sentence_transformers import SentenceTransformer, util
    ST_AVAILABLE = True
    model = SentenceTransformer('all-MiniLM-L6-v2')
except ImportError:
    ST_AVAILABLE = False
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity

def calculate_rouge(preds, targets):
    scorer = rouge_scorer.RougeScorer(['rouge1', 'rougeL'], use_stemmer=True)
    r1_scores = []
    rl_scores = []
    for p, t in zip(preds, targets):
        if not p or not t: continue
        scores = scorer.score(t, p)
        r1_scores.append(scores['rouge1'].fmeasure)
        rl_scores.append(scores['rougeL'].fmeasure)
    return np.mean(r1_scores) if r1_scores else 0, np.mean(rl_scores) if rl_scores else 0

def calculate_cosine(preds, targets):
    if not preds or not targets: return 0
    if ST_AVAILABLE:
        embeddings1 = model.encode(preds, convert_to_tensor=True)
        embeddings2 = model.encode(targets, convert_to_tensor=True)
        cosine_scores = util.cos_sim(embeddings1, embeddings2)
        # Take the diagonal (paired comparisons)
        if torch.is_tensor(cosine_scores):
            return float(torch.mean(torch.diag(cosine_scores)))
        else:
            return float(np.mean(np.diag(cosine_scores)))
    else:
        # Fallback to TF-IDF
        scores = []
        for p, t in zip(preds, targets):
            if not p or not t: continue
            tfidf = TfidfVectorizer().fit_transform([p, t])
            scores.append(cosine_similarity(tfidf[0:1], tfidf[1:2])[0][0])
        return np.mean(scores) if scores else 0

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--pred_dir', default='sec_research/evaluation/hp_cache_qwen3-coder-30b')
    parser.add_argument('--gt_dir', default='sec_research/evaluation/gt_cache')
    args = parser.parse_args()

    pred_path = pathlib.Path(args.pred_dir)
    gt_path = pathlib.Path(args.gt_dir)

    all_preds = []
    all_gts = []

    counts = {
        'non_derivative_transactions': 0,
        'non_derivative_holdings': 0,
        'derivative_transactions': 0,
        'derivative_holdings': 0
    }

    files_processed = 0
    for f in pred_path.glob("*.json"):
        gt_file = gt_path / f.name
        if not gt_file.exists(): continue

        with open(f, 'r') as f_in:
            pred_data = json.load(f_in)
        with open(gt_file, 'r') as f_in:
            gt_data = json.load(f_in)

        for table in counts.keys():
            p_rows = pred_data.get(table, [])
            t_rows = gt_data.get(table, [])
            
            # Count ground truth rows for total volume metrics
            counts[table] += len(t_rows)
            
            # Extract summaries if applicable (only for transactions)
            if 'transactions' in table:
                for i in range(min(len(p_rows), len(t_rows))):
                    p_sum = p_rows[i].get('transaction_summary', '')
                    t_sum = t_rows[i].get('transaction_summary', '')
                    if p_sum and t_sum:
                        all_preds.append(p_sum)
                        all_gts.append(t_sum)
        
        files_processed += 1

    print(f"Processed {files_processed} files.")
    print("-" * 30)
    print("Dataset Volume (Ground Truth):")
    print(f"Table I Transactions: {counts['non_derivative_transactions']}")
    print(f"Table I Holdings:     {counts['non_derivative_holdings']}")
    print(f"Table II Transactions: {counts['derivative_transactions']}")
    print(f"Table II Holdings:    {counts['derivative_holdings']}")
    print("-" * 30)
    
    print(f"Found {len(all_preds)} summary pairs for textual analysis.")
    r1, rl = calculate_rouge(all_preds, all_gts)
    cos = calculate_cosine(all_preds, all_gts)

    print(f"Revised Textual Metrics:")
    print(f"ROUGE-1: {r1:.4f}")
    print(f"ROUGE-L: {rl:.4f}")
    print(f"Cosine Similarity: {cos:.4f}")
    print("-" * 30)

if __name__ == "__main__":
    main()
