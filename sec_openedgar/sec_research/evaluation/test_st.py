from sentence_transformers import SentenceTransformer, util
model = SentenceTransformer('all-MiniLM-L6-v2')
sentences1 = ["The reporting owner acquired 100 shares."]
sentences2 = ["100 shares were bought by the owner."]
embeddings1 = model.encode(sentences1)
embeddings2 = model.encode(sentences2)
cosine_scores = util.cos_sim(embeddings1, embeddings2)
print(f"Similarity: {cosine_scores[0][0].item()}")
