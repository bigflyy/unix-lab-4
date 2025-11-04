import requests
import json
import torch
from sentence_transformers import SentenceTransformer

URL = "http://localhost:8000/embed"
# MODEL_NAME = "sentence-transformers/distiluse-base-multilingual-cased-v2"
# # инициализируем модель для эмбеддингов

# device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
# print(device)
# model = SentenceTransformer(MODEL_NAME, device="cuda")


# for job_idx in range(1000):
#     # print(job_idx)
    
#     model.encode([f"text chunk {i}"*50 for i in range(256)], batch_size=64, convert_to_tensor=True)


for job_idx in range(1000):
    batch = {
        "job_id": f"doc{job_idx}",
        "batch_index": 0,
        "chunks": [f"text chunk {i}"*100 for i in range(256)]
    }
    r = requests.post(URL, json=batch)
    print(r.json())

