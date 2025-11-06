import requests
import json
import torch
from sentence_transformers import SentenceTransformer

URL = "http://localhost:8000/embed"


for job_idx in range(900):
    batch = {
        "job_id": f"doc{job_idx}",
        "batch_index": 0,
        "chunks": [f"text chunk {i}"*40 for i in range(64)]
    }
    r = requests.post(URL, json=batch)
    print(r.json())

