import requests
import json
import torch
from sentence_transformers import SentenceTransformer

URL = "http://localhost:8000/health"


for job_idx in range(90000):
    r = requests.get(URL)
    print(r.json())
    print(job_idx)

