FROM python:3.12

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt --progress-bar=on
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer("'sentence-transformers/distiluse-base-multilingual-cased-v2'")"

COPY worker.py .

CMD ["python", "worker.py"]