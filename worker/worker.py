import pika
import json
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct, VectorParams, Distance, Filter
import torch
import uuid


device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

MODEL_NAME = "sentence-transformers/distiluse-base-multilingual-cased-v2"
# инициализируем модель для эмбеддингов
model = SentenceTransformer(MODEL_NAME, device="cuda")

connection_params = pika.ConnectionParameters(host='rabbitmq',
                                              port=5672)
# подключаемся к бд
qdrant = QdrantClient("qdrant",
                      port=6333)
a = [qdrant.get_collections().collections]
COLLECTION_NAME = "documents"
if COLLECTION_NAME not in [c.name for c in qdrant.get_collections().collections]:
    qdrant.create_collection(
        collection_name=f"{COLLECTION_NAME}",
        vectors_config=VectorParams(size=512, distance=Distance.COSINE),
    )
else:
    qdrant.delete(collection_name="documents", points_selector=Filter(must=[]))


def process_message(channel, method, properties, body):
    # Извлекаем данные из сообщения
    data = json.loads(body)
    
    job_id = data["job_id"]
    batch_index = data["batch_index"]
    chunks = data["chunks"]

    # Получаем эмбеддинги
    points = []
    embeddings = model.encode(chunks, batch_size=16)
    for i, embedding in enumerate(embeddings): 
        points.append(
            PointStruct(
                id=str(uuid.uuid4()),
                vector=embedding,
                payload={"job_id":job_id, "batch_index": batch_index, "chunk_index": i, "text": chunks[i]}
            )
        )

    # # СОХРАНЯЕМ В QDRANT
    qdrant.upsert(collection_name=COLLECTION_NAME, points=points)

    # Уведомляем брокер что сообщение обработано, чтобы сообщение было удалено из очереди 
    # тэг этого сообщения пеердаем
    channel.basic_ack(delivery_tag=method.delivery_tag)


def main():
    # подключаемся к RabbitMQ
    with pika.BlockingConnection(connection_params) as connection:
        with connection.channel() as channel:
            channel.queue_declare(queue='embedding_queue')
            # начинаем "потребление" сообщений
            channel.basic_consume(queue='embedding_queue', on_message_callback=process_message)
            print("Worker started, waiting for tasks...")
            print(f"worker device {device}")
            channel.start_consuming()
    
if __name__ == "__main__":
    main()