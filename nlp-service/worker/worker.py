import pika
import json
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct
import torch

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
LOCAL_MODEL_PATH = "nlp-service\worker\distiluse-multilingual"
# инициализируем модель для эмбеддингов
model = SentenceTransformer(LOCAL_MODEL_PATH, device=device)

connection_params = pika.ConnectionParameters(host='rabbitmq',
                                              port=5672)
# подключаемся к бд
qdrant = QdrantClient() # TODO: вставить из docker'a  
COLLECTION_NAME = "documents"

def process_message(channel, method, properties, body):
    # Извлекаем данные из сообщения
    data = json.loads(body)
    
    job_id = data["job_id"]
    batch_index = data["batch_index"]
    chunks = data["chunks"]

    # Получаем эмбеддинги
    points = []
    for i, chunk_text in enumerate(chunks): 
        embedding = model.encode(chunk_text)
        points.append(
            PointStruct(
                id=f"{job_id}_{batch_index}_{i}", # уникальное
                vector=embedding,
                payload={"job_id":job_id, "batch_index": batch_index, "chunk_index": i, "text": chunk_text}
            )
        )
        print(f"Chunk {i} in batch {batch_index} for job {job_id} done")
    # СОХРАНЯЕМ В QDRANT
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
            channel.start_consuming()
    
if __name__ == "__main__":
    main()