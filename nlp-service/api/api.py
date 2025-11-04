from contextlib import asynccontextmanager
from fastapi import FastAPI
import pika
import json
from pydantic import BaseModel

class EmbedBatch(BaseModel):
    job_id: str         # например id определяющий документ, который обрабатывается 
    batch_index: int    # номер batch'a для этого документа 
    chunks: list[str]   # собственно сам батч (список кусочков текста)

app_state = {
    "rabbitmq": {}
}

connection_params = pika.ConnectionParameters(host='rabbitmq',
                                              port=5672)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Перед запуском сервера
    # Подключаемся к Rabbitmq
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    channel.queue_declare(queue="embeddings_queue")
    
    app_state["rabbitmq"]["connection"] = connection
    app_state["rabbitmq"]["channel"] = channel

    print("Startup complete: RabbitMQ connected")
    
    yield  # FastAPI принимает запросы 

    # При закрытии сервера
    # Закрываем наше соединение к RabbitMQ
    channel.close()
    connection.close()
    app_state["rabbitmq"].clear()
    print("Shutdown complete: resources cleaned up")

app = FastAPI(lifespan=lifespan)

@app.get("/embed")
async def embed_request(batch : EmbedBatch):
    channel = app_state["rabbitmq"]["channel"]
    channel.basic_publish(
        exchange="",
        routing_key="embedding_queue",
        body=json.dumps(batch.model_dump()),
    )
    return {"status": "queued", "job_id": batch.job_id, "batch_index": batch.batch_index}