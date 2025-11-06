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
    channel.queue_declare(queue="embedding_queue")
    
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

"""
sync function inside async - block entire server, event loop  V
sync function inside sync (just def), - fastapi automatically run in an external threadpool (пул потоков) that is then awaited (so does block the event loop) - V

so making sync calls inside of async is the worst type of idea? 
you either have it sync, or you have it async

@app.post("/embed")
async def embed_request(batch : EmbedBatch):
    channel = app_state["rabbitmq"]["channel"]
    # STOPS ENTIRE SERVER ANY SYNC OPERATION, stops every async path operation
    time.sleep(10)
    channel.basic_publish(
        exchange="",
        routing_key="embedding_queue",
        body=json.dumps(batch.model_dump()),
    )
    return {"status": "queued", "job_id": batch.job_id, "batch_index": batch.batch_index}
"""

@app.post("/embed")
def embed_request(batch : EmbedBatch):
    channel = app_state["rabbitmq"]["channel"]
    channel.basic_publish(
        exchange="",
        routing_key="embedding_queue",
        body=json.dumps(batch.model_dump()),
    )
    return {"status": "queued", "job_id": batch.job_id, "batch_index": batch.batch_index}

@app.get("/health")
def health_check():
    return {"status":"alive"}