import os
import pika
import json
import requests
from PyPDF2 import PdfReader
from PIL import Image
from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv("HOST")
SENDER_QUEUE_NAME = os.getenv("SENDER_QUEUE_NAME")
CONSUMER_QUEUE_NAME = os.getenv("CONSUMER_QUEUE_NAME")

print(HOST, SENDER_QUEUE_NAME, CONSUMER_QUEUE_NAME)

mimeTypesExtension = {
    "image/png": "png",
    "image/jpeg": "jpg",
    "image/gif": "gif",
    "application/pdf": "pdf"
}

connection = pika.BlockingConnection(pika.ConnectionParameters(HOST))

rmq_channel = connection.channel()
session = requests.Session()

rmq_channel.exchange_declare(exchange=SENDER_QUEUE_NAME, exchange_type='topic', durable=True)


def download_image(session: requests.Session, url: str, path: str) -> bool:
    r = session.get(url, stream=True)
    if r.status_code == 200:
        with open(path, "wb") as f:
            f.write(r.content)
        return True
    return False


def send(message: str) -> None:
    print(message)
    rmq_channel.basic_publish(exchange=SENDER_QUEUE_NAME, routing_key=SENDER_QUEUE_NAME, body=message)


def on_message(channel, method_frame, header_frame, body) -> None:
    rmq_channel.basic_ack(method_frame.delivery_tag)
    req = json.loads(body)
    url = req["url"]
    upload_id = req["sourceUploadID"]
    mime_type = req["fileInfo"]["contentType"]
    file = f"tmp/{upload_id}.{mimeTypesExtension[mime_type]}"
    if not download_image(session, url, file):
        return None

    data = {
        "type": "IMAGE",
        "pages": 1
    }

    if mime_type == "application/pdf":
        reader = PdfReader(file)
        data["type"] = "SEQUENCE"
        data["pages"] = len(reader.pages)
    elif mime_type == "application/gif":
        data["type"] = "SEQUENCE"
        with Image.open('somegif.gif') as im:
            data["pages"] = im.n_frames
    else:
        data["type"] = "IMAGE"
        data["pages"] = 1

    os.remove(file)

    res = {
        "sourceUploadID": upload_id,
        "data": data
    }

    send(json.dumps(res, separators=(',', ':'), ensure_ascii=False))


result = rmq_channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

rmq_channel.queue_bind(exchange=CONSUMER_QUEUE_NAME, queue=queue_name, routing_key=CONSUMER_QUEUE_NAME)

rmq_channel.basic_consume(
    queue=queue_name, on_message_callback=on_message, auto_ack=False)

try:
    rmq_channel.start_consuming()
except KeyboardInterrupt:
    rmq_channel.close()
    session.close()
