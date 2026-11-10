# celery_config.py
# Konfigurasi Celery: alamat broker (RabbitMQ) dan queue.

import os

from celery import Celery

# URL CloudAMQP diambil dari environment variable agar password tidak ikut ter-push ke GitHub.
BROKER = os.environ.get("CELERY_BROKER_URL", "amqp://guest@localhost//")
app = Celery("tugas2", broker=BROKER, backend="rpc://", include=["tasks"])

app.conf.update(
    # LONG dan SHORT masuk satu queue, karena seperti di Tugas 1 setiap worker melayani keduanya
    task_default_queue="tugas",
    task_acks_late=True,                 # pesan baru di-ack setelah task selesai (dipakai strategi Least Connections)
    worker_send_task_events=True,        # worker melaporkan status task, agar terlihat di Flower
    task_send_sent_event=True,           # task yang baru dikirim juga terlihat di Flower
    broker_connection_timeout=30,        # broker di internet lebih lambat dari localhost
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
)