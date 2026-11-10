import os

from celery import Celery
from kombu import Exchange, Queue

# Initialize Celery app
app = Celery('task_chain', include=['tasks'])

# [MODIFIKASI] Exchange untuk uji 3 pola penanganan pesan
fanout = Exchange('demo.fanout', type='fanout')   # semua queue yang terhubung mendapat salinan
topic = Exchange('demo.topic', type='topic')      # dipilih berdasarkan routing key / pola

# Configure Celery
app.conf.update(
    # [MODIFIKASI] alamat broker dibaca dari environment variable (CloudAMQP), bawaan: RabbitMQ lokal
    broker_url=os.environ.get('CELERY_BROKER_URL', 'amqp://guest@localhost//'),
    result_backend='rpc://',
    task_routes={
        'tasks.process_data': {'queue': 'data_processing'},
        'tasks.generate_report': {'queue': 'report_generation'},
    },
    # [MODIFIKASI] daftar queue beserta binding-nya
    task_queues=[
        # queue bawaan kode contoh
        Queue('celery', routing_key='celery'),
        Queue('data_processing', routing_key='data_processing'),
        Queue('report_generation', routing_key='report_generation'),

        # Fan-Out: routing key diabaikan
        Queue('fan.w1', fanout),
        Queue('fan.w2', fanout),
        Queue('fan.w3', fanout),

        # Topic: routing key harus sama persis
        Queue('topic.data', topic, routing_key='job.data'),
        Queue('topic.report', topic, routing_key='job.report'),

        # Wildcard: * = tepat satu kata, # = nol kata atau lebih
        Queue('wild.bintang', topic, routing_key='job.*'),
        Queue('wild.pagar', topic, routing_key='job.#'),
    ],
    worker_send_task_events=True,     # [MODIFIKASI] worker melaporkan status task, agar terlihat di Flower
    task_send_sent_event=True,        # [MODIFIKASI] task yang baru dikirim juga terlihat di Flower
    broker_connection_timeout=30,     # [MODIFIKASI] broker di internet lebih lambat dari localhost
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)