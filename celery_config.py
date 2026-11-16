# celery_config.py
# File ini tidak menjalankan apa pun. Isinya hanya pengaturan yang dibaca oleh file lain:
#   1. alamat broker RabbitMQ,
#   2. tiga exchange (satu untuk tiap pola: fan-out, topic, wildcard),
#   3. queue milik tiap worker beserta binding-nya (routing key apa yang mau diterima).
# Dipakai oleh tasks.py (worker), producer.py, dan demo.py.

import os
import sys

from celery import Celery
from kombu import Exchange, Queue, binding

# ---------------------------------------------------------------------------
# 1. Aplikasi Celery dan alamat broker
# ---------------------------------------------------------------------------
# URL CloudAMQP diambil dari environment variable agar password tidak ikut ter-push ke GitHub.
# Jika variabel itu tidak ada, Celery mencoba RabbitMQ lokal (localhost).
BROKER = os.environ.get("CELERY_BROKER_URL", "amqp://guest@localhost//")

# "toko" adalah nama aplikasi. include=["tasks"] memberi tahu worker bahwa daftar task ada di tasks.py.
app = Celery("toko", broker=BROKER, include=["tasks"])

# Nama keempat worker. Nama ini dipakai untuk menamai queue dan untuk menyalakan worker (-n).
WORKERS = ["gudang-surabaya", "gudang-jakarta", "kurir-express", "audit"]

# ---------------------------------------------------------------------------
# 2. Tiga exchange
# ---------------------------------------------------------------------------
# Producer tidak mengirim pesan langsung ke queue, melainkan ke exchange.
# Exchange-lah yang memutuskan pesan disalin ke queue mana saja, sesuai tipenya.
FANOUT = Exchange("toko.fanout", type="fanout")      # routing key diabaikan, semua queue dapat salinan
TOPIC = Exchange("toko.topic", type="topic")         # routing key harus sama persis dengan binding
WILDCARD = Exchange("toko.wildcard", type="topic")   # binding boleh berupa pola * dan #

# ---------------------------------------------------------------------------
# 3. Binding: routing key yang diterima tiap worker
# ---------------------------------------------------------------------------
# Format routing key pesanan: pesanan.<kota>.<layanan>, contoh: pesanan.surabaya.express

# Topic (tanpa pola): setiap kombinasi kota dan layanan harus didaftarkan satu per satu.
BINDING_TOPIC = {
    "gudang-surabaya": ["pesanan.surabaya.reguler", "pesanan.surabaya.express"],
    "gudang-jakarta": ["pesanan.jakarta.reguler", "pesanan.jakarta.express"],
    "kurir-express": ["pesanan.surabaya.express", "pesanan.jakarta.express"],
    "audit": ["pesanan.surabaya.reguler", "pesanan.surabaya.express",
              "pesanan.jakarta.reguler", "pesanan.jakarta.express"],
}

# Wildcard: cukup satu pola per worker.
#   *  = tepat satu kata
#   #  = nol kata atau lebih
BINDING_WILDCARD = {
    "gudang-surabaya": ["pesanan.surabaya.*"],   # semua layanan dari Surabaya
    "gudang-jakarta": ["pesanan.jakarta.*"],     # semua layanan dari Jakarta
    "kurir-express": ["pesanan.*.express"],      # layanan express dari kota mana pun
    "audit": ["pesanan.#"],                      # semua pesanan, berapa pun jumlah katanya
}

# Setiap worker punya tiga queue, satu per exchange. Contoh untuk audit:
#   fanout.audit   -> terikat ke toko.fanout
#   topic.audit    -> terikat ke toko.topic dengan 4 routing key di atas
#   wildcard.audit -> terikat ke toko.wildcard dengan pola pesanan.#
# Worker nanti memilih queue mana yang dibaca lewat opsi -Q.
queues = []
for w in WORKERS:
    queues.append(Queue(f"fanout.{w}", FANOUT))
    queues.append(Queue(f"topic.{w}", bindings=[binding(TOPIC, routing_key=k) for k in BINDING_TOPIC[w]]))
    queues.append(Queue(f"wildcard.{w}", bindings=[binding(WILDCARD, routing_key=k) for k in BINDING_WILDCARD[w]]))
app.conf.task_queues = queues

# ---------------------------------------------------------------------------
# 4. Pengaturan lain
# ---------------------------------------------------------------------------
app.conf.update(
    # Satu pesan bisa dikerjakan beberapa worker sekaligus (misalnya fan-out),
    # sehingga hasil task tidak dikirim balik ke producer. Hasilnya dilihat di log worker dan Flower.
    task_ignore_result=True,
    worker_redirect_stdouts=False,   # print() di task tampil apa adanya, tanpa awalan log Celery
    worker_send_task_events=True,    # worker melaporkan setiap task ke Flower
    task_send_sent_event=True,       # producer juga melapor saat mengirim, agar Flower mencatat task sejak dikirim
    broker_connection_timeout=30,    # broker berada di internet, jadi beri waktu lebih lama untuk tersambung
    # CloudAMQP gratis hanya mengizinkan 20 koneksi. Secara bawaan setiap proses menahan koneksi
    # cadangan (pool) agar bisa dipakai ulang. Dengan 0, koneksi tambahan dibuka saat dipakai lalu ditutup.
    broker_pool_limit=0,
    task_serializer="json",          # isi pesan dikirim dalam format JSON
    accept_content=["json"],
)

# Flower menanyai setiap worker 8 hal sekaligus (statistik, queue, task terdaftar, dst.), dan setiap
# pertanyaan butuh 2 koneksi. Itu bisa menghabiskan jatah 20 koneksi CloudAMQP, sehingga producer
# ditolak broker. Khusus saat menjalankan Flower, pertanyaan itu dibuat satu per satu.
if "flower" in sys.argv:
    from flower.inspector import Inspector
    Inspector.max_concurrency = 1
