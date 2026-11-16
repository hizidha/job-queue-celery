# demo.py
# Menjalankan satu percobaan lengkap dalam satu terminal, urutannya:
#   1. kosongkan semua queue dari sisa percobaan sebelumnya
#   2. nyalakan 4 worker, masing-masing membaca queue sesuai mode
#   3. tunggu sampai keempat worker siap
#   4. jalankan producer.py dengan mode yang sama
#   5. matikan semua worker
#
# Cara menjalankan: python demo.py fanout | topic | wildcard | simulasi

import os
import subprocess
import sys
import time

from celery_config import WORKERS, app

# Awalan queue yang dibaca worker untuk tiap mode.
# Contoh mode "simulasi": worker audit membaca wildcard.audit (pesanan) dan fanout.audit (pengumuman).
QUEUE_PER_MODE = {
    "fanout": ["fanout"],
    "topic": ["topic"],
    "wildcard": ["wildcard"],
    "simulasi": ["wildcard", "fanout"],
}

mode = sys.argv[1] if len(sys.argv) > 1 else ""
if mode not in QUEUE_PER_MODE:
    sys.exit("Pilihan: python demo.py fanout | topic | wildcard | simulasi")

python = sys.executable                         # python dari virtual environment yang sedang aktif
env = dict(os.environ, PYTHONUNBUFFERED="1")    # agar log subprocess langsung tampil, tidak tertahan

print(f"=== Percobaan: {mode} ===", flush=True)

# Langkah 1: kosongkan queue (sekaligus membuat queue yang belum ada di broker).
app.control.purge()

# Langkah 2: nyalakan worker. Perintahnya sama dengan mengetik di terminal:
#   celery -A tasks worker -P solo -Q wildcard.audit -n audit@toko ...
#   -P solo   : satu proses mengerjakan satu task sekaligus (wajib di Windows)
#   -Q        : queue yang dibaca worker ini
#   -n        : nama worker, tampil di log dan di Flower
#   --without-mingle --without-gossip : matikan obrolan antarworker agar hemat koneksi
workers = []
for w in WORKERS:
    queue = ",".join(f"{awalan}.{w}" for awalan in QUEUE_PER_MODE[mode])
    print(f"[Demo] nyalakan {w:<15} membaca: {queue}", flush=True)
    workers.append(subprocess.Popen(
        [python, "-m", "celery", "-A", "tasks", "--quiet", "worker", "-P", "solo", "-Q", queue,
         "-n", f"{w}@toko", "--without-mingle", "--without-gossip", "--loglevel=WARNING"], env=env))

try:
    # Langkah 3: ping semua worker sampai keempatnya menjawab (maksimal 60 detik).
    batas = time.time() + 60
    while len(app.control.ping(timeout=1)) < len(WORKERS):
        if time.time() > batas:
            sys.exit("[Demo] worker tidak siap dalam 60 detik; cek CELERY_BROKER_URL")
    print("[Demo] semua worker siap\n", flush=True)

    # Langkah 4: jalankan producer dengan mode yang sama.
    subprocess.run([python, "producer.py", mode, "--seed", "7"], env=env, check=True)
    time.sleep(5)  # shortcut: tunggu tetap 5 detik agar worker selesai mencetak; cukup untuk task ringan ini
finally:
    # Langkah 5: matikan semua worker, apa pun yang terjadi di atas.
    for p in workers:
        p.terminate()
    for p in workers:
        p.wait()
    print("\n[Demo] semua worker dimatikan", flush=True)
