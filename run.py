# run.py
# Menjalankan worker Celery + skrip pengirim sekaligus di satu komputer (RabbitMQ harus sudah jalan).
#   python run.py round-robin        -> 3 worker, tiap worker memegang hingga 4 task (dibagi bergiliran)
#   python run.py least-connections  -> 3 worker, tiap worker memegang 1 task (ke worker yang sedang kosong)

import subprocess
import sys
import time

JATAH = {"round-robin": "4", "least-connections": "1"}   # jumlah task yang boleh dipegang satu worker
mode = sys.argv[1] if len(sys.argv) > 1 else ""
if mode not in JATAH:
    sys.exit("Pilihan: round-robin, least-connections")
py = sys.executable


def worker(nama, prefetch):
    # -P solo: satu proses mengerjakan satu task sekaligus (juga wajib di Windows)
    return subprocess.Popen([py, "-m", "celery", "-A", "tasks", "--quiet", "worker", "-P", "solo",
                             "-Q", "tugas", "-n", f"{nama}@adisaputra-zidha", "--prefetch-multiplier", prefetch,
                             "--without-mingle", "--without-gossip", "--loglevel=WARNING"])


workers = [worker(f"w{i}", JATAH[mode]) for i in (1, 2, 3)]
perintah = [py, "producer.py", "--jumlah", "12", "--seed", "7"]

time.sleep(8)                      # beri waktu worker terhubung ke RabbitMQ
try:
    subprocess.run(perintah)
    time.sleep(3)                  # beri waktu worker mencetak log terakhir
finally:
    for w in workers:
        w.terminate()              # matikan semua worker
    for w in workers:
        w.wait()
