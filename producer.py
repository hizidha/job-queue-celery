# producer.py
# Pembangkit data masukan (input generator).
# Membuat request LONG/SHORT secara acak, mengirimnya ke broker (RabbitMQ),
# lalu menunggu dan mencetak hasil dari worker.

import argparse
import random
import time

from tasks import echo, hitung_prima

KATA = ["halo", "jaringan", "komputasi", "celery", "broker", "worker", "unesa"]

parser = argparse.ArgumentParser(description="Producer: pembangkit request acak")
parser.add_argument("--jumlah", type=int, default=10, help="banyaknya request")
parser.add_argument("--seed", type=int, default=None, help="seed acak agar hasil bisa diulang")
args = parser.parse_args()
random.seed(args.seed)

mulai = time.time()
antrian = []
for i in range(1, args.jumlah + 1):
    if random.random() < 0.3:                          # sekitar 30% request berat
        app_id, data = 1, random.randint(300_000, 1_500_000)
        hasil = hitung_prima.delay(data)               # Application ID 1 -> task hitung_prima
    else:
        app_id, data = 2, random.choice(KATA)
        hasil = echo.delay(data)                       # Application ID 2 -> task echo
    antrian.append((i, app_id, data, hasil))
    print(f"[Producer] #{i} kirim app_id={app_id} data={data}")

for i, app_id, data, hasil in antrian:                 # tunggu semua hasil
    r = hasil.get(timeout=300)
    detik = r["selesai"] - mulai
    print(f"[Producer] #{i} app_id={app_id} data={data} -> {r['hasil']} oleh {r['worker']} (selesai detik ke-{detik:.1f})")

print(f"[Producer] selesai {args.jumlah} request dalam {time.time() - mulai:.1f} detik")