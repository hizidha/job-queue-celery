# producer.py
# Producer = pembangkit data masukan (input generator).
# Tugasnya membuat pesan lalu mengirimnya ke exchange di broker. Producer tidak tahu worker mana
# yang akan menerima; ia hanya menempelkan routing key. Pembagiannya diatur exchange dan binding
# di celery_config.py.
#
# Cara menjalankan:
#   python producer.py fanout                         -> 1 pengumuman diskon ke semua worker
#   python producer.py topic                          -> 5 pesanan tetap ke exchange topic
#   python producer.py wildcard                       -> 5 pesanan yang sama ke exchange wildcard
#   python producer.py simulasi --jumlah 12 --seed 7  -> pesanan acak + pengumuman diskon di tengah

import argparse
import random
import time

from celery_config import app
from tasks import proses_pesanan, ubah_harga

# Lima pesanan tetap, dipakai di mode topic dan wildcard agar hasil keduanya bisa dibandingkan.
# Formatnya: (routing key, barang yang dibeli)
PESANAN_TETAP = [
    ("pesanan.surabaya.reguler", {"kaos": 2}),
    ("pesanan.jakarta.express", {"sepatu": 1}),
    ("pesanan.surabaya.express", {"topi": 1, "kaos": 1}),
    ("pesanan.bandung.express", {"topi": 3}),        # kota baru, belum didaftarkan di binding mana pun
    ("pesanan.jakarta.reguler.cod", {"kaos": 1}),    # routing key 4 kata (COD = bayar di tempat)
]


def siapkan_queue():
    # Membuat semua exchange, queue, dan binding di broker. Aman dijalankan berulang kali.
    # Tanpa ini, pesan yang dikirim sebelum worker menyala bisa hilang karena queue-nya belum ada.
    with app.connection_for_write() as conn:
        for q in app.conf.task_queues:
            q(conn.default_channel).declare()


def kirim_pesanan(exchange, no, routing_key, barang):
    # apply_async mengirim task proses_pesanan ke broker tanpa menunggu hasilnya.
    # exchange dan routing_key menentukan queue mana saja yang menerima salinan pesan ini.
    print(f"[Producer] kirim pesanan #{no} ke {exchange} dengan routing key {routing_key}", flush=True)
    proses_pesanan.apply_async(args=[{"no": no, "barang": barang}], exchange=exchange, routing_key=routing_key)


def kirim_pengumuman(teks, diskon):
    # Exchange fan-out mengabaikan routing key, jadi dikosongkan.
    # exchange_type="fanout" wajib ditulis: tanpa itu Celery menganggap pesan tanpa routing key
    # bertipe "direct" lalu membelokkannya ke queue "celery", sehingga tidak ada worker yang menerima.
    print(f"[Producer] kirim pengumuman '{teks}' ke toko.fanout", flush=True)
    ubah_harga.apply_async(args=[teks, diskon], exchange="toko.fanout", exchange_type="fanout", routing_key="")


def main():
    # Membaca argumen dari command line, misalnya "simulasi --jumlah 12 --seed 7".
    parser = argparse.ArgumentParser(description="Producer toko online")
    parser.add_argument("mode", choices=["fanout", "topic", "wildcard", "simulasi"])
    parser.add_argument("--jumlah", type=int, default=12, help="banyaknya pesanan acak (mode simulasi)")
    parser.add_argument("--seed", type=int, default=None, help="seed acak agar urutan pesanan bisa diulang")
    args = parser.parse_args()

    siapkan_queue()

    if args.mode == "fanout":
        kirim_pengumuman("Flash sale diskon 10%", 10)

    elif args.mode in ("topic", "wildcard"):
        # Pesanan yang sama dikirim; yang berbeda hanya exchange-nya (toko.topic atau toko.wildcard).
        for no, (routing_key, barang) in enumerate(PESANAN_TETAP, start=1):
            kirim_pesanan(f"toko.{args.mode}", no, routing_key, barang)

    else:  # simulasi
        # Seed yang sama menghasilkan urutan acak yang sama, sehingga percobaan bisa diulang.
        random.seed(args.seed)
        for no in range(1, args.jumlah + 1):
            # Tepat di tengah simulasi, kantor pusat mengumumkan diskon lewat fan-out.
            if no == args.jumlah // 2 + 1:
                kirim_pengumuman("Flash sale diskon 10%", 10)
                time.sleep(2)  # shortcut: jeda tetap agar diskon diterima sebelum pesanan berikutnya; pakai konfirmasi bila harus pasti

            # Membuat satu pesanan acak: kota, layanan, lalu 1-2 jenis barang dengan jumlah 1-3.
            kota = random.choice(["surabaya", "jakarta"])
            layanan = random.choice(["reguler", "express"])
            jenis = random.sample(["kaos", "topi", "sepatu"], random.randint(1, 2))
            barang = {nama: random.randint(1, 3) for nama in jenis}

            kirim_pesanan("toko.wildcard", no, f"pesanan.{kota}.{layanan}", barang)
            time.sleep(0.5)  # jeda antarpelanggan, agar urutan di log mudah diikuti


if __name__ == "__main__":
    main()
