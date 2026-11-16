# tasks.py
# Daftar task, yaitu pekerjaan yang dijalankan worker saat menerima pesan.
# Worker dinyalakan dengan: celery -A tasks worker ...
#
# Ada dua task:
#   proses_pesanan -> menghitung tagihan sebuah pesanan
#   ubah_harga     -> menerapkan diskon ke daftar harga
#
# Penting: setiap worker adalah proses terpisah, sehingga setiap worker punya salinan HARGA sendiri.
# Jika harga berubah, semua worker harus diberi tahu. Itulah gunanya fan-out.

import sys

from celery_config import app

HARGA = {"kaos": 75_000, "topi": 50_000, "sepatu": 350_000}   # harga awal per barang (rupiah)
ONGKIR = {"reguler": 10_000, "express": 25_000}                # ongkos kirim per layanan


def rupiah(n):
    # 1234500 -> "Rp 1.234.500"
    return "Rp " + f"{n:,}".replace(",", ".")


def hitung_tagihan(barang, layanan):
    # barang berbentuk {"kaos": 2, "topi": 1}, artinya 2 kaos dan 1 topi.
    # Tagihan = jumlah (harga x banyaknya) untuk setiap barang + ongkir layanan.
    # Layanan yang tidak dikenal dihitung dengan ongkir express.
    total_barang = sum(HARGA[nama] * jumlah for nama, jumlah in barang.items())
    return total_barang + ONGKIR.get(layanan, ONGKIR["express"])


def cetak(task, teks):
    # Mencetak satu baris log diawali nama worker, misalnya "[audit          ] ...".
    # self.request.hostname berisi nama worker seperti "audit@toko"; kita ambil bagian sebelum "@".
    # Satu baris ditulis sekaligus agar log dari beberapa worker tidak saling menempel.
    nama = task.request.hostname.split("@")[0]
    sys.stdout.write(f"[{nama:<15}] {teks}\n")
    sys.stdout.flush()


# bind=True membuat task menerima "self", sehingga bisa membaca info pesan lewat self.request.
@app.task(bind=True)
def proses_pesanan(self, pesanan):
    # pesanan berbentuk {"no": 3, "barang": {"topi": 1, "kaos": 1}}.
    # Routing key tidak ikut di isi pesan; kita baca dari info pengiriman yang ditempel broker.
    routing_key = self.request.delivery_info.get("routing_key")   # contoh: "pesanan.surabaya.express"
    kata = routing_key.split(".")                                 # ["pesanan", "surabaya", "express"]
    layanan = kata[2] if len(kata) >= 3 else ""

    isi = ", ".join(f"{jumlah}x {nama}" for nama, jumlah in pesanan["barang"].items())
    total = hitung_tagihan(pesanan["barang"], layanan)
    cetak(self, f"terima pesanan #{pesanan['no']} ({routing_key}): {isi} -> tagihan {rupiah(total)}")


@app.task(bind=True)
def ubah_harga(self, pengumuman, diskon_persen):
    # Menurunkan semua harga di daftar HARGA milik worker ini sebesar diskon_persen.
    # Contoh diskon 10%: 75.000 x 90 / 100 = 67.500.
    for nama in HARGA:
        HARGA[nama] = HARGA[nama] * (100 - diskon_persen) // 100
    daftar = ", ".join(f"{nama} {rupiah(h)}" for nama, h in HARGA.items())
    cetak(self, f"PENGUMUMAN '{pengumuman}' -> harga baru: {daftar}")


if __name__ == "__main__":
    # Pengecekan cepat perhitungan tanpa broker: python tasks.py  -> mencetak OK
    assert hitung_tagihan({"kaos": 2, "topi": 1}, "reguler") == 210_000    # 150.000 + 50.000 + 10.000
    assert hitung_tagihan({"sepatu": 1}, "express") == 375_000             # 350.000 + 25.000
    assert rupiah(1_234_500) == "Rp 1.234.500"
    print("OK")
