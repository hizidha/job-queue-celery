# tasks.py
# Daftar task yang dikerjakan worker Celery.
# hitung_prima dan echo adalah aplikasi LONG dan SHORT dari Tugas 1, sekarang menjadi task Celery.

import time

from celery_config import app


@app.task(bind=True)
def hitung_prima(self, n):
    # Application ID 1 (LONG): banyaknya bilangan prima dari 2 sampai n.
    # Sengaja dibuat lambat (setiap angka dicek satu per satu) agar terasa sebagai pekerjaan berat.
    hasil = sum(all(i % d for d in range(2, int(i ** 0.5) + 1)) for i in range(2, n + 1))
    print(f"[{self.request.hostname}] LONG n={n} -> {hasil}")
    return {"worker": self.request.hostname, "hasil": hasil, "selesai": time.time()}


@app.task(bind=True)
def echo(self, teks):
    # Application ID 2 (SHORT): kembalikan teks dalam huruf besar.
    print(f"[{self.request.hostname}] SHORT '{teks}' -> '{teks.upper()}'")
    return {"worker": self.request.hostname, "hasil": teks.upper(), "selesai": time.time()}