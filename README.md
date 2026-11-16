# Job Queue dengan Celery

Tugas 2, Komputasi Berbasis Jaringan, S2 Informatika UNESA

| Nama | NIM |
|---|---|
| Adisaputra Zidha Noorizki | 26051905014 |
| Calycha Irmalia Kurniasari | 26051905016 |

Kami membuat simulasi toko online yang memakai Celery dengan broker RabbitMQ (CloudAMQP). Producer membangkitkan pesanan, lalu pesanan diarahkan ke empat worker: `gudang-surabaya`, `gudang-jakarta`, `kurir-express`, dan `audit`. Arah pesanan diatur oleh exchange dan routing key, sehingga satu program ini sekaligus kami pakai untuk menguji pola Fan-Out, Topic, dan Wildcard.

---

## 1. Struktur dan Alur Program

```mermaid
flowchart LR
    C[celery_config.py<br/>broker, exchange, queue] -.dibaca.-> T[tasks.py<br/>worker]
    C -.dibaca.-> P[producer.py]
    D[demo.py] -->|menyalakan 4x| T
    D -->|menjalankan| P
    P -->|pesan + routing key| X{{Exchange<br/>RabbitMQ}}
    X -->|sesuai binding| Q[(Queue tiap worker)]
    Q --> T
    T -.event.-> F[Flower]
```

| File | Peran |
|---|---|
| `celery_config.py` | Pengaturan: alamat broker, tiga exchange, dan queue tiap worker beserta binding-nya. Tidak dijalankan langsung, tetapi dibaca file lain. |
| `tasks.py` | Isi pekerjaan worker: `proses_pesanan` (menghitung tagihan) dan `ubah_harga` (menerapkan diskon). |
| `producer.py` | Pembangkit data masukan. Membuat pesanan atau pengumuman, lalu mengirimnya ke exchange. |
| `demo.py` | Menjalankan satu percobaan lengkap: kosongkan queue, nyalakan 4 worker, jalankan producer, matikan worker. |
| `evidence/` | Log hasil setiap percobaan dan data yang direkam Flower. |

Alurnya:
1. Worker dinyalakan lebih dulu. Saat menyala, worker membaca `celery_config.py` dan membuat queue-nya di RabbitMQ.
2. Producer mengirim pesan ke salah satu exchange dengan routing key, misalnya `pesanan.surabaya.express`.
3. Exchange menyalin pesan ke setiap queue yang binding-nya cocok. Pesan yang tidak cocok dengan binding mana pun dibuang.
4. Setiap worker mengambil pesan dari queue-nya, menjalankan task, dan mencetak hasilnya.
5. Worker mengirim event ke Flower, sehingga semua task bisa dipantau di browser.

Setiap worker punya tiga queue, satu untuk tiap exchange. Contohnya worker `audit` punya `fanout.audit`, `topic.audit`, dan `wildcard.audit`. Saat dinyalakan, worker hanya membaca queue yang dipilih lewat opsi `-Q`.

---

## 2. Fan-Out, Topic, dan Wildcard

Routing key pesanan berformat `pesanan.<kota>.<layanan>`. Binding setiap worker:

| Worker | Topic (harus sama persis) | Wildcard (pola) |
|---|---|---|
| gudang-surabaya | `pesanan.surabaya.reguler`, `pesanan.surabaya.express` | `pesanan.surabaya.*` |
| gudang-jakarta | `pesanan.jakarta.reguler`, `pesanan.jakarta.express` | `pesanan.jakarta.*` |
| kurir-express | `pesanan.surabaya.express`, `pesanan.jakarta.express` | `pesanan.*.express` |
| audit | keempat kombinasi di atas | `pesanan.#` |

Pada Wildcard, `*` mewakili tepat satu kata, sedangkan `#` mewakili nol kata atau lebih.

### Fan-Out

Producer mengirim satu pengumuman "Flash sale diskon 10%" ke exchange `toko.fanout`. Keempat worker menerimanya dan menurunkan daftar harganya (kaos Rp 75.000 menjadi Rp 67.500). Fan-Out tidak membaca routing key sama sekali, jadi semua queue yang terhubung mendapat salinan.

Pola ini dibutuhkan karena setiap worker adalah proses terpisah dengan daftar harga masing-masing. Kalau ada satu worker yang tidak menerima pengumuman, worker itu akan tetap menagih dengan harga lama.

### Topic dan Wildcard

Lima pesanan yang sama kami kirim ke exchange `toko.topic` dan `toko.wildcard`:

| # | Routing key | Diterima (Topic) | Diterima (Wildcard) |
|---|---|---|---|
| 1 | `pesanan.surabaya.reguler` | gudang-surabaya, audit | gudang-surabaya, audit |
| 2 | `pesanan.jakarta.express` | gudang-jakarta, kurir, audit | gudang-jakarta, kurir, audit |
| 3 | `pesanan.surabaya.express` | gudang-surabaya, kurir, audit | gudang-surabaya, kurir, audit |
| 4 | `pesanan.bandung.express` | tidak ada | kurir, audit |
| 5 | `pesanan.jakarta.reguler.cod` | tidak ada | audit |

- Pesanan #1 sampai #3 hasilnya sama. Bedanya ada di jumlah binding: Topic butuh 10 routing key, sedangkan Wildcard cukup 4 pola.
- Pesanan #4 berasal dari kota yang belum didaftarkan. Di Topic pesanan ini hilang, sedangkan di Wildcard tetap sampai ke kurir (`pesanan.*.express`) dan audit (`pesanan.#`).
- Pesanan #5 punya empat kata. Pola `pesanan.jakarta.*` tidak cocok karena `*` hanya menerima satu kata, sehingga hanya audit (`#`) yang menerimanya.
- Di Flower, pesanan #4 dan #5 pada Topic berstatus PENDING terus, karena tidak pernah masuk ke queue mana pun.

### Perbedaan

| | Fan-Out | Topic | Wildcard |
|---|---|---|---|
| Membaca routing key | Tidak | Ya, harus sama persis | Ya, boleh pola `*` dan `#` |
| Pesan sampai ke | Semua queue yang terhubung | Queue dengan binding yang sama persis | Semua queue yang polanya cocok |
| Binding di percobaan ini | 1 per worker | 10 routing key | 4 pola |
| Routing key baru | Tidak berpengaruh | Pesan hilang sampai binding ditambah | Tetap sampai jika cocok pola |
| Cocok untuk | Broadcast ke semua worker | Jalur yang sedikit dan tetap | Jenis pesan yang banyak dan bertambah, misalnya log |

Di RabbitMQ, Topic dan Wildcard sama-sama memakai exchange bertipe `topic`. Topic tanpa pola sebenarnya berperilaku seperti exchange `direct`.

---

## 3. Program dengan Konsep Celery

Mode `simulasi` menggabungkan semuanya. Producer membuat 12 pesanan acak yang dikirim lewat exchange Wildcard. Setelah pesanan ke-6, producer mengirim pengumuman diskon lewat Fan-Out. Dari log terlihat tagihan untuk barang yang sama ikut turun:

```
[gudang-jakarta ] terima pesanan #4 (pesanan.jakarta.reguler): 3x kaos -> tagihan Rp 235.000
[gudang-surabaya] PENGUMUMAN 'Flash sale diskon 10%' -> harga baru: kaos Rp 67.500, ...
[gudang-surabaya] terima pesanan #10 (pesanan.surabaya.reguler): 3x kaos -> tagihan Rp 212.500
```

Konsep Celery yang kami pakai:

| Konsep | Penerapan |
|---|---|
| Task | `proses_pesanan` dan `ubah_harga` di `tasks.py` |
| Worker dengan nama dan queue sendiri | `celery -A tasks worker -n audit@toko -Q wildcard.audit` |
| Exchange, queue, dan binding kustom | `task_queues` di `celery_config.py` |
| Routing saat mengirim | `apply_async(exchange=..., routing_key=...)` di `producer.py` |
| Membaca info pesan di task | `self.request.delivery_info["routing_key"]` dan `self.request.hostname` |
| Kontrol worker | `app.control.purge()` dan `app.control.ping()` di `demo.py` |
| Monitoring | Event task dikirim ke Flower (`worker_send_task_events`, `task_send_sent_event`) |

Hasil task tidak dikirim balik ke producer (`task_ignore_result=True`), karena satu pesan bisa dikerjakan beberapa worker sekaligus. Hasilnya kami lihat dari log worker dan dari Flower.

Masalah yang kami temui selama pengerjaan:
- **Pesan Fan-Out tidak sampai ke siapa pun.** Jika routing key kosong, Celery menganggap pesan bertipe `direct` dan membelokkannya ke queue `celery`. Solusinya menulis `exchange_type="fanout"` saat mengirim.
- **Koneksi ditolak CloudAMQP.** Paket gratis hanya mengizinkan 20 koneksi. Flower menanyai setiap worker 8 hal sekaligus, dan tiap pertanyaan memakai 2 koneksi. Solusinya mematikan pool koneksi (`broker_pool_limit=0`) dan membuat Flower bertanya satu per satu. Setelah itu, puncaknya sekitar 13 koneksi.

---

## 4. Producer

`producer.py` adalah pembangkit data masukan. Producer hanya menentukan exchange dan routing key, tanpa tahu worker mana yang akan menerima.

| Perintah | Yang dikirim |
|---|---|
| `python producer.py fanout` | 1 pengumuman diskon |
| `python producer.py topic` | 5 pesanan tetap ke `toko.topic` |
| `python producer.py wildcard` | 5 pesanan yang sama ke `toko.wildcard` |
| `python producer.py simulasi --jumlah 12 --seed 7` | Pesanan acak (kota, layanan, barang, jumlah), ditambah pengumuman diskon di tengah |

Opsi `--seed` membuat urutan pesanan acak bisa diulang persis sama.

---

## 5. Cara Menjalankan (Windows PowerShell)

### Persiapan (sekali saja)

```powershell
python -m venv myenv
myenv\Scripts\activate
pip install -r requirements.txt
setx CELERY_BROKER_URL "amqps://user:pass@xxx.rmq.cloudamqp.com/user"
```

URL broker diambil dari CloudAMQP (instance Little Lemur, bagian AMQP details). `setx` baru berlaku di terminal yang dibuka sesudahnya. Untuk mengecek perhitungan tagihan tanpa broker, jalankan `python tasks.py`; hasilnya `OK`.

### Urutan menjalankan

**Terminal 1: Flower.** Dijalankan paling awal, karena Flower hanya mencatat task yang terjadi selama ia menyala.

```powershell
celery -A tasks flower
```

Buka `http://localhost:5555`. Halaman *Workers* menampilkan keempat worker, dan halaman *Tasks* menampilkan setiap pesan beserta status dan routing key-nya.

**Terminal 2: percobaan.**

```powershell
python demo.py fanout
python demo.py topic
python demo.py wildcard
python demo.py simulasi
```

`demo.py` mengosongkan queue, menyalakan 4 worker, menunggu sampai semuanya siap, menjalankan producer, lalu mematikan worker.

### Cara manual

Jika ingin setiap worker tampil di terminalnya sendiri, worker dinyalakan dulu, baru producer. Contoh untuk Wildcard:

```powershell
celery -A tasks worker -P solo -Q wildcard.gudang-surabaya -n gudang-surabaya@toko
celery -A tasks worker -P solo -Q wildcard.gudang-jakarta -n gudang-jakarta@toko
celery -A tasks worker -P solo -Q wildcard.kurir-express -n kurir-express@toko
celery -A tasks worker -P solo -Q wildcard.audit -n audit@toko
python producer.py wildcard
```

Untuk percobaan lain, ganti awalan queue menjadi `fanout.` atau `topic.`. Untuk simulasi, setiap worker membaca dua queue, misalnya `-Q wildcard.audit,fanout.audit`. `-P solo` wajib di Windows. Sisa pesan dari percobaan sebelumnya bisa dibuang dengan `celery -A tasks purge -f`.

Versi tugas kami sebelumnya tersimpan di branch `versi-lama`.
