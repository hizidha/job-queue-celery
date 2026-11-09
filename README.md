# Job Queue dengan Celery

Tugas 2 – Komputasi Berbasis Jaringan, S2 Informatika UNESA

Pada tugas ini kami memindahkan sistem broker-worker dari Tugas 1 ke Celery, dengan RabbitMQ di CloudAMQP sebagai broker. Pembahasannya kami bagi menjadi tiga bagian sesuai soal: pola penanganan pesan pada kode contoh perkuliahan, program Tugas 1 versi Celery, dan producer sebagai pembangkit data.

> **Info Broker:** Proyek ini menggunakan **RabbitMQ di CloudAMQP** (Free Tier / Little Lemur). Tidak memerlukan instalasi RabbitMQ lokal atau Docker.

---

## 1. Fan-Out, Topic, dan Wildcard

Bagian ini berada di folder `message-handling-sc-example/`. Kami berangkat dari kode contoh perkuliahan (`ifk-celery`), lalu memodifikasinya untuk menguji tiga pola penanganan pesan. Task bawaan `process_data` kami kirim lewat ketiga pola itu, lalu kami amati worker mana yang menerimanya.

Sebelum menambah fitur, kami lebih dulu memperbaiki empat hal agar kode contoh bisa berjalan:
- `from time import time` diganti `import time`, karena kode memanggil `time.sleep()`.
- Import relatif `from .tasks` diganti `from tasks`, agar `producer.py` bisa dijalankan langsung.
- `.get()` di dalam task orchestrator diberi `disable_sync_subtasks=False`, karena Celery melarang menunggu task lain dari dalam task.
- Opsi `--queue` pada perintah worker diganti `-Q`, karena tidak dikenali Celery versi terbaru.

Setelah itu, kami menambahkan exchange fan-out dan topic beserta queue-nya di `celery_config.py`, tiga mode baru di `producer.py`, serta nama worker dan routing key di log `process_data`. Semua perubahan ditandai dengan komentar `[MODIFIKASI]`.

### Fan-Out

```mermaid
flowchart LR
    P[Producer] -->|pengumuman| F{{fanout}}
    F --> Q1[fan.w1]
    F --> Q2[fan.w2]
    F --> Q3[fan.w3]
```

Pada fan-out, satu pesan disalin ke semua queue yang terhubung, apa pun routing key-nya. Saat kami mengirim satu pengumuman, ketiga worker memprosesnya bersamaan. Pola ini cocok untuk *broadcast*, misalnya memberi tahu semua worker sekaligus.

### Topic

```mermaid
flowchart LR
    P1[Producer] -->|job.data| T{{topic}}
    P2[Producer] -->|job.report| T
    T -->|job.data| Q1[topic.data]
    T -->|job.report| Q2[topic.report]
```

Pada topic, pesan hanya masuk ke queue yang routing key-nya sama persis. Pesan `job.data` masuk ke `topic.data`, dan pesan `job.report` masuk ke `topic.report`. Dengan cara ini, setiap jenis pekerjaan punya jalurnya sendiri.

### Wildcard

```mermaid
flowchart LR
    P[Producer] --> T{{topic}}
    T -->|job.*| Q1[wild.bintang]
    T -->|job.#| Q2[wild.pagar]
```

Wildcard masih memakai exchange topic, tetapi routing key-nya berupa pola. Tanda `*` mewakili tepat satu kata, sedangkan `#` mewakili nol kata atau lebih.

Hasilnya:
- `job.data` masuk ke kedua queue.
- `job.data.urgent` hanya masuk ke `wild.pagar`, karena setelah `job` ada dua kata, sedangkan `*` hanya menerima satu.
- `job` saja juga hanya masuk ke `wild.pagar`, karena `#` boleh kosong.
- `lain.data` tidak cocok dengan pola mana pun, sehingga pesan dibuang.

Pola ini berguna untuk menampung banyak jenis pesan dalam satu queue, misalnya untuk pencatatan log.

### Perbedaan

| Pola | Queue tujuan | Contoh pemakaian |
|---|---|---|
| Fan-Out | Semua queue yang terhubung | Broadcast ke semua worker |
| Topic | Queue dengan routing key yang sama persis | Memisahkan pekerjaan per jenis |
| Wildcard | Queue dengan pola yang cocok (`*`, `#`) | Mengumpulkan banyak jenis pesan, misalnya log |

Selama percobaan, kami juga menemukan bahwa pesan tetap tersimpan di queue sampai ada worker yang mengambilnya. Karena itu, queue perlu dikosongkan sebelum setiap percobaan agar log tidak tercampur sisa pesan sebelumnya.

Log lengkapnya ada di `message-handling-sc-example/evidence/`.

---

## 2. Program Tugas 1 dengan Celery

```mermaid
flowchart LR
    P[Producer] -->|task| R[(RabbitMQ)]
    R --> W1[Worker 1]
    R --> W2[Worker 2]
    R --> W3[Worker 3]
```

Di Tugas 1, kami membangun broker sendiri dengan socket. Sekarang peran itu diambil oleh RabbitMQ. Aplikasi LONG dan SHORT kini menjadi dua task Celery, yaitu `hitung_prima` dan `echo`. Worker socket diganti worker Celery, dan client diganti producer.

Kedua task masuk ke satu queue bernama `tugas`, yang dilayani tiga worker. Seperti di Tugas 1, setiap worker bisa mengerjakan LONG maupun SHORT. Task LONG tetap sengaja dibuat berat, karena setiap angka dicek satu per satu.

Tugas 1 punya dua strategi pembagian beban. Di Celery, keduanya kami wujudkan dengan mengatur **berapa task yang boleh dipegang satu worker sekaligus**.

### Round Robin

```mermaid
flowchart LR
    Q[(Queue)] -->|task 1, 4, 7, 10| W1[w1]
    Q -->|task 2, 5, 8, 11| W2[w2]
    Q -->|task 3, 6, 9, 12| W3[w3]
```

Pada Round Robin, setiap worker boleh memegang hingga empat task sekaligus, dan RabbitMQ membagikannya bergiliran. Pembagian ini tidak melihat apakah worker sedang sibuk. Akibatnya, jika satu worker kebagian beberapa task berat, task ringan di belakangnya ikut menunggu, padahal worker lain sudah menganggur.

### Least Connections

```mermaid
flowchart LR
    Q[(Queue)] --> K{Worker mana<br/>yang kosong?}
    K --> W[Worker itu<br/>mengambil 1 task]
```

Pada Least Connections, setiap worker hanya boleh memegang satu task. Task berikutnya tetap di queue sampai ada worker yang selesai, lalu diambil oleh worker itu. Sebuah task baru dianggap selesai setelah benar-benar dikerjakan, sehingga worker yang masih sibuk tidak bisa mengambil task lain. Hasilnya, task selalu jatuh ke worker yang sedang kosong.

Strategi ini sekaligus memperbaiki kelemahan Tugas 1. Waktu itu, pemerataan dihitung dari request yang *sudah selesai*, sehingga gagal saat banyak request datang bersamaan. Sekarang yang dilihat adalah worker yang *sedang kosong*.

### Hasil

Kami mengirim 12 request yang sama ke tiga worker dengan kedua strategi:
- **Round Robin** membagi task persis bergiliran. Worker w2 kebagian tiga task LONG berturut-turut, sehingga task SHORT terakhir baru selesai di detik ke-17,6. Total waktunya **17,6 detik**.
- **Least Connections** menyelesaikan task SHORT yang sama di detik ke-4,9, karena task itu diambil worker yang sedang kosong. Total waktunya **12,2 detik**.

Log lengkapnya ada di `evidence/hasil-round-robin.txt` dan `evidence/hasil-least-connections.txt`.

---

## 3. Producer

```mermaid
flowchart LR
    A[Buat request acak] --> B[Kirim ke RabbitMQ]
    B --> C[Worker mengerjakan]
    C --> D[Producer mencetak hasil]
```

`producer.py` adalah pembangkit data masukan. Producer membuat request acak: sekitar 30% berupa LONG dengan angka antara 300.000 dan 1.500.000, dan sisanya SHORT berupa kata acak. Semua request dikirim ke RabbitMQ, lalu producer menunggu hasilnya dan mencetak jawaban, worker yang mengerjakan, serta detik selesainya.

```
[Producer] #3 kirim app_id=1 data=1423826
[Producer] #3 app_id=1 data=1423826 -> 108757 oleh w1@adisaputra-zidha (selesai detik ke-7.9)
[Producer] selesai 12 request dalam 12.2 detik
```

Jumlah request bisa diatur dengan `--jumlah`. Opsi `--seed` membuat urutan acaknya bisa diulang persis sama, sehingga kedua strategi bisa dibandingkan secara adil.

---

## 🛠️ Persiapan & Menjalankan Program (Windows)

Semua perintah di bawah dijalankan di **PowerShell**.

### A. Setup Broker RabbitMQ (CloudAMQP)
1. Kunjungi [CloudAMQP Console](https://customer.cloudamqp.com/login) dan lakukan *Sign Up* / *Sign In*.
2. Isi nama team, lalu pilih **Yes** untuk pertanyaan pertama dan **No** untuk pertanyaan kedua.
3. Setelah berhasil sign in, masuk ke menu [Instance List](https://customer.cloudamqp.com/instance), lalu klik tombol **Create New Instance**.
4. Isi **Name** dan **Tag** sesuai kebutuhan.
5. Pilih **Plan**: RabbitMQ - `Little Lemur (Free)`.
6. Klik **Select Region**, pilih provider **Amazon Web Services (AWS)** dan region **AP-NorthEast-2 (Seoul)**.
7. Klik **Review**, pastikan data sesuai, lalu klik **Create Instance**.
8. Buka detail instance yang telah dibuat, lalu catat **URL** pada bagian **AMQP details**.

### B. Setup Environment Python (cukup sekali)

```powershell
git clone <url-repo-ini>
cd job-queue-celery
python -m venv myenv
myenv\Scripts\activate
pip install -r requirements.txt
```

Jika `activate` ditolak dengan pesan *running scripts is disabled*, jalankan perintah berikut sekali, lalu ulangi `activate`:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

### C. Mengisi URL CloudAMQP

URL dari langkah A.8 disimpan di environment variable `CELERY_BROKER_URL`, bukan di kode, karena di dalamnya ada password. Ada dua cara:

**Cara 1: per terminal.** Berlaku hanya untuk terminal tempat perintah ini dijalankan, jadi harus diulang di setiap terminal baru.

```powershell
$env:CELERY_BROKER_URL="amqps://user:pass@xxx.rmq.cloudamqp.com/user"
```

**Cara 2: permanen.** Cukup sekali, tetapi hanya berlaku untuk terminal yang dibuka **sesudahnya**. Jika memakai terminal VS Code, tutup VS Code sepenuhnya lalu buka lagi.

```powershell
setx CELERY_BROKER_URL "amqps://user:pass@xxx.rmq.cloudamqp.com/user"
```

Untuk memastikan URL sudah terbaca, jalankan perintah berikut. Hasilnya harus menampilkan URL tadi, bukan baris kosong.

```powershell
echo $env:CELERY_BROKER_URL
```

### D. Memastikan Koneksi ke CloudAMQP

Dari folder utama `job-queue-celery`:

```powershell
celery -A tasks purge -f
```

Koneksi berhasil jika muncul `No messages purged from 1 known task queues.` (atau `Purged N messages ...`).

Dari folder `message-handling-sc-example`:

```powershell
cd message-handling-sc-example
celery -A celery_config purge -f
```

Koneksi berhasil jika muncul `No messages purged from 10 known task queues.`

Jika muncul `WinError 10061 ... actively refused it`, berarti URL belum terbaca dan program mencoba menyambung ke RabbitMQ lokal. Ulangi langkah C di terminal tersebut.

### E. Menyiapkan Setiap Terminal

Beberapa percobaan butuh 3–4 terminal yang berjalan bersamaan: satu untuk setiap worker dan satu untuk producer. Di Windows Terminal, terminal baru bisa dibuka berdampingan dengan **Alt+Shift+D**.

Setiap terminal baru perlu disiapkan dulu:

```powershell
cd <lokasi>\job-queue-celery          # untuk Bagian 2 dan 3
cd <lokasi>\job-queue-celery\message-handling-sc-example   # untuk Bagian 1
<lokasi>\job-queue-celery\myenv\Scripts\activate
$env:CELERY_BROKER_URL="amqps://..."   # lewati jika memakai Cara 2 di langkah C
```

Setiap percobaan mengikuti pola yang sama:
1. Kosongkan queue dengan `purge`, agar tidak tercampur sisa pesan percobaan sebelumnya.
2. Nyalakan semua worker dan tunggu sampai masing-masing menampilkan `ready`. Worker harus menyala lebih dulu, karena worker-lah yang membuat exchange dan queue di RabbitMQ.
3. Jalankan producer.
4. Setelah selesai, hentikan semua worker dengan **Ctrl+C**.

### F. Menjalankan Bagian 1 (folder `message-handling-sc-example`)

**Fan-Out** (4 terminal)

```powershell
# Terminal 1
celery -A celery_config purge -f
celery -A celery_config worker --loglevel=info -P solo -Q fan.w1 -n fan1@%h
# Terminal 2
celery -A celery_config worker --loglevel=info -P solo -Q fan.w2 -n fan2@%h
# Terminal 3
celery -A celery_config worker --loglevel=info -P solo -Q fan.w3 -n fan3@%h
# Terminal 4
python producer.py fanout
```

Hasil yang diharapkan: ketiga worker memproses `pengumuman untuk semua worker`.

**Topic** (3 terminal)

```powershell
# Terminal 1
celery -A celery_config purge -f
celery -A celery_config worker --loglevel=info -P solo -Q topic.data -n topic-data@%h
# Terminal 2
celery -A celery_config worker --loglevel=info -P solo -Q topic.report -n topic-report@%h
# Terminal 3
python producer.py topic
```

Hasil yang diharapkan: `topic-data` hanya menerima `pesan A`, dan `topic-report` hanya menerima `pesan B`.

**Wildcard** (3 terminal)

```powershell
# Terminal 1
celery -A celery_config purge -f
celery -A celery_config worker --loglevel=info -P solo -Q wild.bintang -n bintang@%h
# Terminal 2
celery -A celery_config worker --loglevel=info -P solo -Q wild.pagar -n pagar@%h
# Terminal 3
python producer.py wildcard
```

Hasil yang diharapkan: `bintang` hanya menerima `pesan C`, sedangkan `pagar` menerima `pesan C`, `D`, dan `E`. `Pesan F` tidak diterima siapa pun.

**Contoh bawaan perkuliahan** (4 terminal)

```powershell
# Terminal 1
celery -A celery_config purge -f
celery -A celery_config worker --loglevel=info -P solo -Q data_processing -n data@%h
# Terminal 2
celery -A celery_config worker --loglevel=info -P solo -Q report_generation -n report@%h
# Terminal 3
celery -A celery_config worker --loglevel=info -P solo -Q celery -n general@%h
# Terminal 4
python producer.py contoh
```

Hasil yang diharapkan: terminal producer menampilkan `Final result` dan `Chain result`, masing-masing berisi `report_id`.

### G. Menjalankan Bagian 2 dan 3 (folder utama)

**Cara otomatis** (1 terminal). `run.py` menyalakan 3 worker, menjalankan producer, lalu mematikan worker kembali.

```powershell
celery -A tasks purge -f
python run.py round-robin

celery -A tasks purge -f
python run.py least-connections
```

**Cara manual** (4 terminal). Cara ini cocok jika setiap worker ingin terlihat di terminalnya sendiri. Untuk Round Robin gunakan angka `4`; untuk Least Connections hentikan worker, lalu nyalakan ulang dengan angka `1`.

```powershell
# Terminal 1
celery -A tasks purge -f
celery -A tasks worker -P solo -Q tugas -n w1@%h --prefetch-multiplier 4
# Terminal 2
celery -A tasks worker -P solo -Q tugas -n w2@%h --prefetch-multiplier 4
# Terminal 3
celery -A tasks worker -P solo -Q tugas -n w3@%h --prefetch-multiplier 4
# Terminal 4
python producer.py --jumlah 12 --seed 7
```

Hasil yang diharapkan: producer menampilkan 12 hasil beserta nama worker dan detik selesainya, lalu `selesai 12 request dalam ... detik`. Gunakan `--seed 7` di kedua strategi agar request-nya sama dan perbandingannya adil.

### H. Kendala yang Mungkin Muncul

| Gejala | Penyebab dan solusi |
|---|---|
| `WinError 10061 ... actively refused it` | URL CloudAMQP belum terbaca. Ulangi langkah C di terminal tersebut. |
| Log berisi pesan dari percobaan sebelumnya | Queue belum dikosongkan. Jalankan `purge` sebelum menyalakan worker. |
| Worker tidak menerima pesan apa pun | Producer dijalankan sebelum worker `ready`, atau perintah producer salah mode. |
| Error koneksi saat banyak worker menyala | Paket gratis CloudAMQP membatasi jumlah koneksi. Hentikan worker dari percobaan sebelumnya. |
| Label `WARNING/MainProcess` di log | Bukan error. Celery memang mencetak `print()` dari task pada level WARNING. |
| Hasil waktu berbeda dengan README | Wajar, karena bergantung pada kecepatan laptop dan jaringan. Yang dibandingkan adalah polanya. |

| File | Isi |
|---|---|
| `message-handling-sc-example/` | Kode contoh `ifk-celery` yang dimodifikasi (Bagian 1) |
| `message-handling-sc-example/evidence/` | Log mode `contoh`, `fanout`, `topic`, `wildcard` |
| `celery_config.py` | Alamat broker dan queue (Bagian 2) |
| `tasks.py` | Task `hitung_prima` (LONG) dan `echo` (SHORT) |
| `producer.py` | Producer (Bagian 3) |
| `run.py` | Menjalankan 3 worker dan producer sekaligus |
| `evidence/` | Log Round Robin dan Least Connections |