# Job Queue with Python Celery

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Celery](https://img.shields.io/badge/Celery-5.x-37B24D?style=for-the-badge&logo=celery&logoColor=white)](https://docs.celeryq.dev/)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-CloudAMQP-FF6600?style=for-the-badge&logo=rabbitmq&logoColor=white)](https://www.cloudamqp.com/)

Asynchronous job queue processing implementation using **Celery** and **Flower** monitoring, featuring **Fan-Out**, **Topic**, and **Wildcard** message routing patterns with custom data producers.

---

## 📁 Struktur Repositori

| File | Deskripsi / Fungsi |
| :--- | :--- |

> **Info Broker:** Proyek ini menggunakan **RabbitMQ di CloudAMQP** (Free Tier / Little Lemur). Tidak memerlukan instalasi RabbitMQ lokal atau Docker.

---

## 🛠️ 1. Persiapan & Konfigurasi (Windows)

### A. Setup Broker RabbitMQ (CloudAMQP)
1. Kunjungi [CloudAMQP Console](https://customer.cloudamqp.com/login) dan lakukan *Sign Up* / *Sign In*.
2. Klik name team, kemudian pilih yes untuk pertanyaan 1 dan klik no di pertanyaan 2
3. Setelah berhasil sign in, kita masuk menu [Instance List](https://customer.cloudamqp.com/instance) lalu klik tombol **Create New Instance**.
4. Mengisi **Name** dan **Tag** sesuai kebutuhan.
5. Memilih list **Plan**: RabbitMQ - `Little Lemur (Free)`.
6. Klik **Select Region**, pilih provider **Amazon Web Services (AWS)** dan region **AP-NorthEast-2 (Seoul)**.
7. Klik **Review**, pastikan data sesuai, lalu klik **Create Instance**.
8. Buka detail instance yang telah dibuat, lalu catat **URL** pada bagian **AMQP details**.