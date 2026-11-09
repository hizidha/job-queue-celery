from celery import chain
from tasks import process_data, generate_report  # [MODIFIKASI] semula '.tasks', gagal jika dijalankan sebagai skrip

def create_task_chain(raw_data):
    """
    Menggunakan chain primitive milik Celery
    """
    job = chain(
        process_data.s(raw_data),
        generate_report.s()
    )
    return job.apply_async()