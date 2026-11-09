import json
import sys

from celery_config import fanout, topic   # [MODIFIKASI] exchange untuk uji pola pesan
from tasks import process_and_report, process_data   # [MODIFIKASI] semula '.tasks'
from run_chain import create_task_chain              # [MODIFIKASI] semula '.run_chain'


def kirim(exchange, routing_key, isi):
    # [MODIFIKASI] kirim task process_data lewat exchange tertentu dengan routing key tertentu
    process_data.apply_async(args=[isi], exchange=exchange, routing_key=routing_key)
    print(f"[Producer] kirim ke {exchange.name} routing_key={routing_key!r}: {isi}", flush=True)


if __name__ == "__main__":
    # [MODIFIKASI] pilih mode: contoh (kode asli), fanout, topic, wildcard
    mode = sys.argv[1] if len(sys.argv) > 1 else "contoh"

    if mode == "contoh":
        # Cara 1: Dengan orchestrator
        print("=== Cara 1: Dengan orchestrator task ===")
        raw_input = "Ini adalah data contoh yang harus diproses dan dilaporkan."

        result = process_and_report.delay(raw_input)
        final_output = result.get()
        print(f"Final result: {json.dumps(final_output, indent=2)}")

        # Cara 2: Dengan chain milik Celery
        print("\n=== Cara 2: Dengan chain milik Celery ===")
        chain_result = create_task_chain(raw_input)
        chain_output = chain_result.get()
        print(f"Chain result: {json.dumps(chain_output, indent=2)}")

    elif mode == "fanout":
        # Satu pesan disalin ke fan.w1, fan.w2, fan.w3
        kirim(fanout, "apa.saja", "pengumuman untuk semua worker")

    elif mode == "topic":
        # Routing key harus sama persis dengan binding queue
        kirim(topic, "job.data", "pesan A")     # -> topic.data
        kirim(topic, "job.report", "pesan B")   # -> topic.report

    elif mode == "wildcard":
        # Routing key dicocokkan dengan pola job.* dan job.#
        kirim(topic, "job.data", "pesan C")          # -> wild.bintang, wild.pagar
        kirim(topic, "job.data.urgent", "pesan D")   # -> wild.pagar saja
        kirim(topic, "job", "pesan E")               # -> wild.pagar saja
        kirim(topic, "lain.data", "pesan F")         # -> tidak ada, pesan dibuang

    else:
        sys.exit("Pilihan: contoh, fanout, topic, wildcard")


"""
Untuk menjalankan worker pada queues yang berbeda, gunakan perintah-perintah berikut pada terminal terpisah:
([MODIFIKASI] -P solo wajib di Windows; --queue diganti -Q karena tidak dikenali Celery versi baru)

# Mode contoh
celery -A celery_config worker --loglevel=info -P solo -Q data_processing -n data@%h
celery -A celery_config worker --loglevel=info -P solo -Q report_generation -n report@%h
celery -A celery_config worker --loglevel=info -P solo -Q celery -n general@%h

# Mode fanout
celery -A celery_config worker --loglevel=info -P solo -Q fan.w1 -n fan1@%h
celery -A celery_config worker --loglevel=info -P solo -Q fan.w2 -n fan2@%h
celery -A celery_config worker --loglevel=info -P solo -Q fan.w3 -n fan3@%h

# Mode topic
celery -A celery_config worker --loglevel=info -P solo -Q topic.data -n topic-data@%h
celery -A celery_config worker --loglevel=info -P solo -Q topic.report -n topic-report@%h

# Mode wildcard
celery -A celery_config worker --loglevel=info -P solo -Q wild.bintang -n bintang@%h
celery -A celery_config worker --loglevel=info -P solo -Q wild.pagar -n pagar@%h
"""
