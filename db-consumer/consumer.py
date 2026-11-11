import json
import os
import signal
import sqlite3
import time

from confluent_kafka import Consumer, KafkaError, KafkaException

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "kullanici.olusturuldu"
GROUP_ID = "db-consumer-group"  # log-consumer'dan FARKLI -> fan-out
DB_FILE = os.getenv("DB_FILE", "/app/users.db")

calisiyor = True


def durdur(signum, frame):
    global calisiyor
    print(f"[DB] Sinyal alındı ({signum}), kapatılıyor...", flush=True)
    calisiyor = False


signal.signal(signal.SIGTERM, durdur)
signal.signal(signal.SIGINT, durdur)


def veritabani_hazirla():
    conn = sqlite3.connect(DB_FILE)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS users ("
        "id TEXT PRIMARY KEY, "
        "ad TEXT, "
        "email TEXT)"
    )
    conn.commit()
    print(f"[DB] Veritabanı hazır: {DB_FILE} (tablo: users)", flush=True)
    return conn


def consumer_olustur():
    """Kafka hazır olana kadar tekrar deneyerek consumer oluşturur."""
    deneme = 0
    while calisiyor:
        deneme += 1
        try:
            consumer = Consumer({
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "group.id": GROUP_ID,
                "auto.offset.reset": "earliest",
                # Offset'i ancak DB'ye yazdıktan sonra commit ediyoruz (at-least-once)
                "enable.auto.commit": False,
                # Topic henüz yoksa abonelik sırasında broker'a oluşturtur;
                # aksi halde ilk mesajın fark edilmesi metadata yenilemesine (~5 dk) kalabilir
                "allow.auto.create.topics": True,
            })
            consumer.list_topics(timeout=5)
            consumer.subscribe([TOPIC])
            print(
                f"[DB] Kafka'ya bağlanıldı ({KAFKA_BOOTSTRAP}), "
                f"topic='{TOPIC}', group='{GROUP_ID}'",
                flush=True,
            )
            return consumer
        except KafkaException as e:
            bekle = min(2 * deneme, 15)
            print(f"[DB] Kafka hazır değil (deneme {deneme}): {e} -> {bekle}s sonra tekrar", flush=True)
            time.sleep(bekle)
    return None


def kullanici_kaydet(conn, olay: dict):
    # Idempotency: aynı id tekrar gelirse (at-least-once) sessizce yok sayılır
    cur = conn.execute(
        "INSERT OR IGNORE INTO users (id, ad, email) VALUES (?, ?, ?)",
        (olay["id"], olay["ad"], olay["email"]),
    )
    conn.commit()
    if cur.rowcount == 1:
        print(f"[DB] Kaydedildi -> id={olay['id']} ad={olay['ad']} email={olay['email']}", flush=True)
    else:
        print(f"[DB] Mükerrer mesaj, atlandı -> id={olay['id']}", flush=True)


def main():
    conn = veritabani_hazirla()
    consumer = consumer_olustur()
    if consumer is None:
        conn.close()
        return

    try:
        while calisiyor:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() in (
                    KafkaError._PARTITION_EOF,
                    KafkaError.UNKNOWN_TOPIC_OR_PART,
                ):
                    continue
                print(f"[DB] Kafka hatası: {msg.error()}", flush=True)
                continue

            try:
                olay = json.loads(msg.value().decode("utf-8"))
                kullanici_kaydet(conn, olay)
            except (json.JSONDecodeError, UnicodeDecodeError, KeyError) as e:
                # Bozuk mesaj: kaydetme ama commit et ki sonsuz döngüye girmesin
                print(f"[DB] Geçersiz mesaj atlandı: {e} | ham={msg.value()!r}", flush=True)
            except sqlite3.Error as e:
                # DB hatası: commit ETME -> yeniden başlatmada mesaj tekrar işlenir
                print(f"[DB] SQLite hatası, mesaj commit edilmedi: {e}", flush=True)
                continue

            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
        conn.close()
        print("[DB] Consumer ve veritabanı kapatıldı.", flush=True)


if __name__ == "__main__":
    main()

