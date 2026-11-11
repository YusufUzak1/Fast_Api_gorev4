import json
import os
import signal
import time
from datetime import datetime, timedelta, timezone

from confluent_kafka import Consumer, KafkaError, KafkaException

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "kullanici.olusturuldu"
GROUP_ID = "log-consumer-group"  # db-consumer'dan FARKLI -> fan-out
LOG_FILE = os.getenv("LOG_FILE", "/app/users.log")
# Türkiye saati (UTC+3); tzdata'ya gerek duymadan standart kütüphaneyle
TR_SAATI = timezone(timedelta(hours=3))

calisiyor = True


def durdur(signum, frame):
    global calisiyor
    print(f"[LOG] Sinyal alındı ({signum}), kapatılıyor...", flush=True)
    calisiyor = False


signal.signal(signal.SIGTERM, durdur)
signal.signal(signal.SIGINT, durdur)


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
                "enable.auto.commit": True,
                # Topic henüz yoksa abonelik sırasında broker'a oluşturtur;
                # aksi halde ilk mesajın fark edilmesi metadata yenilemesine (~5 dk) kalabilir
                "allow.auto.create.topics": True,
            })
            # Broker'a gerçekten ulaşılabildiğini doğrula
            consumer.list_topics(timeout=5)
            consumer.subscribe([TOPIC])
            print(
                f"[LOG] Kafka'ya bağlanıldı ({KAFKA_BOOTSTRAP}), "
                f"topic='{TOPIC}', group='{GROUP_ID}'",
                flush=True,
            )
            return consumer
        except KafkaException as e:
            bekle = min(2 * deneme, 15)
            print(f"[LOG] Kafka hazır değil (deneme {deneme}): {e} -> {bekle}s sonra tekrar", flush=True)
            time.sleep(bekle)
    return None


def log_yaz(olay: dict):
    zaman = datetime.now(TR_SAATI).strftime("%Y-%m-%d %H:%M:%S")
    satir = f"[{zaman}] {json.dumps(olay, ensure_ascii=False)}\n"
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(satir)
    print(
        f"[LOG] {zaman} | Yeni kullanıcı -> id={olay.get('id')} "
        f"ad={olay.get('ad')} email={olay.get('email')} event={olay.get('event')}",
        flush=True,
    )


def main():
    consumer = consumer_olustur()
    if consumer is None:
        return

    try:
        while calisiyor:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                # Topic henüz oluşmadıysa (auto-create öncesi) bekle, çökme
                if msg.error().code() in (
                    KafkaError._PARTITION_EOF,
                    KafkaError.UNKNOWN_TOPIC_OR_PART,
                ):
                    continue
                print(f"[LOG] Kafka hatası: {msg.error()}", flush=True)
                continue

            try:
                olay = json.loads(msg.value().decode("utf-8"))
                log_yaz(olay)
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                print(f"[LOG] Geçersiz mesaj atlandı: {e} | ham={msg.value()!r}", flush=True)
    finally:
        consumer.close()
        print("[LOG] Consumer kapatıldı.", flush=True)


if __name__ == "__main__":
    main()

