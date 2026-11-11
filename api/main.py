import json
import os
from uuid import uuid4

from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, EmailStr

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "kullanici.olusturuldu"

app = FastAPI(title="Kullanıcı Kayıt API")

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "client.id": "kullanici-api",
    "acks": "all",
})


class KullaniciOlustur(BaseModel):
    ad: str
    email: EmailStr


def teslimat_raporu(err, msg):
    """Kafka'ya teslimat sonucunu loglar."""
    if err is not None:
        print(f"[API] Mesaj teslim edilemedi: {err}", flush=True)
    else:
        print(
            f"[API] Mesaj teslim edildi -> topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}",
            flush=True,
        )


@app.get("/health")
def health():
    return {"status": "ok"}


# confluent_kafka senkron (blocking) olduğu için endpoint 'def' olarak tanımlandı;
# FastAPI bu fonksiyonu threadpool'da çalıştırır ve event loop bloklanmaz.
@app.post("/users", status_code=status.HTTP_201_CREATED)
def kullanici_olustur(veri: KullaniciOlustur):
    kullanici_id = str(uuid4())
    olay = {
        "id": kullanici_id,
        "ad": veri.ad,
        "email": veri.email,
        "event": "KULLANICI_OLUSTURULDU",
    }

    try:
        producer.produce(
            TOPIC,
            key=kullanici_id,
            value=json.dumps(olay, ensure_ascii=False).encode("utf-8"),
            callback=teslimat_raporu,
        )
        kalan = producer.flush(10)
    except Exception as e:
        print(f"[API] Kafka produce hatası: {e}", flush=True)
        raise HTTPException(status_code=503, detail="Kafka'ya mesaj gönderilemedi")

    if kalan > 0:
        print(f"[API] flush zaman aşımı, {kalan} mesaj gönderilemedi", flush=True)
        raise HTTPException(status_code=503, detail="Kafka şu anda erişilemez")

    print(f"[API] Olay yayınlandı: {olay}", flush=True)
    return olay

