"""Publica eventos de prueba para demostrar el flujo Kafka."""
import argparse
from kafka import KafkaProducer
from json_codec import JsonSerializer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cantidad", type=int, default=6)
    args = parser.parse_args()
    if args.cantidad < 1:
        parser.error("--cantidad debe ser mayor que cero")

    producer = KafkaProducer(
        bootstrap_servers="127.0.0.1:9092",
        value_serializer=JsonSerializer(),
    )
    try:
        for numero in range(1, args.cantidad + 1):
            evento = {
                "tipo": "usuario_creado",
                "usuario": {
                    "id": numero,
                    "nombre": f"Usuario Demo {numero}",
                    "correo": f"usuario{numero}@ejemplo.com",
                },
            }
            particion = numero % 2
            metadata = producer.send(
                "usuarios-eventos",
                key=str(numero).encode("utf-8"),
                value=evento,
                partition=particion,
            ).get(timeout=10)
            print(
                f"Publicado id={numero} en partición={metadata.partition} "
                f"offset={metadata.offset}"
            )
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
