"""Publica eventos de pedidos de prueba."""
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
                "tipo": "pedido_creado",
                "pedido": {
                    "id": numero,
                    "cliente": f"Cliente Demo {numero}",
                    "producto": f"Producto {numero}",
                    "cantidad": numero % 3 + 1,
                    "estado": "CREADO",
                },
            }
            particion = numero % 2
            metadata = producer.send(
                "pedidos-eventos",
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
