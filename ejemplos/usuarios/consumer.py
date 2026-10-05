"""Consume eventos de usuarios y muestra la asignación de particiones."""
import argparse
from kafka import KafkaConsumer
from json_codec import JsonDeserializer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--group", default="grupo-usuarios")
    parser.add_argument("--client-id", default=None)
    args = parser.parse_args()

    consumer = KafkaConsumer(
        "usuarios-eventos",
        bootstrap_servers="127.0.0.1:9092",
        group_id=args.group,
        client_id=args.client_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=JsonDeserializer(),
    )
    print(f"Consumiendo usuarios-eventos en el grupo {args.group}; Ctrl+C para salir.")
    asignacion_anterior = set()
    try:
        while True:
            registros = consumer.poll(timeout_ms=1000)
            asignacion = consumer.assignment()
            if asignacion != asignacion_anterior:
                detalle = sorted(f"{p.topic}[{p.partition}]" for p in asignacion)
                print(f"Particiones asignadas a este consumer: {detalle}")
                asignacion_anterior = asignacion
            for mensajes in registros.values():
                for mensaje in mensajes:
                    print(
                        f"[CONSUMER] particion={mensaje.partition} "
                        f"offset={mensaje.offset} evento={mensaje.value}"
                    )
    except KeyboardInterrupt:
        print("Consumer detenido.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
