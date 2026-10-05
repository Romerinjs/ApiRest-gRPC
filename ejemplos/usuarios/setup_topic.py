"""Crea el topic de usuarios con dos particiones (es seguro repetirlo)."""
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


def main():
    admin = KafkaAdminClient(
        bootstrap_servers="127.0.0.1:9092",
        client_id="usuarios-topic-setup",
        request_timeout_ms=10000,
    )
    try:
        try:
            admin.create_topics(
                [NewTopic(name="usuarios-eventos", num_partitions=2, replication_factor=1)]
            )
            print("Topic usuarios-eventos creado con 2 particiones.")
        except TopicAlreadyExistsError:
            print("El topic usuarios-eventos ya existe.")
    finally:
        admin.close()


if __name__ == "__main__":
    main()
