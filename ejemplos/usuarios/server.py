"""Servicio gRPC de usuarios que emite eventos a Kafka."""
import queue
import threading
from concurrent import futures

import grpc
from kafka import KafkaProducer
from kafka.errors import KafkaError

import usuarios_pb2
import usuarios_pb2_grpc
from json_codec import JsonSerializer


KAFKA_BOOTSTRAP = "127.0.0.1:9092"
KAFKA_TOPIC = "usuarios-eventos"


class UsuarioService(usuarios_pb2_grpc.UsuarioServiceServicer):
    def __init__(self):
        self.lock = threading.Lock()
        self.siguiente_id = 1
        self.suscriptores = []
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=JsonSerializer(),
        )

    def CrearUsuario(self, request, context):
        if not request.nombre.strip() or not request.correo.strip():
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Se requieren nombre y correo",
            )

        with self.lock:
            respuesta = usuarios_pb2.UsuarioResponse(
                id=self.siguiente_id,
                nombre=request.nombre.strip(),
                correo=request.correo.strip(),
            )
            evento = {
                "tipo": "usuario_creado",
                "usuario": {
                    "id": respuesta.id,
                    "nombre": respuesta.nombre,
                    "correo": respuesta.correo,
                },
            }
            try:
                self.producer.send(
                    KAFKA_TOPIC,
                    key=str(respuesta.id).encode("utf-8"),
                    value=evento,
                    partition=respuesta.id % 2,
                ).get(timeout=10)
            except KafkaError as error:
                context.abort(
                    grpc.StatusCode.UNAVAILABLE,
                    f"No fue posible publicar el evento en Kafka: {error}",
                )

            self.siguiente_id += 1
            for suscriptor in self.suscriptores:
                suscriptor.put(respuesta)
        return respuesta

    def SuscribirUsuarios(self, request, context):
        suscriptor = queue.Queue()
        with self.lock:
            self.suscriptores.append(suscriptor)
        try:
            while context.is_active():
                try:
                    yield suscriptor.get(timeout=1)
                except queue.Empty:
                    continue
        finally:
            with self.lock:
                if suscriptor in self.suscriptores:
                    self.suscriptores.remove(suscriptor)


def serve():
    service = UsuarioService()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    usuarios_pb2_grpc.add_UsuarioServiceServicer_to_server(service, server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("Servidor gRPC de usuarios escuchando en localhost:50051")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=2)
    finally:
        service.producer.close()


if __name__ == "__main__":
    serve()
