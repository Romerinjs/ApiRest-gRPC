"""Servicio gRPC de pedidos que emite eventos a Kafka."""
import queue
import threading
from concurrent import futures

import grpc
from kafka import KafkaProducer
from kafka.errors import KafkaError

import pedidos_pb2
import pedidos_pb2_grpc
from json_codec import JsonSerializer


KAFKA_BOOTSTRAP = "127.0.0.1:9092"
KAFKA_TOPIC = "pedidos-eventos"


class PedidoService(pedidos_pb2_grpc.PedidoServiceServicer):
    def __init__(self):
        self.lock = threading.Lock()
        self.siguiente_id = 1
        self.suscriptores = []
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=JsonSerializer(),
        )

    def CrearPedido(self, request, context):
        if not request.cliente.strip() or not request.producto.strip():
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Se requieren cliente y producto",
            )
        if request.cantidad <= 0:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "La cantidad debe ser mayor que cero",
            )

        with self.lock:
            respuesta = pedidos_pb2.PedidoResponse(
                id=self.siguiente_id,
                cliente=request.cliente.strip(),
                producto=request.producto.strip(),
                cantidad=request.cantidad,
                estado="CREADO",
            )
            evento = {
                "tipo": "pedido_creado",
                "pedido": {
                    "id": respuesta.id,
                    "cliente": respuesta.cliente,
                    "producto": respuesta.producto,
                    "cantidad": respuesta.cantidad,
                    "estado": respuesta.estado,
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

    def SuscribirPedidos(self, request, context):
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
    service = PedidoService()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pedidos_pb2_grpc.add_PedidoServiceServicer_to_server(service, server)
    server.add_insecure_port("[::]:50052")
    server.start()
    print("Servidor gRPC de pedidos escuchando en localhost:50052")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=2)
    finally:
        service.producer.close()


if __name__ == "__main__":
    serve()
