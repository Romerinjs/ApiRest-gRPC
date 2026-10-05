"""Pruebas unitarias del servicio gRPC sin requerir un broker Kafka activo."""
import unittest
from unittest.mock import patch

import pedidos_pb2
import server


class FakeContext:
    def abort(self, status, description):
        raise RuntimeError(f"{status.name}: {description}")


class FakeFuture:
    def get(self, timeout=None):
        return None


class FakeProducer:
    def __init__(self):
        self.calls = []

    def send(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return FakeFuture()


class PedidoServiceTests(unittest.TestCase):
    def setUp(self):
        self.producer = FakeProducer()
        with patch.object(server, "KafkaProducer", return_value=self.producer):
            self.service = server.PedidoService()
        self.context = FakeContext()

    def test_crear_pedido_publica_evento_y_devuelve_respuesta(self):
        response = self.service.CrearPedido(
            pedidos_pb2.PedidoRequest(
                cliente=" Ana ", producto=" Teclado ", cantidad=2
            ),
            self.context,
        )

        self.assertEqual(
            (response.id, response.cliente, response.producto, response.estado),
            (1, "Ana", "Teclado", "CREADO"),
        )
        args, kwargs = self.producer.calls[0]
        self.assertEqual(args[0], "pedidos-eventos")
        self.assertEqual(kwargs["partition"], 1)
        self.assertEqual(kwargs["value"]["tipo"], "pedido_creado")

    def test_rechaza_cantidad_no_positiva_sin_publicar(self):
        with self.assertRaisesRegex(RuntimeError, "INVALID_ARGUMENT"):
            self.service.CrearPedido(
                pedidos_pb2.PedidoRequest(
                    cliente="Ana", producto="Teclado", cantidad=0
                ),
                self.context,
            )
        self.assertEqual(self.producer.calls, [])


if __name__ == "__main__":
    unittest.main()
