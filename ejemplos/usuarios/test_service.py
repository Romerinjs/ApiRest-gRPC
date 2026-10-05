"""Pruebas unitarias del servicio gRPC sin requerir un broker Kafka activo."""
import unittest
from unittest.mock import patch

import usuarios_pb2
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


class UsuarioServiceTests(unittest.TestCase):
    def setUp(self):
        self.producer = FakeProducer()
        with patch.object(server, "KafkaProducer", return_value=self.producer):
            self.service = server.UsuarioService()
        self.context = FakeContext()

    def test_crear_usuario_publica_evento_y_devuelve_respuesta(self):
        response = self.service.CrearUsuario(
            usuarios_pb2.UsuarioRequest(nombre=" Ana ", correo="ana@example.com"),
            self.context,
        )

        self.assertEqual((response.id, response.nombre), (1, "Ana"))
        args, kwargs = self.producer.calls[0]
        self.assertEqual(args[0], "usuarios-eventos")
        self.assertEqual(kwargs["partition"], 1)
        self.assertEqual(kwargs["value"]["tipo"], "usuario_creado")

    def test_rechaza_campos_vacios_sin_publicar(self):
        with self.assertRaisesRegex(RuntimeError, "INVALID_ARGUMENT"):
            self.service.CrearUsuario(
                usuarios_pb2.UsuarioRequest(nombre=" ", correo="a@b.com"),
                self.context,
            )
        self.assertEqual(self.producer.calls, [])


if __name__ == "__main__":
    unittest.main()
