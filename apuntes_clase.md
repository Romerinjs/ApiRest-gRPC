# Apuntes de la clase: REST y gRPC

Este archivo conserva los comandos y fragmentos que estaban junto a las dependencias en `requirements.txt`. El archivo de dependencias queda reservado para paquetes instalables con `pip`.

## 1. Probar la API REST de usuarios

Crear un usuario:

```bash
curl -X POST http://localhost:5000/usuarios \
  -H "Content-Type: application/json" \
  -d '{"nombre":"Ana","correo":"ana@correo.com"}'
```

Listar usuarios:

```bash
curl http://localhost:5000/usuarios
```

La API de referencia está implementada en `rest_api/app.py`; mantiene los datos en memoria.

## 2. Esqueleto inicial de Protocol Buffers

Los espacios se completan al definir el servicio, los mensajes y sus campos:

```proto
syntax = "proto3";
package usuarios;

service ______ {
  rpc ______ (______) returns (______);
}

message UsuarioRequest {
  ____ ______ = _;
}

message UsuarioResponse {
  ____ ______ = _;
}
```

Un método server-streaming se declara con `stream` en la respuesta:

```proto
rpc SuscribirUsuarios (SuscripcionRequest) returns (stream UsuarioResponse);
message SuscripcionRequest {}
```

## 3. Generar los stubs de gRPC

Desde la carpeta que contiene el archivo `.proto`:

```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. usuarios.proto
```

Esto genera los módulos `usuarios_pb2.py` y `usuarios_pb2_grpc.py`, que contienen los mensajes y el código cliente/servidor.

## 4. Servidor gRPC de referencia

El ejemplo de la clase mantiene un identificador incremental y una cola por suscriptor. El bloqueo protege el estado compartido porque el servidor atiende llamadas concurrentemente.

```python
import queue
import threading
from concurrent import futures

import grpc
import usuarios_pb2
import usuarios_pb2_grpc


class UsuarioService(usuarios_pb2_grpc.UsuarioServiceServicer):
    def __init__(self):
        self.lock = threading.Lock()
        self.siguiente_id = 1
        self.suscriptores = []  # Una cola por cliente suscrito.

    def CrearUsuario(self, request, context):
        if not request.nombre or not request.correo:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Se requieren nombre y correo",
            )
        with self.lock:
            respuesta = usuarios_pb2.UsuarioResponse(
                id=self.siguiente_id,
                nombre=request.nombre,
                correo=request.correo,
            )
            self.siguiente_id += 1
            for cola in self.suscriptores:
                cola.put(respuesta)
        return respuesta

    def SuscribirUsuarios(self, request, context):
        cola = queue.Queue()
        with self.lock:
            self.suscriptores.append(cola)
        try:
            while context.is_active():
                try:
                    yield cola.get(timeout=1)
                except queue.Empty:
                    continue
        finally:
            with self.lock:
                self.suscriptores.remove(cola)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    usuarios_pb2_grpc.add_UsuarioServiceServicer_to_server(
        UsuarioService(), server
    )
    server.add_insecure_port("[::]:50051")
    server.start()
    print("Servidor gRPC escuchando en el puerto 50051")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
```

## 5. Cliente unary para crear un usuario

El cliente llama a `CrearUsuario` y espera una respuesta única:

```python
import sys

import grpc
import usuarios_pb2
import usuarios_pb2_grpc

nombre = sys.argv[1] if len(sys.argv) > 1 else "Ana"
correo = sys.argv[2] if len(sys.argv) > 2 else "ana@correo.com"

with grpc.insecure_channel("localhost:50051") as canal:
    stub = usuarios_pb2_grpc.UsuarioServiceStub(canal)
    respuesta = stub.CrearUsuario(
        usuarios_pb2.UsuarioRequest(nombre=nombre, correo=correo)
    )
    print(
        f"Creado -> id={respuesta.id} "
        f"nombre={respuesta.nombre} correo={respuesta.correo}"
    )
```

Ejemplo de invocación:

```bash
python client_crear.py Ana ana@correo.com
```

## 6. Cliente server-streaming

El cliente permanece conectado y muestra cada usuario nuevo que el servidor transmite:

```python
import grpc
import usuarios_pb2
import usuarios_pb2_grpc

with grpc.insecure_channel("localhost:50051") as canal:
    stub = usuarios_pb2_grpc.UsuarioServiceStub(canal)
    print("Suscrito. Esperando usuarios nuevos... (Ctrl+C para salir)")
    try:
        for usuario in stub.SuscribirUsuarios(
            usuarios_pb2.SuscripcionRequest()
        ):
            print(
                f"[STREAM] id={usuario.id} nombre={usuario.nombre} "
                f"correo={usuario.correo}"
            )
    except KeyboardInterrupt:
        pass
```

Los ejemplos ejecutables de esta actividad están separados en `ejemplos/usuarios/` y `ejemplos/pedidos/`.
