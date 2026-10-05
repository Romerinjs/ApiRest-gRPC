# Ejemplo ejecutable: pedidos

Este es el segundo dominio funcional y autocontenido. `CrearPedido` usa gRPC unary, `SuscribirPedidos` es server-streaming y cada pedido aceptado produce un evento `pedido_creado` en Kafka. El consumer simula la recepción por servicios de inventario o notificaciones.

## Requisitos

- Python 3.10 o superior.
- Docker Desktop con Docker Compose.
- Puertos locales `50052` (gRPC) y `9092` (Kafka) disponibles.

## Preparación

Ejecutar todos los comandos desde esta carpeta (`ejemplos/pedidos`). En Windows PowerShell activa el entorno con `.venv\Scripts\Activate.ps1`.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. pedidos.proto
```

Si PowerShell bloquea la activación del entorno, no es necesario activarlo: reemplaza `python` por `.venv\Scripts\python.exe` en los comandos de Python.

## Kafka

Inicia este broker independiente (no levantes a la vez el de `ejemplos/usuarios`):

```powershell
docker compose up -d --wait
python setup_topic.py
```

Topic: `pedidos-eventos`, con dos particiones. El broker usa KRaft, sin ZooKeeper, límites locales de CPU/memoria y almacenamiento temporal dentro del contenedor. `docker compose down` elimina los datos y libera los recursos:

```powershell
docker compose down
```

## Prueba gRPC + Kafka

Inicia el servidor:

```powershell
python server.py
```

En otra terminal, escucha el stream:

```powershell
python client_subscribe.py
```

En una tercera terminal, crea un pedido (cliente, producto y cantidad):

```powershell
python client_create.py Ana Teclado 2
```

El pedido validado se devuelve al cliente gRPC, se envía al stream activo y se publica como evento en Kafka.

## Producer y dos consumers del mismo grupo

Abre dos terminales consumidoras con el mismo `--group`:

```powershell
python consumer.py --group demo-pedidos --client-id consumidor-1
```

```powershell
python consumer.py --group demo-pedidos --client-id consumidor-2
```

Cuando ambos muestren su asignación de particiones, publica eventos de prueba:

```powershell
python producer.py --cantidad 8
```

El producer alterna explícitamente entre las dos particiones para facilitar la observación del reparto.

## Pruebas unitarias

Con el entorno virtual activo:

```powershell
python -m unittest -v
```

## Evidencias sugeridas

Guarda capturas o logs del método unary, del stream, de eventos Kafka publicados y procesados, y de las particiones asignadas a los dos consumers. Los pedidos se mantienen en memoria mientras el servidor está activo.
