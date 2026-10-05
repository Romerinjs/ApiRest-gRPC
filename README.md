# Actividad de Sistemas Distribuidos: gRPC y Kafka

El repositorio contiene dos ejemplos Python completos y aislados. Ambos cumplen el ejercicio práctico de gRPC y Kafka; `usuarios` aprovecha el ejercicio de clase y `pedidos` sirve como segundo dominio para comparar y explicar cómo se reutilizan los mismos patrones.

## Estructura

```text
.
├── actividad.txt
├── apuntes_clase.md
├── arquitectura.md
├── rest_api/                 # API REST de referencia del profesor; no modificada
└── ejemplos/
    ├── usuarios/             # gRPC :50051, topic usuarios-eventos
    └── pedidos/              # gRPC :50052, topic pedidos-eventos
```

Cada ejemplo contiene su `.proto`, servidor, clientes unary/streaming, producer, consumer, creación de topic, dependencias, Compose y guía local.

## Requisitos generales

- Python 3.10 o superior.
- Docker Desktop y Docker Compose.
- Puertos locales disponibles: Kafka `9092`, gRPC usuarios `50051`, gRPC pedidos `50052`.

Los dos Docker Compose son independientes pero ambos exponen Kafka en `localhost:9092`; ejecuta **solo un ejemplo a la vez**. Cada broker es un único nodo Kafka en KRaft, sin ZooKeeper, y tiene límites de 768 MB de memoria y 1 CPU. El límite de heap Java se fija en 512 MB. Detén el ejemplo antes de cambiar al otro.

## Ejecutar el ejemplo de usuarios

Sigue la guía completa en [`ejemplos/usuarios/README.md`](ejemplos/usuarios/README.md). Resumen desde PowerShell:

```powershell
cd ejemplos/usuarios
python -m venv .venv
# Activa .venv en tu terminal y continúa:
python -m pip install -r requirements.txt
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. usuarios.proto
docker compose up -d --wait
python setup_topic.py
```

En terminales adicionales (desde `ejemplos/usuarios` y con el entorno activado):

```powershell
python server.py
python client_subscribe.py
python client_create.py Ana ana@correo.com
```

Para observar Kafka con dos consumidores, usa dos terminales con el mismo grupo y después ejecuta el producer:

```powershell
python consumer.py --group demo-usuarios --client-id consumidor-1
python consumer.py --group demo-usuarios --client-id consumidor-2
python producer.py --cantidad 8
```

Al terminar, desde la carpeta del ejemplo: `docker compose down`.

## Ejecutar el ejemplo extra de pedidos

Sigue [`ejemplos/pedidos/README.md`](ejemplos/pedidos/README.md). El procedimiento es análogo, ejecutado desde `ejemplos/pedidos`; genera los stubs desde `pedidos.proto`, levanta su broker, crea `pedidos-eventos` y arranca `server.py`. Los clientes `client_create.py` y `client_subscribe.py` utilizan el puerto `50052`.

```powershell
python client_create.py Ana Teclado 2
```

El ejemplo incluye también producer y dos consumers de grupo para demostrar el reparto de particiones. Primero detén el broker de usuarios antes de levantar el broker de pedidos, y viceversa.

## Qué hace cada interacción

- **gRPC unary:** el cliente solicita crear un usuario o pedido y recibe una respuesta única. El servidor valida los campos y publica el evento resultante a Kafka.
- **gRPC server-streaming:** los clientes conectados reciben en vivo nuevas creaciones hechas durante la ejecución del servidor.
- **Kafka:** `usuarios-eventos` y `pedidos-eventos` tienen dos particiones. Los consumers procesan eventos del topic; dos consumers con el mismo grupo se reparten las particiones.
- **REST de referencia:** `rest_api/app.py` conserva el ejemplo Flask visto en clase. Los dos nuevos ejemplos son independientes de esa API y no requieren base de datos.

Los datos de usuarios/pedidos y los streams gRPC son en memoria. Kafka guarda los eventos temporalmente en el contenedor; al ejecutar `docker compose down`, el contenedor y esos datos se eliminan.

## Material para entregar

- Código fuente y `.proto` de los dos ejemplos.
- Logs o capturas de gRPC y de Kafka (incluyendo el reparto entre consumers): guardarlos en `evidencias/`.
- Diagrama y justificación en [`arquitectura.md`](arquitectura.md).
- Fragmentos originales de clase ordenados en [`apuntes_clase.md`](apuntes_clase.md).

No se incluyen capturas inventadas: guarda allí la evidencia de las ejecuciones reales.
