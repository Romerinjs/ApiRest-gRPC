# Ejemplo ejecutable: usuarios

Este ejemplo combina gRPC y Kafka. `CrearUsuario` es unary y guarda el usuario en memoria; al crearlo, el servidor publica `usuario_creado` en Kafka. `SuscribirUsuarios` es server-streaming y envía en vivo los usuarios creados a los clientes conectados.

## Requisitos

- Python 3.10 o superior.
- Docker Desktop con Docker Compose.
- Puertos locales `50051` (gRPC) y `9092` (Kafka) disponibles.

## Preparación

Ejecuta los comandos desde esta carpeta (`ejemplos/usuarios`). En Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. usuarios.proto
```

Si PowerShell bloquea la activación del entorno, no es necesario activarlo: reemplaza `python` por `.venv\Scripts\python.exe` en los comandos de Python.

## Kafka

Iniciar el único broker del ejemplo:

```powershell
docker compose up -d --wait
```

Crear el topic con dos particiones:

```powershell
python setup_topic.py
```

Kafka usa KRaft (no requiere ZooKeeper), publica solo en `localhost`, tiene límites de memoria y CPU, y guarda datos de forma temporal dentro del contenedor. `docker compose down` elimina el contenedor y los datos. Para detenerlo y liberar recursos:

```powershell
docker compose down
```

## Prueba gRPC + Kafka

Abre terminales separadas, todas en esta carpeta y con el entorno virtual activo:

1. Inicia el servidor gRPC (necesita Kafka activo y el topic creado):

   ```powershell
   python server.py
   ```

2. Para ver el stream en tiempo real, inicia:

   ```powershell
   python client_subscribe.py
   ```

3. Crea usuarios por el método unary:

   ```powershell
   python client_create.py Ana ana@correo.com
   ```

El cliente recibe el usuario por gRPC y el servidor publica el evento correspondiente en Kafka.

## Producer y dos consumers del mismo grupo

Esta prueba independiente permite observar el reparto de las dos particiones. Inicia dos terminales consumidoras:

```powershell
python consumer.py --group demo-usuarios --client-id consumidor-1
```

```powershell
python consumer.py --group demo-usuarios --client-id consumidor-2
```

Espera a que ambos indiquen sus particiones asignadas y luego publica eventos desde otra terminal:

```powershell
python producer.py --cantidad 8
```

Los eventos de prueba se envían alternadamente a las particiones 1 y 0 para que el reparto sea visible. Los dos consumers deben usar exactamente el mismo `--group`.

## Pruebas unitarias

Con el entorno virtual activo:

```powershell
python -m unittest -v
```

## Evidencia sugerida

Captura los logs del servidor y clientes gRPC, la publicación del producer con los números de partición y las asignaciones/mensajes de ambos consumers. El estado de usuarios es en memoria: al reiniciar el servidor, se reinicia el contador.
