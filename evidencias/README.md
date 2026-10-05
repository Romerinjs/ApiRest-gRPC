# Evidencias de ejecución

Estos son logs reales de las pruebas locales de gRPC y Kafka. Se pueden usar como evidencia o complementar con capturas de pantalla:

- `usuarios_server.log` y `usuarios_stream.log`: servidor y cliente server-streaming; la respuesta unary aparece en el stream.
- `usuarios_consumer1.log` y `usuarios_consumer2.log`: dos consumers del grupo `demo-usuarios`, asignados a particiones distintas.
- `pedidos_server.log` y `pedidos_stream.log`: servidor y stream del dominio pedidos.
- `pedidos_consumer1.log` y `pedidos_consumer2.log`: dos consumers del grupo `demo-pedidos`, asignados a particiones distintas.

La terminal de pruebas confirmó además que cada topic tiene dos particiones y el cliente unary recibió la respuesta. Los producers enviaron ocho eventos a particiones alternas en cada dominio.
