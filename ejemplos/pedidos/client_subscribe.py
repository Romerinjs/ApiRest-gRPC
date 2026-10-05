"""Cliente server-streaming para recibir pedidos nuevos."""
import grpc

import pedidos_pb2
import pedidos_pb2_grpc


def main():
    with grpc.insecure_channel("localhost:50052") as channel:
        stub = pedidos_pb2_grpc.PedidoServiceStub(channel)
        print("Suscrito a pedidos; presiona Ctrl+C para salir.")
        try:
            for pedido in stub.SuscribirPedidos(
                pedidos_pb2.SuscripcionRequest()
            ):
                print(
                    f"[STREAM] id={pedido.id} cliente={pedido.cliente} "
                    f"producto={pedido.producto} cantidad={pedido.cantidad} "
                    f"estado={pedido.estado}"
                )
        except KeyboardInterrupt:
            print("Suscripción finalizada.")


if __name__ == "__main__":
    main()
