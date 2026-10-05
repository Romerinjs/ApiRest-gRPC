"""Cliente unary para registrar pedidos."""
import argparse

import grpc

import pedidos_pb2
import pedidos_pb2_grpc


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("cliente", nargs="?", default="Ana")
    parser.add_argument("producto", nargs="?", default="Teclado")
    parser.add_argument("cantidad", nargs="?", type=int, default=1)
    args = parser.parse_args()

    with grpc.insecure_channel("localhost:50052") as channel:
        stub = pedidos_pb2_grpc.PedidoServiceStub(channel)
        pedido = stub.CrearPedido(
            pedidos_pb2.PedidoRequest(
                cliente=args.cliente,
                producto=args.producto,
                cantidad=args.cantidad,
            ),
            timeout=15,
        )
        print(
            f"Pedido creado -> id={pedido.id} cliente={pedido.cliente} "
            f"producto={pedido.producto} cantidad={pedido.cantidad} "
            f"estado={pedido.estado}"
        )


if __name__ == "__main__":
    main()
