"""Cliente unary: crea usuarios y muestra la respuesta del servidor."""
import argparse

import grpc

import usuarios_pb2
import usuarios_pb2_grpc


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("nombre", nargs="?", default="Ana")
    parser.add_argument("correo", nargs="?", default="ana@correo.com")
    args = parser.parse_args()

    with grpc.insecure_channel("localhost:50051") as channel:
        stub = usuarios_pb2_grpc.UsuarioServiceStub(channel)
        usuario = stub.CrearUsuario(
            usuarios_pb2.UsuarioRequest(nombre=args.nombre, correo=args.correo),
            timeout=15,
        )
        print(
            f"Creado -> id={usuario.id} nombre={usuario.nombre} "
            f"correo={usuario.correo}"
        )


if __name__ == "__main__":
    main()
