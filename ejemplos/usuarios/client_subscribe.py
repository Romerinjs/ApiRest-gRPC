"""Cliente server-streaming: recibe usuarios mientras el proceso está activo."""
import grpc

import usuarios_pb2
import usuarios_pb2_grpc


def main():
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = usuarios_pb2_grpc.UsuarioServiceStub(channel)
        print("Suscrito a usuarios; presiona Ctrl+C para salir.")
        try:
            for usuario in stub.SuscribirUsuarios(
                usuarios_pb2.SuscripcionRequest()
            ):
                print(
                    f"[STREAM] id={usuario.id} nombre={usuario.nombre} "
                    f"correo={usuario.correo}"
                )
        except KeyboardInterrupt:
            print("Suscripción finalizada.")


if __name__ == "__main__":
    main()
