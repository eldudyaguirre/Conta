from fastapi import APIRouter, HTTPException
from app.database.client_connection import probar_conexion_cliente
from app.services.cliente_service import ClienteService
from app.services.base_cliente_service import BaseClienteService

router = APIRouter(
    prefix="/clientes",
    tags=["Clientes"],
)


@router.get("")
def listar_clientes():
    return ClienteService.listar_clientes()


@router.get("/{ruc}")
def obtener_cliente(ruc: str):

    cliente = ClienteService.obtener_cliente(ruc)

    if not cliente:
        raise HTTPException(
            status_code=404,
            detail="Cliente no encontrado"
        )

    return cliente

@router.get("/{ruc}/base")
def probar_base_cliente(ruc: str):

    cliente = ClienteService.obtener_cliente(ruc)

    if not cliente:
        raise HTTPException(
            status_code=404,
            detail="Cliente no encontrado en BdTotal"
        )

    try:

        conexion = probar_conexion_cliente(ruc)

        return {
            "ruc": ruc,
            "nombre": cliente["nombre"],
            "base_datos": conexion["base_datos"],
            "conexion": conexion["conexion"],
        }

    except Exception as e:

        raise HTTPException(
            status_code=500,
            detail=f"No se pudo conectar a la base del cliente: {str(e)}"
        )

@router.get("/{ruc}/tablas")
def listar_tablas_cliente(ruc: str):

    cliente = ClienteService.obtener_cliente(ruc)

    if not cliente:
        raise HTTPException(
            status_code=404,
            detail="Cliente no encontrado en BdTotal"
        )

    try:

        tablas = BaseClienteService.listar_tablas(ruc)

        return {
            "ruc": ruc,
            "base_datos": ruc,
            "total_tablas": len(tablas),
            "tablas": tablas,
        }

    except Exception as e:

        raise HTTPException(
            status_code=500,
            detail=f"No se pudieron consultar las tablas: {str(e)}"
        )

@router.get("/{ruc}/tablas/{tabla}")
def estructura_tabla(ruc: str, tabla: str):

    cliente = ClienteService.obtener_cliente(ruc)

    if not cliente:
        raise HTTPException(
            status_code=404,
            detail="Cliente no encontrado en BdTotal"
        )

    try:

        columnas = BaseClienteService.estructura_tabla(
            ruc,
            tabla
        )

        if not columnas:
            raise HTTPException(
                status_code=404,
                detail=f"La tabla '{tabla}' no existe"
            )

        return {
            "ruc": ruc,
            "base_datos": ruc,
            "tabla": tabla,
            "total_columnas": len(columnas),
            "columnas": columnas,
        }

    except HTTPException:
        raise

    except Exception as e:

        raise HTTPException(
            status_code=500,
            detail=str(e)
        )