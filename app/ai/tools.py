from app.services.tributario_service import TributarioService


class ContaTools:

    @staticmethod
    def resumen_compras(ruc: str, anio: int, mes: int):
        return TributarioService.resumen_compras(
            ruc=ruc,
            anio=anio,
            mes=mes,
        )

    @staticmethod
    def listar_compras(
        ruc: str,
        anio: int,
        mes: int,
        tipcom: str | None = None,
    ):
        return TributarioService.listar_compras(
            ruc=ruc,
            anio=anio,
            mes=mes,
            tipcom=tipcom,
        )