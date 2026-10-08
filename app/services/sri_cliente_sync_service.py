        # de cada detalle. Esa estructura es la fuente principal.
        impuestos_clasificados = Decimal("0")
        # No basta con revisar impuestos_clasificados para decidir si usamos
        # totalConImpuestos: un IVA 0% es un impuesto válido, pero su base no
        # incrementa impuestos_clasificados. Si usamos ese contador, la base
        # del detalle se suma otra vez desde totalConImpuestos.
        impuestos_detalle_encontrados = False

        for impuesto in doc.findall(".//detalle/impuestos/impuesto"):
            codigo = cls._txt(impuesto, "codigo")
            if codigo != "2":
                continue

            impuestos_detalle_encontrados = True

            codigo_pct = cls._txt(impuesto, "codigoPorcentaje")
            tarifa = cls._dec(cls._txt(impuesto, "tarifa"))
            base = cls._dec(cls._txt(impuesto, "baseImponible"))
            valor = cls._dec(cls._txt(impuesto, "valor"))

            _iva_debug_log(
                "XML DETALLE | clave=%s | codigo=%s | codigoPorcentaje=%s | tarifa=%s | baseImponible=%s | valor=%s",
                cls._txt(it, "claveAcceso"), codigo, codigo_pct, tarifa, base, valor,
            )

            tarifa_key = format(tarifa, "f").rstrip("0").rstrip(".")
            if tarifa_key in {"5", "8", "12", "14", "15"}:
                tasa = tarifa_key
            else:
                tasa = codigo_a_tasa.get(codigo_pct)

            _iva_debug_log(
                "XML CLASIFICACION | clave=%s | codigoPorcentaje=%s | tarifa=%s | tasa_resultante=%s | base=%s | valor=%s",
                cls._txt(it, "claveAcceso"), codigo_pct, tarifa, tasa, base, valor,
            )

            if tasa in {"5", "8", "12", "14", "15"}:
                bases[tasa] += base
                ivas[tasa] += valor
                impuestos_clasificados += base
            elif tasa == "0":
                bases["0"] += base
            elif codigo_pct == "6":
                bases["no_objeto"] += base
            elif codigo_pct == "7":
                bases["exenta"] += base
            else:
                bases["no_objeto"] += base

        # Respaldo: si el XML no trae impuestos dentro de los detalles,
        # usamos totalConImpuestos. En los XML normales de compras no se
        # llega aquí, pero permite procesar comprobantes con estructura
        # incompleta.
        if not impuestos_detalle_encontrados:
            totals = inf.find("totalConImpuestos")
            if totals is not None:
                for ti in totals.findall("totalImpuesto"):
                    codigo = cls._txt(ti, "codigo")
                    if codigo == "3":
                        ice += cls._dec(cls._txt(ti, "valor"))
                        continue
                    if codigo != "2":
                        continue

                    codigo_pct = cls._txt(ti, "codigoPorcentaje")
                    tarifa = cls._dec(cls._txt(ti, "tarifa"))
                    base = cls._dec(cls._txt(ti, "baseImponible"))
                    valor = cls._dec(cls._txt(ti, "valor"))

                    tarifa_key = format(tarifa, "f").rstrip("0").rstrip(".")
                    tasa = (
                        tarifa_key
                        if tarifa_key in {"0", "5", "8", "12", "14", "15"}