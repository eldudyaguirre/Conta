from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import subprocess
import time
import urllib.request
import tempfile
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from sqlalchemy import text

from app.core.config import settings
from app.database.client_connection import obtener_session_cliente
from app.database.connection import engine


logger = logging.getLogger("conta.sri_sync")

# Log de diagnóstico específico para rastrear la clasificación de IVA
# desde el XML del SRI hasta la fila final de comprasnue.
IVA_DEBUG_LOG = Path(__file__).resolve().parents[2] / "logs" / "sri_iva_debug.log"


class SriJobCancelado(Exception):
    """Señala que un trabajo SRI fue cancelado por el usuario."""


class SriClienteSyncService:
    JOB_TABLE = "conta_sri_jobs"

    @staticmethod
    def _iva_debug_log(message: str, *args: Any) -> None:
        try:
            IVA_DEBUG_LOG.parent.mkdir(parents=True, exist_ok=True)
            text_message = message % args if args else message
            with IVA_DEBUG_LOG.open("a", encoding="utf-8") as fh:
                fh.write(f"{datetime.now().isoformat(timespec='seconds')} | {text_message}\n")
        except Exception:
            # El diagnóstico nunca debe detener una sincronización SRI.
            pass

    @classmethod
    def _ensure_jobs_table(cls) -> None:
        with engine.begin() as db:
            db.execute(text(f"""CREATE TABLE IF NOT EXISTS {cls.JOB_TABLE} (
                    job_id VARCHAR(64) PRIMARY KEY,
                    estado VARCHAR(20) NOT NULL,
                    ruc VARCHAR(13) NOT NULL,
                    cliente TEXT NOT NULL DEFAULT '',
                    anio INTEGER NOT NULL,
                    mes INTEGER NOT NULL,
                    tipo_comprobante VARCHAR(2) NOT NULL,
                    operacion VARCHAR(30) NOT NULL DEFAULT 'compras',
                    sri INTEGER NOT NULL DEFAULT 0,
                    ya_existentes INTEGER NOT NULL DEFAULT 0,
                    descargadas INTEGER NOT NULL DEFAULT 0,
                    guardadas INTEGER NOT NULL DEFAULT 0,
                    errores JSONB NOT NULL DEFAULT '[]'::jsonb,
                    paginas INTEGER NOT NULL DEFAULT 0,
                    mensaje TEXT NOT NULL DEFAULT '',
                    detalle TEXT,
                    creado TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    actualizado TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )"""))
            db.execute(text(f"ALTER TABLE {cls.JOB_TABLE} ADD COLUMN IF NOT EXISTS operacion VARCHAR(30) NOT NULL DEFAULT 'compras'"))
            db.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{cls.JOB_TABLE}_estado ON {cls.JOB_TABLE}(estado, creado)"))

    @classmethod
    def _job_update(cls, job_id: str | None, **values) -> None:
        if not job_id:
            return
        cls._ensure_jobs_table()
        allowed = {"estado","ruc","cliente","anio","mes","tipo_comprobante","operacion","sri","ya_existentes","descargadas","guardadas","errores","paginas","mensaje","detalle"}
        sets = []
        params = {"job_id": job_id}
        for key, value in values.items():
            if key not in allowed:
                continue
            if key == "errores":
                value = json.dumps(value, ensure_ascii=False)
            sets.append(f"{key} = :{key}")
            params[key] = value
        if not sets:
            return
        sets.append("actualizado = CURRENT_TIMESTAMP")
        with engine.begin() as db:
            db.execute(text(f"UPDATE {cls.JOB_TABLE} SET {', '.join(sets)} WHERE job_id = :job_id"), params)

    @classmethod
    def iniciar_sincronizacion(cls, ruc: str, anio: int, mes: int, tipo_comprobante: int = 1, operacion: str = "compras") -> dict[str, Any]:
        cred = cls._credenciales(ruc)
        cls._ensure_jobs_table()
        with engine.begin() as db:
            existente = db.execute(text(f"""
                SELECT job_id, estado FROM {cls.JOB_TABLE}
                WHERE ruc = :ruc AND anio = :anio AND mes = :mes
                  AND tipo_comprobante = :tipo AND operacion = :operacion
                  AND estado IN ('pendiente', 'ejecutando', 'captcha')
                ORDER BY creado DESC LIMIT 1
            """), {"ruc":ruc,"anio":anio,"mes":mes,"tipo":cls._tipo(tipo_comprobante),"operacion":operacion}).mappings().first()
            if existente:
                return {"job_id":existente["job_id"],"estado":existente["estado"],"duplicado":True}
            job_id = uuid.uuid4().hex
            db.execute(text(f"""
                INSERT INTO {cls.JOB_TABLE}
                (job_id, estado, ruc, cliente, anio, mes, tipo_comprobante, operacion, mensaje)
                VALUES (:job_id, 'pendiente', :ruc, :cliente, :anio, :mes, :tipo, :operacion, :mensaje)
            """), {"job_id":job_id,"ruc":ruc,"cliente":cred["nombre"],"anio":anio,"mes":mes,"tipo":cls._tipo(tipo_comprobante),"operacion":operacion,"mensaje":"Sincronización en cola. Esperando al worker SRI interactivo."})
        return {"job_id":job_id,"estado":"pendiente","duplicado":False}

    @classmethod
    async def _ejecutar_job(cls, job_id: str, ruc: str, anio: int, mes: int, tipo_comprobante: int, operacion: str = "compras") -> None:
        cls._job_update(job_id, estado="ejecutando", mensaje="Worker SRI activo. Iniciando navegador y conexión con el SRI.")
        try:
            resultado = await cls.sincronizar_mes(ruc, anio, mes, tipo_comprobante, job_id=job_id, operacion=operacion)
            cls._job_update(job_id, **resultado, estado="finalizado", mensaje="Sincronización finalizada.")
        except SriJobCancelado as exc:
            cls._job_update(job_id, estado="cancelado", mensaje=str(exc))
        except Exception as exc:
            cls._job_update(job_id, estado="error", mensaje=str(exc), detalle=str(exc))

    @classmethod
    def cancelar_sincronizacion(cls, job_id: str) -> dict[str, Any] | None:
        cls._ensure_jobs_table()
        with engine.begin() as db:
            row = db.execute(text(f"""SELECT job_id, estado, ruc, cliente, anio, mes, operacion FROM {cls.JOB_TABLE} WHERE job_id = :job_id FOR UPDATE"""), {"job_id":job_id}).mappings().first()
            if not row:
                return None
            if row["estado"] in ("finalizado","error","cancelado"):
                return dict(row)
            db.execute(text(f"""UPDATE {cls.JOB_TABLE} SET estado='cancelado', mensaje='Cancelación solicitada por el usuario.', detalle=NULL, actualizado=CURRENT_TIMESTAMP WHERE job_id=:job_id"""), {"job_id":job_id})
            result = dict(row); result["estado"]="cancelado"; result["mensaje"]="Cancelación solicitada por el usuario."
            return result

    @classmethod
    def _verificar_cancelacion(cls, job_id: str | None) -> None:
        if not job_id:
            return
        with engine.connect() as db:
            estado = db.execute(text(f"SELECT estado FROM {cls.JOB_TABLE} WHERE job_id=:job_id"), {"job_id":job_id}).scalar()
        if estado == "cancelado":
            raise SriJobCancelado("Sincronización cancelada por el usuario.")

    @classmethod
    def estado_sincronizacion(cls, job_id: str) -> dict[str, Any] | None:
        cls._ensure_jobs_table()
        with engine.connect() as db:
            row = db.execute(text(f"""SELECT job_id, estado, ruc, cliente, anio, mes, tipo_comprobante, sri, ya_existentes, descargadas, guardadas, errores, paginas, mensaje, detalle, creado, actualizado, operacion FROM {cls.JOB_TABLE} WHERE job_id=:job_id"""), {"job_id":job_id}).mappings().first()
        if not row: return None
        result=dict(row)
        for key in ("creado","actualizado"):
            if result.get(key): result[key]=result[key].isoformat()
        return result

    @classmethod
    def obtener_trabajo_pendiente(cls) -> dict[str, Any] | None:
        cls._ensure_jobs_table()
        with engine.begin() as db:
            row=db.execute(text(f"""SELECT job_id,ruc,anio,mes,tipo_comprobante,operacion FROM {cls.JOB_TABLE} WHERE estado='pendiente' ORDER BY creado FOR UPDATE SKIP LOCKED LIMIT 1""")).mappings().first()
            if not row: return None
            db.execute(text(f"""UPDATE {cls.JOB_TABLE} SET estado='ejecutando', mensaje='Trabajo reclamado por el worker SRI interactivo.', actualizado=CURRENT_TIMESTAMP WHERE job_id=:job_id"""), {"job_id":row["job_id"]})
        return dict(row)

    LOGIN_URL = "https://srienlinea.sri.gob.ec/sri-en-linea/contribuyente/perfil"
    PORTAL_URL = "https://srienlinea.sri.gob.ec/tuportal-internet/accederAplicacion.jspa?redireccion=60&idGrupo=58"
    RECIBIDOS_URL = "https://srienlinea.sri.gob.ec/comprobantes-electronicos-internet/pages/consultas/recibidos/comprobantesRecibidos.jsf"

    @staticmethod
    def _dec(value: Any) -> Decimal:
        try: return Decimal(str(value or "0").strip().replace(",", "."))
        except (InvalidOperation, ValueError): return Decimal("0")

    @staticmethod
    def _txt(node: ET.Element | None, tag: str, default: str = "") -> str:
        return ((node.findtext(tag) if node is not None else None) or default).strip()

    @staticmethod
    def _tipo(tipo: int) -> str:
        return {1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07"}.get(tipo,f"{tipo:02d}")

    @classmethod
    def _credenciales(cls, ruc: str) -> dict[str, str]:
        with engine.connect() as db:
            row=db.execute(text("""SELECT ruccedcli, nomclient, activo, clavesri FROM clientes WHERE ruccedcli=:ruc LIMIT 1"""), {"ruc":ruc}).mappings().first()
        if not row: raise ValueError("El RUC no existe en BdTotal.")
        if not bool(row["activo"]): raise ValueError("El cliente no está activo.")
        clave=str(row["clavesri"] or "").strip()
        if not clave: raise ValueError("El cliente no tiene clave SRI configurada.")
        return {"ruc":str(row["ruccedcli"]),"nombre":str(row["nomclient"] or ""),"clave":clave}

    @classmethod
    def _parsear_xml(cls, path: Path) -> dict[str, Any]:
        root=ET.parse(path).getroot()
        raw=root.findtext("comprobante")
        if not raw: raise ValueError("El XML no contiene comprobante.")
        doc=ET.fromstring(raw)
        it=doc.find("infoTributaria"); inf=doc.find("infoFactura")
        if it is None or inf is None: raise ValueError("El comprobante no contiene infoTributaria/infoFactura.")
        fecha_txt=cls._txt(inf,"fechaEmision")
        try: fecha=datetime.strptime(fecha_txt,"%d/%m/%Y")
        except ValueError as exc: raise ValueError(f"Fecha de emisión inválida: {fecha_txt}") from exc
        bases={"no_objeto":Decimal("0"),"0":Decimal("0"),"5":Decimal("0"),"8":Decimal("0"),"12":Decimal("0"),"14":Decimal("0"),"15":Decimal("0"),"exenta":Decimal("0")}
        ivas={k:Decimal("0") for k in ("5","8","12","14","15")}
        ice=Decimal("0")
        codigo_a_tasa={"0":"0","2":"12","3":"14","4":"15","5":"5","8":"8"}
        impuestos_clasificados=Decimal("0")
        for impuesto in doc.findall(".//detalle/impuestos/impuesto"):
            codigo=cls._txt(impuesto,"codigo")
            if codigo!="2": continue
            codigo_pct=cls._txt(impuesto,"codigoPorcentaje")
            tarifa=cls._dec(cls._txt(impuesto,"tarifa"))
            base=cls._dec(cls._txt(impuesto,"baseImponible"))
            valor=cls._dec(cls._txt(impuesto,"valor"))
            cls._iva_debug_log("XML DETALLE | clave=%s | codigo=%s | codigoPorcentaje=%s | tarifa=%s | baseImponible=%s | valor=%s",cls._txt(it,"claveAcceso"),codigo,codigo_pct,tarifa,base,valor)
            tarifa_key=format(tarifa,"f").rstrip("0").rstrip(".")
            tasa=tarifa_key if tarifa_key in {"5","8","12","14","15"} else codigo_a_tasa.get(codigo_pct)
            cls._iva_debug_log("XML CLASIFICACION | clave=%s | codigoPorcentaje=%s | tarifa=%s | tasa_resultante=%s | base=%s | valor=%s",cls._txt(it,"claveAcceso"),codigo_pct,tarifa,tasa,base,valor)
            if tasa in {"5","8","12","14","15"}:
                bases[tasa]+=base; ivas[tasa]+=valor; impuestos_clasificados+=base
            elif tasa=="0": bases["0"]+=base
            elif codigo_pct=="6": bases["no_objeto"]+=base
            elif codigo_pct=="7": bases["exenta"]+=base
            else: bases["no_objeto"]+=base

        if impuestos_clasificados==0:
            totals=inf.find("totalConImpuestos")
            if totals is not None:
                for ti in totals.findall("totalImpuesto"):
                    codigo=cls._txt(ti,"codigo")
                    if codigo=="3": ice+=cls._dec(cls._txt(ti,"valor")); continue
                    if codigo!="2": continue
                    codigo_pct=cls._txt(ti,"codigoPorcentaje"); tarifa=cls._dec(cls._txt(ti,"tarifa")); base=cls._dec(cls._txt(ti,"baseImponible")); valor=cls._dec(cls._txt(ti,"valor"))
                    tarifa_key=format(tarifa,"f").rstrip("0").rstrip(".")
                    tasa=tarifa_key if tarifa_key in {"0","5","8","12","14","15"} else codigo_a_tasa.get(codigo_pct)
                    if tasa in {"5","8","12","14","15"}: bases[tasa]+=base; ivas[tasa]+=valor
                    elif tasa=="0": bases["0"]+=base
                    elif codigo_pct=="6": bases["no_objeto"]+=base
                    elif codigo_pct=="7": bases["exenta"]+=base
                    else: bases["no_objeto"]+=base

        logger.warning("SRI COMPRA PARSER | clave=%s | bases=%s | ivas=%s | subtotal=%s",cls._txt(it,"claveAcceso"),{k:str(v) for k,v in bases.items()},{k:str(v) for k,v in ivas.items()},cls._txt(inf,"totalSinImpuestos"))
        cls._iva_debug_log("XML FINAL | clave=%s | bases=%s | ivas=%s | subtotal=%s | total=%s",cls._txt(it,"claveAcceso"),{k:str(v) for k,v in bases.items()},{k:str(v) for k,v in ivas.items()},cls._txt(inf,"totalSinImpuestos"),cls._txt(inf,"importeTotal"))
        pagos=inf.find("pagos"); formas=[] if pagos is None else [cls._txt(p,"formaPago") for p in pagos.findall("pago")]

        return {"ruc":cls._txt(it,"ruc"),"razon_social":cls._txt(it,"razonSocial"),"cod_doc":cls._txt(it,"codDoc"),"numest":cls._txt(it,"estab"),"numptoemi":cls._txt(it,"ptoEmi"),"numsec":cls._txt(it,"secuencial"),"clave_acceso":cls._txt(it,"claveAcceso"),"fecha_emision":fecha_txt,"fecha":fecha,"fecha_autorizacion":cls._txt(root,"fechaAutorizacion"),"bases":bases,"ivas":ivas,"ice":ice,"subtotal":cls._dec(cls._txt(inf,"totalSinImpuestos")),"total":cls._dec(cls._txt(inf,"importeTotal")),"tipopago":formas[0] if formas else ""}

    @classmethod
    async def sincronizar_mes(cls, ruc: str, anio: int, mes: int, tipo_comprobante: int = 1, job_id: str | None = None, operacion: str = "compras") -> dict[str, Any]:
        cls._iva_debug_log("SINCRONIZAR MES | ruc=%s | anio=%s | mes=%s | tipo=%s | operacion=%s | servicio=%s",ruc,anio,mes,tipo_comprobante,operacion,str(Path(__file__).resolve()))
        cred=cls._credenciales(ruc); p=browser=context=page=chrome_process=None
        result={"ruc":ruc,"cliente":cred["nombre"],"anio":anio,"mes":mes,"tipo_comprobante":cls._tipo(tipo_comprobante),"sri":0,"ya_existentes":0,"descargadas":0,"guardadas":0,"errores":[],"paginas":0}
        try:
            cls._job_update(job_id,mensaje="Abriendo sesión del SRI.")
            p,browser,context,page,chrome_process=await cls._login(ruc,cred["clave"])
            cls._job_update(job_id,estado="captcha",mensaje="Consultando comprobantes en el SRI. Si aparece CAPTCHA, resuélvalo en Chromium.")
            if operacion=="ventas": await cls._consultar_emitidos(page,anio,mes)
            else: await cls._consultar_recibidos(page,anio,mes,tipo_comprobante)
            cls._job_update(job_id,estado="ejecutando",mensaje="Consulta completada. Procesando comprobantes.")
            db=obtener_session_cliente(ruc); procesadas=set()
            try:
                if operacion=="ventas":
                    await cls._procesar_emitidos_ventas(page,db,result,job_id,procesadas,anio,mes); return result
                for pagina in range(1,1001):
                    result["paginas"]=pagina
                    cls._job_update(job_id,mensaje=f"Procesando página {pagina}.",paginas=pagina)
                    links=page.locator('a[id*="lnkXml"], a[id$=":lnkXml"], input[id*="lnkXml"], button[id*="lnkXml"], a[title*="XML"], a[href*="xml"]')
                    total_links=await links.count()
                    if total_links==0: raise RuntimeError("SRI no devolvió comprobantes en la tabla actual.")
                    for idx in range(total_links):
                        try:
                            enlace_xml=links.nth(idx)
                            async with page.expect_download(timeout=30000) as info: await enlace_xml.click()
                            download=await info.value
                            with tempfile.TemporaryDirectory(prefix="conta_sri_") as tmp:
                                path=Path(tmp)/download.suggested_filename
                                await download.save_as(str(path))
                                factura=cls._parsear_xml(path)
                            clave=factura["clave_acceso"]; result["sri"]+=1
                            cls._job_update(job_id,sri=result["sri"],mensaje=f"Procesando comprobante {result['sri']}.")
                            if not clave: raise ValueError("El XML no contiene clave de acceso.")
                            if clave in procesadas: result["ya_existentes"]+=1; continue
                            procesadas.add(clave)
                            exists=db.execute(text("SELECT 1 FROM comprasnue WHERE TRIM(numaut::text)=:clave LIMIT 1"),{"clave":clave}).first()
                            if exists: result["ya_existentes"]+=1; continue
                            cls._insertar(db,factura,tipo_comprobante); db.commit()
                            result["descargadas"]+=1; result["guardadas"]+=1
                            cls._job_update(job_id,guardadas=result["guardadas"],descargadas=result["descargadas"],ya_existentes=result["ya_existentes"])
                        except Exception as exc:
                            db.rollback()
                            result["errores"].append({"pagina":pagina,"fila":idx+1,"detalle":str(exc)})
                            cls._job_update(job_id,errores=result["errores"],mensaje=f"Error procesando fila {idx+1}: {exc}")
                    if not await cls._siguiente_pagina(page): break
            finally: db.close()
        finally:
            if browser is not None:
                try: await browser.close()
                except Exception: pass
            elif context is not None:
                try: await context.close()
                except Exception: pass
            cls._cerrar_chrome(chrome_process)
            if p is not None: await p.stop()
        result["ok"]=not result["errores"]; return result

    @classmethod
    def _insertar(cls, db, factura: dict[str, Any], tipo_comprobante: int) -> None:
        b,i=factura["bases"],factura["ivas"]
        db.execute(text("SELECT pg_advisory_xact_lock(hashtext('conta_comprasnue_numcompra'))"))
        logger.warning("SRI COMPRA INSERT | clave=%s | base0=%s | base5=%s | base8=%s | base12=%s | base14=%s | base15=%s | iva15=%s",factura["clave_acceso"],b["0"],b["5"],b["8"],b["12"],b["14"],b["15"],i["15"])
        values={"codsus":"01","tipid":"01","ruccedprovee":factura["ruc"],"tipcom":cls._tipo(tipo_comprobante),"fecreg":factura["fecha"],"numest":factura["numest"],"numptoemi":factura["numptoemi"],"numsec":factura["numsec"],"fecemi":factura["fecha_emision"],"numaut":factura["clave_acceso"],"baseimpnoobj":b["no_objeto"],"baseimpiva0":b["0"],"baseimpiva12":b["12"],"baseexenta":b["exenta"],"montoice":factura["ice"],"montoiva":sum(i.values(),Decimal("0")),"retencioniva10":0,"retencioniva20":0,"retencioniva30":0,"retencioniva70":0,"retencioniva100":0,"totbases":sum(b.values(),Decimal("0")),"codret":"","baseimpret":"","porret":"","valret":"","numestret":"","numptoemiret":"","numsecret":"","numautret":"","fecret":"","tipopago":factura["tipopago"],"codtipodoc":"","numestmod":"","numptoemimod":"","numsecmod":"","numautmod":"","mes":f"{factura['fecha'].month:02d}","año":str(factura["fecha"].year),"nomprovee":factura["razon_social"],"baseimpiva5":b["5"],"baseimpiva8":b["8"],"baseimpiva14":b["14"],"baseimpiva15":b["15"],"montoiva5":i["5"],"montoiva8":i["8"],"montoiva12":i["12"],"montoiva14":i["14"],"montoiva15":i["15"]}
        next_num=db.execute(text("""SELECT COALESCE(MAX(CASE WHEN TRIM(numcompra::text) ~ '^[0-9]+$' THEN TRIM(numcompra::text)::bigint ELSE 0 END),0)+1 FROM comprasnue""")).scalar_one()
        values["numcompra"]=str(next_num)
        cols=", ".join(f'"{k}"' if k=="año" else k for k in values); params=", ".join(f":{k}" for k in values)
        cls._iva_debug_log("BD ANTES INSERT | clave=%s | numcompra=%s | baseimpiva0=%s | baseimpiva5=%s | baseimpiva8=%s | baseimpiva12=%s | baseimpiva14=%s | baseimpiva15=%s | montoiva5=%s | montoiva8=%s | montoiva12=%s | montoiva14=%s | montoiva15=%s",factura["clave_acceso"],values["numcompra"],values["baseimpiva0"],values["baseimpiva5"],values["baseimpiva8"],values["baseimpiva12"],values["baseimpiva14"],values["baseimpiva15"],values["montoiva5"],values["montoiva8"],values["montoiva12"],values["montoiva14"],values["montoiva15"])
        db.execute(text(f"INSERT INTO comprasnue ({cols}) VALUES ({params})"),values)
        almacenado=db.execute(text("""SELECT numcompra,numaut,baseimpiva0,baseimpiva5,baseimpiva8,baseimpiva12,baseimpiva14,baseimpiva15,montoiva5,montoiva8,montoiva12,montoiva14,montoiva15 FROM comprasnue WHERE numcompra=:numcompra LIMIT 1"""),{"numcompra":values["numcompra"]}).mappings().first()
        if almacenado:
            cls._iva_debug_log("BD DESPUES INSERT | clave=%s | numcompra=%s | baseimpiva0=%s | baseimpiva5=%s | baseimpiva8=%s | baseimpiva12=%s | baseimpiva14=%s | baseimpiva15=%s | montoiva5=%s | montoiva8=%s | montoiva12=%s | montoiva14=%s | montoiva15=%s",almacenado["numaut"],almacenado["numcompra"],almacenado["baseimpiva0"],almacenado["baseimpiva5"],almacenado["baseimpiva8"],almacenado["baseimpiva12"],almacenado["baseimpiva14"],almacenado["baseimpiva15"],almacenado["montoiva5"],almacenado["montoiva8"],almacenado["montoiva12"],almacenado["montoiva14"],almacenado["montoiva15"])
