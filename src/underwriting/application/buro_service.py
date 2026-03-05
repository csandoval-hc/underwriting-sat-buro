import os
import requests
import pandas as pd
from abc import ABC, abstractmethod
from dotenv import load_dotenv
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
# Cargar variables de entorno
load_dotenv()

TIPO_CREDITO = {
    "1302": "QUIROG",
    "1305": "SIMPLE",
    "6280": "LÍNEA DE CRÉDITO",
    "1380": "T. CRED. EMPRESARIAL CORPORATIVA",
    "6260": "CRÉDITO FISCAL",
    "3011": "FACTORAJE C/REC",
    "1322": "ARREND",
    "6270": "CRÉDITO AUTOMOTRIZ",
}

map_tipo_cuenta = {
    'I': 'Pagos Fijos',
    'O': 'Cuenta Abierta',
    'R': 'Revolvente',
    'M': 'Hipotecario'
}

map_tipo_contrato = {
    'PL': 'Personal',
    'CC': 'Tarjeta Crédito',
    'RE': 'Hipotecario',
    'CL': 'Línea Crédito',
    'AU': 'Automotriz',
    'LR': 'Arrendamiento',
    'AL': 'Arrendamiento',
    'PS': 'Servicios'
}


# ======================================================
# =============== CLASE BASE ===========================
# ======================================================

class MoffinBuroBase(ABC):
    def __init__(self, rfc: str, service_name: str, original_key_prefix: str):
        self.rfc = rfc
        self.service_name = service_name
        self.original_key_prefix = original_key_prefix

        self.base_url = "https://app.moffin.mx/api/v1"
        self.token = os.getenv("MOFFIN_TOKEN", "").strip()

        if not self.token:
            raise EnvironmentError("MOFFIN_TOKEN no definido")

        self.headers = {
            "Authorization": f"Token {self.token}",
            "Accept": "application/json",
        }

        self._bureau_json: dict | None = None
        self._fecha_consulta: str | None = None
        self._original_key: str | None = None

    # ------------------------------
    # Obtener JSON más reciente
    # ------------------------------
    def _obtener_json_mas_reciente(self, limit: int = 50) -> None:
        offset = 0

        while True:
            params = {
                "search": self.rfc,
                "limit": limit,
                "offset": offset,
                "order": "DESC",
            }

            resp = requests.get(
                f"{self.base_url}/service_queries",
                headers=self.headers,
                params=params,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()

            batch = data.get("serviceQueries", [])
            if not batch:
                break

            for q in batch:
                if q.get("service") == self.service_name and q.get("response"):
                    self._bureau_json = q["response"]
                    self._fecha_consulta = q.get("createdAt")
                    self._original_key = (
                        f"{self.original_key_prefix}/{self.rfc}/{self._fecha_consulta}"
                        if self._fecha_consulta
                        else f"{self.original_key_prefix}/{self.rfc}"
                    )
                    return

            if len(batch) < limit:
                break

            offset += limit

        raise ValueError(f"No se encontró {self.service_name} para el RFC {self.rfc}")
    

    # ------------------------------
    # Métodos a implementar
    # ------------------------------
    @abstractmethod
    def _extraer_registros(self) -> list[dict]:
        pass

    # ------------------------------
    # Normalización base
    # ------------------------------
    def _estructurar_dataframe(self, registros: list[dict]) -> pd.DataFrame:
        if not registros:
            return pd.DataFrame()

        return pd.json_normalize(registros)

    # ------------------------------
    # Hook de formato final
    # ------------------------------
    def formatear_tabla(self, df: pd.DataFrame) -> pd.DataFrame:
        return df
    
    def _formatear_fecha_consulta(self):
        """
        Convierte createdAt tipo:
        '2024-06-10T11:28:08.123Z' -> '2024-06-10'
        """
        try:
            if not self._fecha_consulta:
                return None
                
            return self._fecha_consulta.split("T")[0]
        except Exception:
            return None


    def _formatear_monto(self, valor):
        """
        Convierte números a formato dinero: $12,345.67
        """
        try:
            if valor is None :
                return  f"${float(0):,.2f}"
            if valor is np.nan :
                return  f"${float(0):,.2f}"

            if isinstance(valor, str):
                valor = (
                    valor.replace("$", "")
                    .replace(",", "")
                    .replace("+", "")
                    .strip()
                )

            if valor == "":
                valor = 0

            return f"${float(valor):,.2f}"
        except Exception:
            return None
    
    def _formatear_fecha(self, valor):
        """
        Convierte distintos formatos de fecha a 'YYYY-MM-DD'

        Soporta:
        - '2024-10-28T00:00:00'
        - '2024-10-28'
        - '202003' (YYYYMM)
        - '31102014' (DDMMYYYY)
        """

        if not valor:
            return None

        valor = str(valor).strip()

        try:
            # ISO con tiempo
            if "T" in valor:
                return valor.split("T")[0]

            # YYYY-MM-DD
            if "-" in valor and len(valor) >= 10:
                return valor[:10]

            # YYYYMM (ej: 202003)
            if len(valor) == 6 and valor.isdigit():
                fecha = datetime.strptime(valor, "%Y%m")
                return fecha.strftime("%Y-%m")

            # DDMMYYYY (ej: 31102014)
            if len(valor) == 8 and valor.isdigit():
                fecha = datetime.strptime(valor, "%d%m%Y")
                return fecha.strftime("%Y-%m-%d")

            return None

        except Exception:
            return None



    def calcular_peor_mop(self, hist):

        if pd.isna(hist):
            return None

        hist = str(hist)
        hist = "".join(c for c in hist if c.isdigit())

        if len(hist) == 0:
            return None

        return max(int(c) for c in hist)

    # ------------------------------
    # Orquestador
    # ------------------------------
    def caller(self) -> pd.DataFrame:
        self._obtener_json_mas_reciente()
        registros = self._extraer_registros()
        df = self._estructurar_dataframe(registros)
        # 👇 AQUÍ inyectamos la fecha real de consulta
        if not df.empty:
            df["Fecha consulta"] = self._formatear_fecha_consulta()

        return self.formatear_tabla(df)

# ======================================================
# =============== PERSONA FÍSICA =======================
# ======================================================

class BuroMoffinPF(MoffinBuroBase):
    def __init__(self, rfc: str):
        super().__init__(
            rfc=rfc,
            service_name="bureau_pf",
            original_key_prefix="moffin_pf",
        )

    def _extraer_registros(self) -> list[dict]:
        persona = self._bureau_json["return"]["Personas"]["Persona"][0]
        cuentas = persona.get("Cuentas", {}).get("Cuenta", [])

        if isinstance(cuentas, dict):
            cuentas = [cuentas]

        return cuentas or []


    def _obtener_monto_pagar(self, df: pd.DataFrame) -> float:
        """
        Calcula el monto total a pagar a partir de la columna MontoPagar.
        Regresa float (no formateado).
        """
        if "MontoPagar" not in df.columns:
            return 0.0

        try:
            montos = (
                df["MontoPagar"]
                .astype(str)
                .str.replace("$", "", regex=False)
                .str.replace(",", "", regex=False)
                .str.replace("+", "", regex=False)
                .str.strip()
                .replace("", "0")
                .astype(float)
            )
            return montos.sum()
        except Exception:
            return 0.0



    def formatear_tabla(self, df: pd.DataFrame) -> pd.DataFrame:
        columnas = {
            "FechaActualizacion": "Fecha actualización",
            "FechaAperturaCuenta": "Fecha apertura",
            "NombreOtorgante": "Otorgante",
            "TipoCuenta": "Tipo de cuenta",
            "TipoContrato": "Tipo de contrato",
            "FrecuenciaPagos": "Frecuencia de pago",
            "MontoPagar": "Monto a pagar",
            "SaldoActual":"Saldo Actual",
            "Fecha consulta": "Fecha Consulta",
            "HistoricoPagos":"Comportamiento"


        }

        # Seleccionar solo columnas relevantes
        df_out = df[[c for c in columnas if c in df.columns]].copy()

        # Formatear fechas
        for col in ["FechaActualizacion", "FechaAperturaCuenta"]:
            if col in df_out.columns:
                df_out[col] = df_out[col].apply(self._formatear_fecha)

        # Formatear monto a pagar
        if "MontoPagar" in df_out.columns:
            df_out["MontoPagar"] = df_out["MontoPagar"].apply(self._formatear_monto)

        if "SaldoActual" in df_out.columns:
            df_out["SaldoActual"] = df_out["SaldoActual"].apply(self._formatear_monto)



        # Monto Total a pagar
        monto_total = self._obtener_monto_pagar(df_out)
        df_out["MontoTotalPagar"] = monto_total
        df_out["MontoTotalPagar"] = df_out["MontoTotalPagar"].apply(self._formatear_monto)
    
        # Hacemos map de tipo de cuenta y tipo de contrato
        df_out["TipoCuenta"] = (
            df_out["TipoCuenta"]
            .astype(str)
            .map(map_tipo_cuenta)
            .fillna(df_out["TipoCuenta"])
        )

        df_out["TipoContrato"] = (
            df_out["TipoContrato"]
            .astype(str)
            .map(map_tipo_contrato)
            .fillna(df_out["TipoContrato"])
        )
        # calculamos el peor mop dado el histórico de pagos
        df_out["peor_mop"] = df_out["HistoricoPagos"].apply(self.calcular_peor_mop)

        ############################################# METRICAS CONSTANTES (NO CAMBIAN CONFORME LAS FILAS)

        df_out["PEOR_MOP TOTAL"] = df_out["peor_mop"].max()
        df_out["_monto_num"] = pd.to_numeric(
            df_out["MontoPagar"]
            .astype(str)
            .str.replace(r"[$,]", "", regex=True),
            errors="coerce"
        ).fillna(0)

        df_out["Monto Máx"] = df_out["_monto_num"].max()

        totales_por_tipo = (
            df_out
            .groupby("TipoContrato")["_monto_num"]
            .sum()
        )

        for tipo in map_tipo_contrato.values():
            df_out[f"{tipo}"] = totales_por_tipo.get(tipo, 0)

        df_out["Monto Total"] = df_out["_monto_num"].sum()
        df_out = df_out.drop(columns = {"_monto_num"})
        ########################################### 

        linea_credito = pd.to_numeric(
            df_out["SaldoActual"].str.replace(r"[$,]", "", regex=True),
            errors="coerce"
        )
        uso = pd.to_numeric(
            df_out["MontoPagar"].str.replace(r"[$,]", "", regex=True),
            errors="coerce"
        )
        
        df_out["Porcentaje Ocupación"] = np.where(
            (df_out["TipoCuenta"] == "Revolvente") &
            (linea_credito > 0),
            uso / linea_credito,
            None
        )

        # Renombrar columnas
        df_out.rename(columns=columnas, inplace=True)

        # Orden lógico
        if "Fecha apertura" in df_out.columns:
            df_out.sort_values(
                by="Fecha apertura",
                ascending=False,
                inplace=True,
                ignore_index=True,
            )

    
        return df_out.drop(columns = "original_key", errors="ignore")


# ======================================================
# =============== PERSONA MORAl ========================
# ======================================================


class BuroMoffinPM(MoffinBuroBase):
    def __init__(self, rfc: str):
        super().__init__(
            rfc=rfc,
            service_name="bureau_pm",
            original_key_prefix="moffin_pm",
        )

    def _extraer_registros(self) -> list[dict]:
        respuesta = self._bureau_json.get("respuesta", {})
        credito = respuesta.get("creditoFinanciero", [])
        personas = respuesta.get("accionista", [])
  

        if isinstance(credito, dict):
            credito = [credito]
        
        if isinstance(personas, dict):
            personas = [personas]
        personas = self._estructurar_dataframe(personas)
        personas = personas.dropna(subset = "rfc")
        personas = personas.drop_duplicates(subset="rfc", keep = "first")
        self.personas = personas

        return credito or []
    

    def _calcular_pago_mensual(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula:
        PM(A) = saldoVigente / (plazo / 30.4)
        PM(B) = (saldoInicial - saldoVigente) / tiempo_transcurrido
        PM(C) = saldoVigente * 10%
        PM(D) = saldoInicial / (plazo / 30.4)

        Enriquece el dataframe con:
        - pago_a
        - pago_b
        - pago_c
        - pago_d
        """

        # -----------------------------
        # Conversión numérica segura
        # -----------------------------
        saldo_vigente = pd.to_numeric(df.get("saldoVigente"), errors="coerce")
        saldo_inicial = pd.to_numeric(df.get("saldoInicial"), errors="coerce")
        plazo_dias = pd.to_numeric(df.get("plazo"), errors="coerce")

        plazo_meses = plazo_dias / 30.4
        df["apertura"] = df["apertura"].apply(self._formatear_fecha)
        df["apertura"] = pd.to_datetime(
            df["apertura"],
            format="%Y-%m-%d",
            errors="coerce"
        )
        df["last_update"] = df["ultimoPeriodoActualizado"].apply(self._formatear_fecha)
        df["last_update"] = pd.to_datetime(
            df["last_update"],
            format="%Y-%m",
            errors="coerce"
       )

        
        df["fecha_estimada_finalizacion"] = df["apertura"] + pd.to_timedelta(plazo_dias, unit="D")
        df["fecha_estimada_finalizacion"] = df["fecha_estimada_finalizacion"].dt.to_period("M")
        plazo_meses_restantes = (
            (df["fecha_estimada_finalizacion"].dt.year * 12 + df["fecha_estimada_finalizacion"].dt.month)
            - (df["last_update"].dt.year * 12 + df["last_update"].dt.month)
        )

        plazo_meses_restantes = plazo_meses_restantes.clip(lower=1)

        # -----------------------------
        # PM (A)
        # -----------------------------
        df["pago_a"] = (saldo_vigente / plazo_meses_restantes)

        # -----------------------------
        # PM (B)
        # -----------------------------
        pagado = saldo_inicial - saldo_vigente

        fecha_apertura = pd.to_datetime(
            df.get("apertura"),
            errors="coerce",
            format="%d%m%Y"
        )

        hoy = pd.Timestamp.today()

        tiempo_transcurrido_meses = (hoy - fecha_apertura).dt.days / 30.4
        tiempo_transcurrido_meses = tiempo_transcurrido_meses.clip(lower=1)

        df["pago_b"] = pagado / tiempo_transcurrido_meses

        # -----------------------------
        # PM (C)
        # -----------------------------
        df["pago_c"] = saldo_vigente * 0.10

        # -----------------------------
        # PM (D)
        # -----------------------------
        df["pago_d"] = saldo_inicial / plazo_meses

        # -----------------------------
        # Limpieza final
        # -----------------------------
        df[["pago_a", "pago_b", "pago_c", "pago_d"]] = (
            df[["pago_a", "pago_b", "pago_c", "pago_d"]]
            .replace([np.inf, -np.inf], 0)
            .fillna(0)
        )

        return df



    # -------- método principal --------

    def formatear_tabla(self, df: pd.DataFrame) -> pd.DataFrame:

        columnas = {
            "ultimoPeriodoActualizado": "Fecha actualización",
            "apertura": "Fecha apertura",
            "tipoCredito": "Tipo de contrato",
            "tipoUsuario": "Tipo de otorgante",
            "saldoVigente": "Saldo Actual",
            "saldoInicial": "Monto Original",
            "plazo": "Plazo",
            "historicoPagos": "Comportamiento",
            "Fecha consulta": "Fecha Consulta",
            "fecha_estimada_finalizacion":"Fecha Estimada Finalización", 

        }

        # Seleccionar columnas existentes
        df_out = df[[c for c in columnas if c in df.columns]].copy()

        # Cambiamos el tipo de credito usando el catalogo 
        df_out["tipoCredito"] = (
            df_out["tipoCredito"]
            .astype(str)
            .map(TIPO_CREDITO)
            .fillna(df_out["tipoCredito"])
        )
        # obtenemos el peor mop
        df_out["peor_mop"] = df_out["historicoPagos"].apply(self.calcular_peor_mop)
        # Definimos nueva variable
        saldo_vigente = pd.to_numeric(
            df_out["saldoVigente"].str.replace(r"[$,]", "", regex=True),
            errors="coerce"
        )
        saldo_inicial = pd.to_numeric(
            df_out["saldoInicial"].str.replace(r"[$,]", "", regex=True),
            errors="coerce"
        )

        df_out["Porcentaje Ocupación"] = np.where(
            (df_out["tipoCredito"] == "LÍNEA DE CRÉDITO") &
            (saldo_inicial > 0),
            saldo_vigente / saldo_inicial,
            None
        )


        # Calcular MontoTotalPagar
        df_out= self._calcular_pago_mensual(df_out)

        # Formatear fechas
        for col in ["ultimoPeriodoActualizado", "apertura"]:
            if col in df_out.columns:
                df_out[col] = df_out[col].apply(self._formatear_fecha)

        # Formatear montos
        for col in ["saldoVigente", "saldoInicial"]:
            if col in df_out.columns:
                df_out[col] = df_out[col].apply(self._formatear_monto)


        # Renombrar columnas
        df_out.rename(columns=columnas, inplace=True)

        # Ordenar por fecha apertura
        if "Fecha apertura" in df_out.columns:
            df_out.sort_values(
                by="Fecha apertura",
                ascending=False,
                inplace=True,
                ignore_index=True,
            )
        df_out = df_out.drop(columns="original_key", errors="ignore")

        return df_out




# ======================================================
# =============== FUNCIÓN ÚNICA DE ENTRADA ==============
# ======================================================

def obtener_buro_moffin_por_rfc(rfc: str) -> pd.DataFrame:
    if not isinstance(rfc, str):
        raise TypeError("El RFC debe ser string")

    rfc = rfc.strip().upper()

    if len(rfc) == 13:
        return BuroMoffinPF(rfc).caller()

    elif len(rfc) == 12:
        obj = BuroMoffinPM(rfc)
        df = obj.caller()
        personas = obj.personas
        if "Fecha Consulta" not in df.columns:
            df = df.rename(columns = {"Fecha consulta":"Fecha Consulta"})
        return df, personas
    else:
        raise ValueError(
            f"RFC inválido: '{rfc}'. PF = 13 caracteres, PM = 12."
        )

#df, personas = obtener_buro_moffin_por_rfc("MLU190131AN6")

#personas.to_csv("personas_morales.csv")

#print(obtener_buro_moffin_por_rfc("OIPG850321355"))
#obtener_buro_moffin_por_rfc("VAPD630513HJ0").to_csv("pf.csv")
# df = obtener_buro_moffin_por_rfc("VAPD630513HJ0")
# print(df["Tipo de contrato"].unique())
#print(df["Tipo de cuenta"].unique())
 