import time
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import gspread
import pandas as pd
import plotly.express as px
import streamlit as st
from google.oauth2.service_account import Credentials

# =========================================================================
# CONFIGURACIÓN GENERAL
# =========================================================================
st.set_page_config(layout="wide", page_title="🚚 Planeación de Aliados", page_icon="🚚")

TZ_COL = ZoneInfo("America/Bogota")


def now_col():
    return datetime.now(TZ_COL).replace(tzinfo=None)


SHEET_ID = "1ySPooqSBmL3yJTyPONwdM7__RYwunge669xQg7Ngrao"  # mismo libro de siempre
META = 20  # rutas/cargues para "graduarse"

ANALISTAS = {
    "Deisy Liliana Garcia": "dgarcia@clicoh.com",
    "Erica Tatiana Garzon": "etgarzon@clicoh.com",
    "Dayan Stefany Suarez": "dsuarez@clicoh.com",
    "Carlos Andres Loaiza": "cloaiza@clicoh.com",
    "Diana Paola Rueda Jimenez": "drueda@clicoh.com",
}
NOMBRES_ANALISTAS = list(ANALISTAS.keys())
VEHICULOS = ["Moto", "Carry / Van", "Camión", "Otro"]

# Una única fuente de verdad para el acceso de coordinación (igual que en Programación).
COORDINATOR_PASSWORD = st.secrets.get("coordinator_password", "clicoh")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# -------------------------------------------------------------------------
# Catálogos: GESTIÓN DE ALIADOS (Planeación)
# -------------------------------------------------------------------------
RESULTADOS = ["Apagado", "Fuera de servicio", "No contestó", "Número errado", "Sí contestó"]

ESTADOS_FINALES_ALIADOS = [
    "Aliado Rechaza la oferta",
    "Aliado Fleet/Delivery no acepta hub/Carga en otra operacion",
    "Interesado Carga/Reserva",
    "Interesado esporádico no fijo",
    "Pendiente confirmación",
    "Point",
]

RAZONES = [
    "—",
    "Interesado carga ",
    "No le interesa / cuestiones personales",
    "No tiene Vh / Vh dañado",
    "Peso / Volumen / recorrido",
    "Tarifa/pago",
    "Tiene trabajo fijo",
    "Fuera de la ciudad",
    "Aliado no carga en HUB",
    "Pendiente confirmación",
    "Ocasional no fijo",
    "Se reserva",
    "Point",
]

SIN_CONTACTO = {"Apagado", "Fuera de servicio", "No contestó", "Número errado"}
BLOQUEO_INMEDIATO_ALIADOS = {"Aliado Rechaza la oferta", "Point"}
RAZONES_BLOQUEO_ALIADOS = {"No le interesa / cuestiones personales"}
# Razones que, sin importar el estado final elegido, también agendan la validación
# del día siguiente ("¿sí cargó?") en vez de un recontacto normal.
RAZONES_VALIDACION_ALIADOS = {"Interesado carga hoy", "Se reserva"}

# -------------------------------------------------------------------------
# Catálogos: REQUERIMIENTOS SUPPLY
# -------------------------------------------------------------------------
ESTADOS_FINALES_REQ = [
    "Aliado rechaza la oferta",
    "Carga en otra operación",
    "Interesado en carga / reserva",
    "Pendiente confirmación"
    "Interesado esporádico, no fijo",
]
RAZONES_REQ = [
    "—",
    "Interesado carga",
    "Pendiente confirmación"
    "No le interesa / cuestiones personales",
    "No tiene vehículo / vehículo dañado",
    "Peso / volumen / recorrido",
    "Tarifa / pago",
    "Tiene trabajo fijo",
    "Fuera de la ciudad",
    "Ocasional, no fijo",
]
BLOQUEO_INMEDIATO_REQ = {"Aliado rechaza la oferta"}
RAZONES_BLOQUEO_REQ = {"No le interesa / cuestiones personales"}
# Esta razón dispara la validación del día siguiente ("¿sí cargó?"), sin importar el estado final elegido.
RAZONES_VALIDACION_REQ = {"Interesado pendiente de cargue"}

# -------------------------------------------------------------------------
# Área responsable del aliado (aplica a Planeación y a Requerimientos)
# -------------------------------------------------------------------------
AREA_ALIADO_OPCIONES = ["—", "Supply", "Fleet", "Programación"]

# -------------------------------------------------------------------------
# Catálogos: COORDINADOR (base que envía el área de Implementación)
# -------------------------------------------------------------------------
ESTADO_CLICOH = ["Activo", "Inactivo", "Validacion Finalizada"]
ESTADO_IMPLEMENTACION = [
    "Activo", "Deserta", "En implementacion", "No responde",
    "Rechazado por Clicoh", "Se reserva", "Supera Implementacion",
]
ALIAS_COORDINADOR = {
    "nombre": "nombre", "documento": "documento", "cedula": "documento",
    "celular": "celular", "telefono": "celular", "ciudad": "ciudad",
    "vehiculo": "vehiculo", "vehículo": "vehiculo",
    "# rutas": "rutas", "rutas": "rutas", "numero de rutas": "rutas", "cantidad de rutas": "rutas",
    "estado clicoh": "estado_clicoh", "estado implementacion": "estado_implementacion",
    "estado implementación": "estado_implementacion", "fecha ultimo cargue": "fecha_ultimo_cargue",
    "fecha último cargue": "fecha_ultimo_cargue",
}

# Alias de encabezados para las bases de Planeación y Requerimientos que sube el Coordinador.
ALIAS_PLANEACION = {
    "identificacion": "identificacion", "identificación": "identificacion", "cedula": "identificacion",
    "cédula": "identificacion", "documento": "identificacion", "id": "identificacion",
    "nombre": "nombre", "celular": "celular", "telefono": "celular", "teléfono": "celular",
    "zona": "zona", "hub": "zona", "municipio": "zona",
    "vehiculo": "vehiculo", "vehículo": "vehiculo", "analista": "analista",
    "area": "area_aliado", "área": "area_aliado", "area del aliado": "area_aliado", "área del aliado": "area_aliado",
}
ALIAS_REQUERIMIENTOS_BASE = {
    "nombre": "nombre", "tel": "telefono", "telefono": "telefono", "teléfono": "telefono",
    "vh": "vehiculo", "vehiculo": "vehiculo", "vehículo": "vehiculo",
    "numero de requerimiento": "numero_requerimiento", "número de requerimiento": "numero_requerimiento",
    "numero_requerimiento": "numero_requerimiento", "requerimiento": "numero_requerimiento",
    "cantidad de rutas": "cantidad_rutas", "cantidad_rutas": "cantidad_rutas", "rutas": "cantidad_rutas",
    "area": "area_aliado", "área": "area_aliado", "area del aliado": "area_aliado", "área del aliado": "area_aliado",
}

# -------------------------------------------------------------------------
# Nombres de hojas y columnas (las 6 hojas ya existentes en el libro)
# -------------------------------------------------------------------------
COLS_PLANEACION = [
    "identificacion", "nombre", "celular", "zona", "vehiculo", "analista", "area_aliado",
    "estado_planeacion", "categoria", "razon",
    "intentos_llamada", "intentos_sin_contacto",
    "ultimo_resultado", "proxima_gestion",
    "bloqueado", "fecha_ingreso", "ultima_gestion", "observaciones",
]
COLS_PLANEACION_GESTIONES = ["fecha", "identificacion", "analista", "resultado", "estado_final", "razon", "proxima_gestion", "observaciones"]

COLS_REQUERIMIENTOS = [
    "numero_requerimiento", "nombre", "telefono", "vehiculo", "cantidad_rutas", "area_aliado",
    "estado_gestion", "ultimo_estado", "razon", "intentos_llamada", "intentos_sin_contacto",
    "ultimo_resultado", "proxima_gestion", "bloqueado", "fecha_ingreso", "ultima_gestion", "observaciones",
]
COLS_REQUERIMIENTOS_GESTIONES = ["fecha", "analista", "telefono", "numero_requerimiento", "nombre", "resultado", "estado_final", "razon", "proxima_gestion", "observaciones"]

COLS_COORDINADOR = [
    "documento", "nombre", "celular", "ciudad", "vehiculo", "rutas",
    "estado_clicoh", "estado_implementacion", "fecha_ultimo_cargue",
    "proxima_gestion", "ultima_gestion", "observaciones",
]
COLS_COORDINADOR_GESTIONES = ["fecha", "documento", "nombre", "estado_implementacion", "rutas", "observaciones"]


# =========================================================================
# CONEXIÓN Y E/S CON GOOGLE SHEETS  (mismo patrón robusto de Programación:
# get_all_values + limpieza de encabezados, en vez de get_all_records)
# =========================================================================
@st.cache_resource
def conectar_sheets():
    try:
        creds = Credentials.from_service_account_info(dict(st.secrets["gcp_service_account"]), scopes=SCOPES)
        return gspread.authorize(creds).open_by_key(st.secrets.get("planeacion_sheet_id", SHEET_ID))
    except Exception as e:
        st.error(f"Error de conexión con Google Sheets: {e}")
        return None


def _safe_str(val):
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except (TypeError, ValueError):
        pass
    if hasattr(val, "strftime"):
        try:
            return val.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return ""
    try:
        import numpy as np
        if isinstance(val, np.integer):
            return str(int(val))
        if isinstance(val, np.floating):
            return str(float(val))
        if isinstance(val, np.bool_):
            return str(bool(val))
    except ImportError:
        pass
    try:
        return str(val)
    except Exception:
        return ""


def _df_to_rows(df: pd.DataFrame) -> list:
    return [[_safe_str(v) for v in row] for row in df.values]


def _str_dict(d):
    """
    Convierte todos los valores de un dict a texto con _safe_str antes de
    meterlos en un DataFrame. Necesario porque el pandas de este entorno usa
    dtype 'str' estricto: un int, bool o date crudo revienta la carga entera
    ("Invalid value ... for dtype 'str'") apenas se intenta crear la fila.
    """
    return {k: _safe_str(v) for k, v in d.items()}


def leer_hoja(nombre_hoja, esperado_cols=None):
    """Lee toda la hoja con get_all_values (más tolerante a encabezados vacíos/duplicados que get_all_records)."""
    try:
        sh = conectar_sheets()
        if sh is None:
            return pd.DataFrame(columns=esperado_cols or [])
        try:
            ws = sh.worksheet(nombre_hoja)
        except gspread.WorksheetNotFound:
            cols = esperado_cols or []
            ws = sh.add_worksheet(nombre_hoja, rows=2000, cols=max(len(cols), 10) + 2)
            if cols:
                ws.append_row(cols)
            return pd.DataFrame(columns=cols)
        vals = ws.get_all_values()
        if not vals:
            if esperado_cols:
                ws.append_row(esperado_cols)
            return pd.DataFrame(columns=esperado_cols or [])
        headers = vals[0]
        clean, seen = [], {}
        for h in headers:
            h = str(h).strip()
            if not h or h.lower() == "none":
                h = f"_x{len(clean)}"
            if h in seen:
                seen[h] += 1
                h = f"{h}_{seen[h]}"
            else:
                seen[h] = 0
            clean.append(h)
        if len(vals) < 2:
            return pd.DataFrame(columns=clean)
        n = len(clean)
        rows = [r + [""] * (n - len(r)) if len(r) < n else r[:n] for r in vals[1:]]
        df = pd.DataFrame(rows, columns=clean)
        df = df[[c for c in df.columns if not c.startswith("_x")]]
        return df
    except Exception as e:
        st.warning(f"Aviso leyendo {nombre_hoja}: {e}")
        return pd.DataFrame(columns=esperado_cols or [])


def agregar_filas(nombre_hoja, rows: list):
    try:
        sh = conectar_sheets()
        if sh:
            sh.worksheet(nombre_hoja).append_rows(rows, value_input_option="USER_ENTERED")
    except Exception as e:
        st.error(f"Error guardando en {nombre_hoja}: {e}")


def reemplazar_hoja(nombre_hoja, df: pd.DataFrame):
    try:
        sh = conectar_sheets()
        if sh is None:
            return
        ws = sh.worksheet(nombre_hoja)
        ws.clear()
        if not df.empty:
            df_clean = df.loc[:, ~df.columns.duplicated()].copy()
            data = [df_clean.columns.tolist()] + _df_to_rows(df_clean)
            ws.update(data)
    except Exception as e:
        st.error(f"Error reemplazando {nombre_hoja}: {e}")


def actualizar_fila_por_id(nombre_hoja, columna_id, valor_id, cambios: dict):
    """
    Actualiza SOLO las celdas necesarias de una fila (batch_update) en vez de
    reescribir toda la hoja. Mismo patrón que actualizar_base_crm() de
    Programación: más rápido y evita bajar/subir todos los datos por cada gestión.
    """
    try:
        sh = conectar_sheets()
        if sh is None:
            return
        ws = sh.worksheet(nombre_hoja)
        headers = ws.row_values(1)
        if columna_id not in headers:
            return
        col_id_idx = headers.index(columna_id) + 1
        col_id_vals = ws.col_values(col_id_idx)
        try:
            fila_idx = col_id_vals.index(str(valor_id)) + 1
        except ValueError:
            return
        updates = []
        for campo, valor in cambios.items():
            if campo in headers:
                col_idx = headers.index(campo) + 1
                celda = gspread.utils.rowcol_to_a1(fila_idx, col_idx)
                updates.append({"range": celda, "values": [[_safe_str(valor)]]})
        if updates:
            ws.batch_update(updates)
    except Exception as e:
        st.warning(f"No se pudo actualizar {nombre_hoja}: {e}")


def a_entero(valor, defecto=0):
    try:
        return int(float(valor))
    except (ValueError, TypeError):
        return defecto


def es_verdadero(valor):
    return str(valor).strip().lower() in ("true", "1", "sí", "si", "yes")


def _normalizar_tel(valor):
    """
    Deja solo dígitos y quita el '.0' que Excel/una carga vieja pudo haber
    dejado en un teléfono o cédula. Se usa para BUSCAR, así encuentra el
    aliado aunque el dato guardado tenga espacios, guiones o ese sufijo.
    """
    v = str(valor).strip()
    if v.endswith(".0"):
        v = v[:-2]
    return "".join(ch for ch in v if ch.isdigit())


def _asegurar_columnas(nombre_hoja, columnas):
    """
    Si la hoja ya existe pero al código le agregaron columnas nuevas
    (p.ej. 'area_aliado', 'razon' en Requerimientos), las añade al final
    del encabezado sin tocar los datos existentes.
    """
    try:
        sh = conectar_sheets()
        if sh is None:
            return
        ws = sh.worksheet(nombre_hoja)
        headers = ws.row_values(1)
        if not headers:
            return
        faltantes = [c for c in columnas if c not in headers]
        for c in faltantes:
            headers.append(c)
            ws.update_cell(1, len(headers), c)
    except gspread.WorksheetNotFound:
        return
    except Exception as e:
        st.warning(f"No se pudieron verificar las columnas de {nombre_hoja}: {e}")


# =========================================================================
# CACHÉ EN SESIÓN  (mismo patrón que _get_base / _get_hist de Programación:
# los "rosters" se invalidan solo al escribir; los históricos se refrescan
# cada 30s o al forzar)
# =========================================================================
def _get_roster(cache_key, nombre_hoja, columnas, forzar=False):
    check_key = f"{cache_key}_cols_ok"
    if not st.session_state.get(check_key, False):
        _asegurar_columnas(nombre_hoja, columnas)
        st.session_state[check_key] = True
    stale_key = f"{cache_key}_stale"
    if forzar or cache_key not in st.session_state or st.session_state.get(stale_key, True):
        df = leer_hoja(nombre_hoja, columnas)
        for c in columnas:
            if c not in df.columns:
                df[c] = ""
        df = df[columnas].fillna("")
        st.session_state[cache_key] = df
        st.session_state[stale_key] = False
    return st.session_state[cache_key]


def _invalidar_roster(cache_key):
    st.session_state[f"{cache_key}_stale"] = True


def _get_historial(cache_key, nombre_hoja, columnas, ttl=30, forzar=False):
    key_t = f"{cache_key}_last"
    ahora = time.time()
    ultima = st.session_state.get(key_t, 0)
    if forzar or cache_key not in st.session_state or (ahora - ultima) > ttl:
        df = leer_hoja(nombre_hoja, columnas)
        if df.empty:
            df = pd.DataFrame(columns=columnas)
        else:
            for c in columnas:
                if c not in df.columns:
                    df[c] = ""
            df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
            df = df.dropna(subset=["fecha"])
        st.session_state[cache_key] = df
        st.session_state[key_t] = ahora
    return st.session_state[cache_key]


def _agregar_local(cache_key, fila_dict, columnas):
    """Actualiza la caché en memoria sin releer Sheets (igual que _hist_agregar_local)."""
    nuevo = pd.DataFrame([{c: _safe_str(fila_dict.get(c, "")) for c in columnas}])
    if "fecha" in nuevo.columns:
        nuevo["fecha"] = pd.to_datetime(fila_dict.get("fecha"))
    if cache_key in st.session_state and isinstance(st.session_state[cache_key], pd.DataFrame):
        st.session_state[cache_key] = pd.concat([st.session_state[cache_key], nuevo], ignore_index=True)
    else:
        st.session_state[cache_key] = nuevo


def _actualizar_roster_local(cache_key, columna_id, valor_id, cambios):
    if cache_key in st.session_state:
        df = st.session_state[cache_key]
        idx = df[df[columna_id].astype(str) == str(valor_id)].index
        if len(idx):
            for campo, valor in cambios.items():
                df.loc[idx[0], campo] = _safe_str(valor)
            st.session_state[cache_key] = df


# =========================================================================
# REGLAS DE NEGOCIO
# =========================================================================
def regla_intentos_sin_contacto(intentos):
    if intentos >= 15:
        return None, "Bloqueado permanente", True
    if intentos >= 10:
        return 15, "Pausado", False
    return 1, "En gestión", False


def procesar_gestion_planeacion(fila, resultado, estado_final, razon, nota):
    hoy = now_col().date()
    intentos_llamada = a_entero(fila.get("intentos_llamada")) + 1
    intentos_sin_contacto = a_entero(fila.get("intentos_sin_contacto"))

    cambios = {
        "intentos_llamada": intentos_llamada, "ultimo_resultado": resultado,
        "ultima_gestion": now_col(), "observaciones": nota or fila.get("observaciones", ""),
    }
    estado_final_log, razon_log = "", ""

    if resultado in SIN_CONTACTO:
        intentos_sin_contacto += 1
        dias, estado_planeacion, bloqueado = regla_intentos_sin_contacto(intentos_sin_contacto)
        cambios.update({
            "intentos_sin_contacto": intentos_sin_contacto, "estado_planeacion": estado_planeacion,
            "bloqueado": bloqueado, "proxima_gestion": "" if bloqueado else hoy + timedelta(days=dias),
        })
    else:
        estado_final_log, razon_log = estado_final, razon
        if (estado_final in BLOQUEO_INMEDIATO_ALIADOS) or (razon in RAZONES_BLOQUEO_ALIADOS):
            cambios.update({"estado_planeacion": "Bloqueado permanente", "bloqueado": True, "proxima_gestion": "",
                             "categoria": estado_final, "razon": razon})
        elif estado_final == "Aliado Fleet/Delivery no acepta hub/Carga en otra operacion":
            cambios.update({"estado_planeacion": "Pausado", "bloqueado": False,
                             "proxima_gestion": hoy + timedelta(days=5), "categoria": estado_final, "razon": razon})
        elif estado_final == "Interesado esporádico no fijo":
            cambios.update({"estado_planeacion": "En gestión", "bloqueado": False,
                             "proxima_gestion": hoy + timedelta(days=3), "categoria": estado_final, "razon": razon})
        elif estado_final == "Interesado Carga/Reserva" or razon in RAZONES_VALIDACION_ALIADOS:
            cambios.update({"estado_planeacion": "Validación pendiente", "bloqueado": False,
                             "proxima_gestion": hoy + timedelta(days=1), "categoria": estado_final, "razon": razon})
        else:
            cambios.update({"estado_planeacion": "En gestión", "bloqueado": False,
                             "proxima_gestion": hoy + timedelta(days=1), "categoria": estado_final, "razon": razon})

    log = {
        "fecha": now_col(), "identificacion": fila.get("identificacion"), "analista": fila.get("analista"),
        "resultado": resultado, "estado_final": estado_final_log, "razon": razon_log,
        "proxima_gestion": cambios.get("proxima_gestion", ""), "observaciones": nota,
    }
    return cambios, log


def procesar_validacion_planeacion(fila, cargo, nota):
    hoy = now_col().date()
    if cargo:
        cambios = {"estado_planeacion": "Completado", "proxima_gestion": "", "ultima_gestion": now_col(),
                   "observaciones": nota or fila.get("observaciones", "")}
        estado_log = "Validado - inició cargue"
    else:
        cambios = {"estado_planeacion": "En gestión", "proxima_gestion": hoy + timedelta(days=1),
                   "ultima_gestion": now_col(), "observaciones": nota or fila.get("observaciones", "")}
        estado_log = "No llegó a cargue - recontacto"
    log = {
        "fecha": now_col(), "identificacion": fila.get("identificacion"), "analista": fila.get("analista"),
        "resultado": "Sí contestó", "estado_final": estado_log, "razon": "",
        "proxima_gestion": cambios.get("proxima_gestion", ""), "observaciones": nota,
    }
    return cambios, log


def procesar_gestion_requerimiento(fila, resultado, estado_final, razon, nota):
    hoy = now_col().date()
    intentos_llamada = a_entero(fila.get("intentos_llamada")) + 1
    intentos_sin_contacto = a_entero(fila.get("intentos_sin_contacto"))
    cambios = {"intentos_llamada": intentos_llamada, "ultimo_resultado": resultado,
               "ultima_gestion": now_col(), "observaciones": nota or fila.get("observaciones", "")}
    estado_final_log, razon_log = "", ""

    if resultado in SIN_CONTACTO:
        intentos_sin_contacto += 1
        dias, estado_gestion, bloqueado = regla_intentos_sin_contacto(intentos_sin_contacto)
        cambios.update({"intentos_sin_contacto": intentos_sin_contacto, "estado_gestion": estado_gestion,
                         "bloqueado": bloqueado, "proxima_gestion": "" if bloqueado else hoy + timedelta(days=dias)})
    else:
        estado_final_log, razon_log = estado_final, razon
        if (estado_final in BLOQUEO_INMEDIATO_REQ) or (razon in RAZONES_BLOQUEO_REQ):
            cambios.update({"estado_gestion": "Bloqueado permanente", "bloqueado": True, "proxima_gestion": "",
                             "ultimo_estado": estado_final, "razon": razon})
        elif razon in RAZONES_VALIDACION_REQ:
            cambios.update({"estado_gestion": "Validación pendiente", "bloqueado": False,
                             "proxima_gestion": hoy + timedelta(days=1), "ultimo_estado": estado_final, "razon": razon})
        elif estado_final == "Carga en otra operación":
            cambios.update({"estado_gestion": "Pausado", "bloqueado": False,
                             "proxima_gestion": hoy + timedelta(days=5), "ultimo_estado": estado_final, "razon": razon})
        elif estado_final == "Interesado esporádico, no fijo":
            cambios.update({"estado_gestion": "En gestión", "bloqueado": False,
                             "proxima_gestion": hoy + timedelta(days=3), "ultimo_estado": estado_final, "razon": razon})
        else:
            cambios.update({"estado_gestion": "En gestión", "bloqueado": False,
                             "proxima_gestion": hoy + timedelta(days=1), "ultimo_estado": estado_final, "razon": razon})

    log = {
        "fecha": now_col(), "analista": fila.get("analista"), "telefono": fila.get("telefono"), "numero_requerimiento": fila.get("numero_requerimiento"),
        "nombre": fila.get("nombre"), "resultado": resultado, "estado_final": estado_final_log, "razon": razon_log,
        "proxima_gestion": cambios.get("proxima_gestion", ""), "observaciones": nota,
    }
    return cambios, log


def procesar_validacion_requerimiento(fila, uso_cupo, nota):
    hoy = now_col().date()
    if uso_cupo:
        cambios = {"estado_gestion": "Cerrado", "ultimo_estado": "Requerimiento cerrado/finalizado",
                   "proxima_gestion": "", "ultima_gestion": now_col(), "observaciones": nota or fila.get("observaciones", "")}
    else:
        cambios = {"estado_gestion": "En gestión", "proxima_gestion": hoy + timedelta(days=1),
                   "ultima_gestion": now_col(), "observaciones": nota or fila.get("observaciones", "")}
    log = {
        "fecha": now_col(), "analista": fila.get("analista"), "telefono": fila.get("telefono"), "numero_requerimiento": fila.get("numero_requerimiento"),
        "nombre": fila.get("nombre"), "resultado": "Sí contestó",
        "estado_final": cambios.get("ultimo_estado", "Recontacto - no usó el cupo"),
        "proxima_gestion": cambios.get("proxima_gestion", ""), "observaciones": nota,
    }
    return cambios, log


def _leer_archivo_subido(archivo):
    return pd.read_csv(archivo) if archivo.name.lower().endswith("csv") else pd.read_excel(archivo)


def cargar_incremental_planeacion(archivo):
    """
    Sube/actualiza la base de Planeación por 'identificacion'. Si el aliado ya
    existe, solo se actualizan sus datos de contacto (nombre/celular/zona/
    vehiculo/analista) — el historial de gestión (intentos, categoría,
    próxima gestión, bloqueo) NUNCA se toca. Si es nuevo, entra con CRM limpio.
    """
    df = _leer_archivo_subido(archivo)
    df.columns = [str(c).strip().lower() for c in df.columns]
    df = df.rename(columns={k: v for k, v in ALIAS_PLANEACION.items() if k in df.columns})
    if "identificacion" not in df.columns:
        st.error("El archivo no tiene columna de identificación (cédula/documento).")
        return 0, 0
    # Todo el archivo se pasa a texto de una vez. Excel suele traer teléfonos/
    # cédulas como número (int64); si no se convierten aquí, más abajo falla
    # al intentar meter un int en una columna de texto ("Invalid value ... for dtype 'str'").
    df = df.astype(str).replace("nan", "")
    existente = _get_roster("plan_roster", "PLANEACION_ALIADOS", COLS_PLANEACION, forzar=True)
    existente_norm = existente.identificacion.apply(_normalizar_tel)
    nuevos_n, actualizados_n = 0, 0
    for _, fn in df.iterrows():
        ident = str(fn.get("identificacion", "")).strip()
        ident_norm = _normalizar_tel(ident)
        if not ident_norm:
            continue
        datos = {c: str(fn.get(c, "")).strip() for c in ["nombre", "celular", "zona", "vehiculo", "analista", "area_aliado"] if c in df.columns and str(fn.get(c, "")).strip()}
        coincide = existente_norm[existente_norm == ident_norm]
        if not coincide.empty:
            idx = coincide.index[0]
            for campo, valor in datos.items():
                existente.loc[idx, campo] = valor
            actualizados_n += 1
        else:
            nueva_fila = {c: "" for c in COLS_PLANEACION}
            nueva_fila.update({
                "identificacion": ident, **datos, "estado_planeacion": "Nuevo",
                "intentos_llamada": 0, "intentos_sin_contacto": 0,
                "proxima_gestion": now_col().date(), "bloqueado": False, "fecha_ingreso": now_col().date(),
            })
            existente = pd.concat([existente, pd.DataFrame([_str_dict(nueva_fila)])], ignore_index=True)
            existente_norm = pd.concat([existente_norm, pd.Series([ident_norm], index=[existente.index[-1]])])
            nuevos_n += 1
    reemplazar_hoja("PLANEACION_ALIADOS", existente)
    st.session_state["plan_roster"] = existente
    st.session_state["plan_roster_stale"] = False
    return nuevos_n, actualizados_n


def cargar_incremental_requerimientos(archivo):
    """
    Sube/actualiza la base de Requerimientos por 'telefono' — NO por número de
    requerimiento, porque un mismo requerimiento trae varios aliados distintos
    y el teléfono es lo único que identifica a cada persona.
    """
    df = _leer_archivo_subido(archivo)
    df.columns = [str(c).strip().lower() for c in df.columns]
    df = df.rename(columns={k: v for k, v in ALIAS_REQUERIMIENTOS_BASE.items() if k in df.columns})
    if "telefono" not in df.columns:
        st.error("El archivo no tiene columna de teléfono (TEL).")
        return 0, 0
    df = df.astype(str).replace("nan", "")
    existente = _get_roster("req_roster", "REQUERIMIENTOS_ALIADOS", COLS_REQUERIMIENTOS, forzar=True)
    existente_norm = existente.telefono.apply(_normalizar_tel)
    nuevos_n, actualizados_n = 0, 0
    for _, fn in df.iterrows():
        tel = str(fn.get("telefono", "")).strip()
        tel_norm = _normalizar_tel(tel)
        if not tel_norm:
            continue
        datos = {c: str(fn.get(c, "")).strip() for c in ["nombre", "numero_requerimiento", "vehiculo", "cantidad_rutas", "area_aliado"] if c in df.columns and str(fn.get(c, "")).strip()}
        coincide = existente_norm[existente_norm == tel_norm]
        if not coincide.empty:
            idx = coincide.index[0]
            for campo, valor in datos.items():
                existente.loc[idx, campo] = valor
            actualizados_n += 1
        else:
            nueva_fila = {c: "" for c in COLS_REQUERIMIENTOS}
            nueva_fila.update({
                "telefono": tel, **datos, "estado_gestion": "Nuevo",
                "intentos_llamada": 0, "intentos_sin_contacto": 0,
                "proxima_gestion": now_col().date(), "bloqueado": False, "fecha_ingreso": now_col().date(),
            })
            existente = pd.concat([existente, pd.DataFrame([_str_dict(nueva_fila)])], ignore_index=True)
            existente_norm = pd.concat([existente_norm, pd.Series([tel_norm], index=[existente.index[-1]])])
            nuevos_n += 1
    reemplazar_hoja("REQUERIMIENTOS_ALIADOS", existente)
    st.session_state["req_roster"] = existente
    st.session_state["req_roster_stale"] = False
    return nuevos_n, actualizados_n


# =========================================================================
# CUMPLIMIENTO DE CARGUE: ¿los aliados que dijeron que iban a cargar, cargaron?
# Se alimenta con la exportación del tablero de Looker "Servicio - Operaciones
# Latam / Cumplimiento Diario Aliados" (una fila por hoja de ruta), que se va
# acumulando en la hoja CARGUES_REALES.
# =========================================================================
import re
import unicodedata

HOJA_CARGUES = "CARGUES_REALES"
COLS_CARGUES_REALES = ["cedula", "nombre", "ciudad", "hoja_ruta", "fecha_cargue", "estado_hr",
                       "paquetes", "entregados", "gestionados", "fecha_subida"]
# Solo cuentan como cargue las hojas de ruta que el aliado sí sacó a la calle.
# "Disponible" (nadie la tomó) y "Tomado" (asignada pero sin salir) no cuentan.
ESTADOS_HR_CARGUE = {"abierto", "cerrado"}
ESTADOS_VALIDACION = {"Validado - inició cargue", "No llegó a cargue - recontacto"}
ESTADO_COMPROMISO = "Interesado Carga/Reserva"
ESTADO_VALIDADO = "Validado - inició cargue"
DIAS_SEMANA = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
ANALISTA_CUMPLIMIENTO_DEFECTO = "Diana Paola Rueda Jimenez"


def _norm_encabezado(c):
    """'Hoja de Ruta', 'hoja_ruta' y 'HOJA DE RUTA ' quedan igual: sin tildes, minúsculas, espacios simples."""
    s = "".join(ch for ch in unicodedata.normalize("NFD", str(c)) if unicodedata.category(ch) != "Mn")
    return " ".join(s.lower().replace("_", " ").split())


# Columnas de la exportación de Looker (Cumplimiento Diario Aliados), ya normalizadas
# con _norm_encabezado. Las demás columnas del archivo se ignoran.
ALIAS_CARGUES = {
    "identificacion": "cedula",
    "nombre aliado": "nombre",
    "ciudad": "ciudad",
    "hjrt id": "hoja_ruta",
    "creacion": "fecha_cargue",
    "estado hr": "estado_hr",
    "tot paq": "paquetes",
    "entregados": "entregados",
    "paq gestionados por aliado": "gestionados",
}
COLS_LOOKER_OBLIGATORIAS = {"cedula": "identificacion", "hoja_ruta": "HJRT ID", "fecha_cargue": "Creacion", "paquetes": "Tot Paq"}
MESES_ES = {"ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6, "jul": 7, "ago": 8,
            "sep": 9, "set": 9, "oct": 10, "nov": 11, "dic": 12}


def _parse_fecha_cargue_serie(serie: pd.Series) -> pd.Series:
    """Acepta 'sept 22, 2026' (formato de Looker), fechas de Excel, '2026-09-22', '22/09/2026', '20260922' y seriales."""
    out = []
    for v in serie:
        if isinstance(v, (pd.Timestamp, datetime, date)):
            out.append(pd.Timestamp(v))
            continue
        s = _safe_str(v).strip()
        if s.endswith(".0"):
            s = s[:-2]
        if not s or s.lower() in ("nan", "none", "nat"):
            out.append(pd.NaT)
            continue
        m = re.match(r"^([a-zA-Z]{3})[a-zA-Z.]*\s+(\d{1,2}),?\s+(\d{4})", _norm_encabezado(s))
        if m and m.group(1) in MESES_ES:
            out.append(pd.Timestamp(int(m.group(3)), MESES_ES[m.group(1)], int(m.group(2))))
            continue
        if s.isdigit() and len(s) == 8:
            out.append(pd.to_datetime(s, format="%Y%m%d", errors="coerce"))
            continue
        try:
            n = float(s)
            if 20000 < n < 80000:
                out.append(pd.Timestamp("1899-12-30") + pd.Timedelta(days=n))
                continue
        except ValueError:
            pass
        iso = len(s) >= 10 and s[4] in "-/"
        out.append(pd.to_datetime(s, dayfirst=not iso, errors="coerce"))
    return pd.Series(out, index=serie.index)


def _ciudad_corta(valor):
    """'MELI Cali', 'CALI - VALLE DEL CAUCA' y 'Meli Cali' -> 'Cali'."""
    s = _norm_encabezado(valor).upper()
    if s in ("", "-", "NO APLICA", "NAN"):
        return "Sin ciudad"
    for pref in ("POINT ", "XPT ", "MELI "):
        if s.startswith(pref):
            s = s[len(pref):]
    s = s.replace("MELI", "").replace("PRIMERA MILLA", "").strip()
    s = s.split(" - ")[0].split("/")[0].replace(" CLARO", "").replace("D.C.", "").strip()
    s = {"SAN JOSE DE CUCUTA": "CUCUTA", "CARTAGENA DE INDIAS": "CARTAGENA"}.get(s, s)
    return s.title() if s else "Sin ciudad"


def cargar_incremental_cargues(archivo):
    """
    Suma a CARGUES_REALES la exportación de Looker tal como sale del tablero.
    La llave es la hoja de ruta (HJRT ID): si ya existía, se reemplaza con lo que
    trae el archivo nuevo, así se actualizan estado, entregados y gestionados.
    """
    df = _leer_archivo_subido(archivo)
    df.columns = [_norm_encabezado(c) for c in df.columns]
    df = df.rename(columns={k: v for k, v in ALIAS_CARGUES.items() if k in df.columns})
    df = df.loc[:, ~df.columns.duplicated()]
    faltan = [original for interno, original in COLS_LOOKER_OBLIGATORIAS.items() if interno not in df.columns]
    if faltan:
        st.error(f"Al archivo le faltan columnas del tablero: {', '.join(faltan)}. Descárgalo de nuevo desde Looker sin quitar columnas.")
        return None

    texto = lambda col: df[col].map(lambda v: _safe_str(v).strip()) if col in df.columns else ""
    numero = lambda col: pd.to_numeric(df[col], errors="coerce").fillna(0).round().astype(int) if col in df.columns else 0
    d = pd.DataFrame({
        "cedula": df["cedula"].map(_normalizar_tel),
        "nombre": texto("nombre").map(lambda v: " ".join(v.split())) if "nombre" in df.columns else "",
        "ciudad": texto("ciudad"),
        "hoja_ruta": df["hoja_ruta"].map(lambda v: _safe_str(v).strip()[:-2] if _safe_str(v).strip().endswith(".0") else _safe_str(v).strip()),
        "fecha": _parse_fecha_cargue_serie(df["fecha_cargue"]),
        "estado_hr": texto("estado_hr"),
        "paquetes": numero("paquetes"), "entregados": numero("entregados"), "gestionados": numero("gestionados"),
    })
    validas = (d.cedula != "") & (d.hoja_ruta != "") & d.fecha.notna()
    descartadas = int((~validas).sum())
    d = d[validas].copy()
    d["fecha_cargue"] = d.fecha.dt.strftime("%Y-%m-%d")
    d = d.drop_duplicates("hoja_ruta", keep="last")
    d["fecha_subida"] = now_col().strftime("%Y-%m-%d %H:%M:%S")
    d = d[COLS_CARGUES_REALES].astype(str)

    existente = _get_roster("cargues_roster", HOJA_CARGUES, COLS_CARGUES_REALES, forzar=True)
    llave_exist = existente.hoja_ruta.astype(str).str.strip()
    actualizados = int(d.hoja_ruta.isin(set(llave_exist)).sum())
    nuevos = len(d) - actualizados
    combinado = pd.concat([existente[~llave_exist.isin(set(d.hoja_ruta))], d], ignore_index=True)
    combinado = combinado.sort_values(["fecha_cargue", "cedula"]).astype(str).replace("nan", "")
    reemplazar_hoja(HOJA_CARGUES, combinado)
    st.session_state["cargues_roster"] = combinado
    st.session_state["cargues_roster_stale"] = False
    rango = (d.fecha_cargue.min(), d.fecha_cargue.max()) if len(d) else (None, None)
    return {"filas": len(df), "nuevos": nuevos, "actualizados": actualizados, "descartadas": descartadas,
            "desde": rango[0], "hasta": rango[1]}


def construir_cumplimiento(hist, cargues, aliados, analista, f1, f2):
    """
    hist: PLANEACION_GESTIONES · cargues: CARGUES_REALES · aliados: tabla con
    identificacion / nombre / ciudad. Devuelve KPIs, detalle por aliado
    comprometido, cruce por estado de todos los que contestaron y tablas por
    día de la semana, día y ciudad. Regla: cuenta como cargue una hoja de ruta
    creada desde el DÍA SIGUIENTE a la llamada; si no la hay, es "No cargó".
    """
    h = hist.copy()
    h["identificacion"] = h.identificacion.map(_normalizar_tel)
    for c in ["analista", "resultado", "estado_final", "razon"]:
        h[c] = h[c].astype(str).str.strip().replace({"nan": "", "None": ""})
    h = h.sort_values("fecha")
    # Las validaciones ("¿sí cargó?") pueden quedar sin analista: se le atribuyen
    # a quien llamó antes a ese mismo aliado.
    h["analista"] = h.analista.replace("", pd.NA)
    h["analista"] = h.groupby("identificacion").analista.ffill().fillna("Sin analista")
    h["es_llamada"] = ~h.estado_final.isin(ESTADOS_VALIDACION)
    h = h[(h.fecha.dt.date >= f1) & (h.fecha.dt.date <= f2)]
    if analista != "Todas":
        h = h[h.analista == analista]
    llamadas = h[h.es_llamada].copy()

    # ---- cargues reales
    c = cargues.copy()
    c["cedula"] = c.cedula.map(_normalizar_tel)
    c["fecha"] = pd.to_datetime(c.fecha_cargue, errors="coerce")
    for col in ["paquetes", "entregados", "gestionados"]:
        c[col] = pd.to_numeric(c[col], errors="coerce").fillna(0)
    c = c.dropna(subset=["fecha"])
    estado = c.estado_hr.astype(str).str.strip().str.lower()
    c = c[estado.isin(ESTADOS_HR_CARGUE) | (estado == "")]  # solo HR que sí salieron
    por_cedula = {k: g for k, g in c.groupby("cedula")}
    salto = pd.Timedelta(days=1)  # el cargue se cuenta desde el día siguiente a la llamada

    def cruce(ident, fecha_llamada):
        umbral = fecha_llamada.normalize() + salto
        g = por_cedula.get(ident)
        post = g[g.fecha >= umbral] if g is not None else c.iloc[0:0]
        antes = g[g.fecha < fecha_llamada.normalize()] if g is not None else c.iloc[0:0]
        rutas = int(post.hoja_ruta.nunique())
        res = "✅ Cargó" if rutas > 0 else "❌ No cargó"
        primer = post.fecha.min() if rutas else pd.NaT
        return {
            "resultado": res, "rutas_despues": rutas, "paquetes_despues": int(post.paquetes.sum()),
            "entregados_despues": int(post.entregados.sum()), "gestionados_despues": int(post.gestionados.sum()),
            "primer_cargue": primer.date() if pd.notna(primer) else None,
            "dias_hasta_cargar": int((primer - fecha_llamada.normalize()).days) if pd.notna(primer) else None,
            "ultimo_cargue_antes": antes.fecha.max().date() if not antes.empty else None,
        }

    # Nombre y ciudad: se toma el último dato no vacío de cada fuente (Looker, Planeación, Coordinador)
    info = pd.concat([aliados, cargues.rename(columns={"cedula": "identificacion"})[["identificacion", "nombre", "ciudad"]]
                      .assign(ciudad=lambda t: t.ciudad.map(_ciudad_corta))], ignore_index=True)
    info["identificacion"] = info.identificacion.map(_normalizar_tel)
    info = info.replace({"": pd.NA, "Sin ciudad": pd.NA, "nan": pd.NA}).groupby("identificacion").last()
    nombre_de = lambda i: info.nombre.get(i) if pd.notna(info.nombre.get(i)) else ""
    ciudad_de = lambda i: info.ciudad.get(i) if pd.notna(info.ciudad.get(i)) else "Sin ciudad"

    # ---- aliados comprometidos (dijeron que iban a cargar)
    marca = h.estado_final.isin([ESTADO_COMPROMISO, ESTADO_VALIDADO]) | h.razon.isin(RAZONES_VALIDACION_ALIADOS)
    comp = h[marca].sort_values("fecha").groupby("identificacion", as_index=False).first()
    filas = []
    for r in comp.itertuples():
        x = cruce(r.identificacion, r.fecha)
        filas.append({
            "cedula": r.identificacion, "nombre": nombre_de(r.identificacion), "ciudad": ciudad_de(r.identificacion),
            "fecha_llamada": r.fecha.date(), "dia": DIAS_SEMANA[r.fecha.weekday()], "analista": r.analista,
            "estado_registrado": r.estado_final, "razon": r.razon, **x,
        })
    detalle = pd.DataFrame(filas, columns=[
        "cedula", "nombre", "ciudad", "fecha_llamada", "dia", "analista", "estado_registrado", "razon",
        "resultado", "rutas_despues", "paquetes_despues", "entregados_despues", "gestionados_despues",
        "primer_cargue", "dias_hasta_cargar", "ultimo_cargue_antes"])

    # ---- todos los que contestaron, según lo que respondieron
    cont = llamadas[llamadas.resultado == "Sí contestó"]
    por_estado = pd.DataFrame(columns=["Respuesta en la llamada", "Aliados", "Cargaron", "No cargaron", "% cumplimiento", "Hojas de ruta", "Paquetes", "Entregados"])
    if not cont.empty:
        primera = cont.groupby("identificacion").fecha.min()
        ultima_resp = cont.groupby("identificacion").estado_final.last().replace("", "Sin estado")
        filas_e = []
        for ident, f0 in primera.items():
            x = cruce(ident, f0)
            filas_e.append({"estado": ultima_resp[ident], **x})
        e = pd.DataFrame(filas_e)
        e["cargo"] = e.resultado == "✅ Cargó"
        e["no"] = e.resultado == "❌ No cargó"
        por_estado = e.groupby("estado").agg(Aliados=("estado", "size"), Cargaron=("cargo", "sum"), **{"No cargaron": ("no", "sum")},
                                             **{"Hojas de ruta": ("rutas_despues", "sum")}, Paquetes=("paquetes_despues", "sum"),
                                             Entregados=("entregados_despues", "sum")).reset_index()
        medibles = por_estado.Cargaron + por_estado["No cargaron"]
        por_estado["% cumplimiento"] = (por_estado.Cargaron / medibles.where(medibles > 0) * 100).round(1)
        por_estado = por_estado.rename(columns={"estado": "Respuesta en la llamada"}).sort_values("Aliados", ascending=False)
        por_estado = por_estado[["Respuesta en la llamada", "Aliados", "Cargaron", "No cargaron", "% cumplimiento", "Hojas de ruta", "Paquetes", "Entregados"]]

    # ---- KPIs generales
    n = len(llamadas)
    dias = llamadas.fecha.dt.date.nunique()
    contestadas = int((llamadas.resultado == "Sí contestó").sum())
    horas = 0.0
    if n:
        horas = llamadas.groupby([llamadas.analista, llamadas.fecha.dt.date]).fecha.agg(
            lambda s: (s.max() - s.min()).total_seconds() / 3600).sum()
    cargaron = int((detalle.resultado == "✅ Cargó").sum())
    no_cargaron = int((detalle.resultado == "❌ No cargó").sum())

    # ---- por día de la semana
    dw = pd.DataFrame({"Día": DIAS_SEMANA})
    dw["Llamadas"] = [int((llamadas.fecha.dt.weekday == i).sum()) for i in range(7)]
    dw["Contestaron"] = [int(((llamadas.fecha.dt.weekday == i) & (llamadas.resultado == "Sí contestó")).sum()) for i in range(7)]
    dias_det = pd.to_datetime(detalle.fecha_llamada).dt.weekday if not detalle.empty else pd.Series(dtype=int)
    dw["Dijeron que cargarían"] = [int((dias_det == i).sum()) for i in range(7)]
    dw["Cargaron"] = [int(((dias_det == i) & (detalle.resultado == "✅ Cargó")).sum()) for i in range(7)]
    dw["Paquetes"] = [int(detalle.paquetes_despues[dias_det == i].sum()) for i in range(7)]
    dw = dw[(dw.Llamadas > 0) | (dw["Dijeron que cargarían"] > 0)]

    def mejor(tabla, col_nombre):
        if tabla.empty:
            return "—"
        claves = [k for k in ["Cargaron", "Dijeron que cargarían", "Contestaron"] if k in tabla.columns]
        orden = tabla.sort_values(claves, ascending=False)
        return orden.iloc[0][col_nombre]

    # ---- por ciudad
    if not detalle.empty:
        cy = detalle.groupby("ciudad").agg(**{"Dijeron que cargarían": ("cedula", "size")},
                                           Cargaron=("resultado", lambda s: int((s == "✅ Cargó").sum())),
                                           **{"No cargaron": ("resultado", lambda s: int((s == "❌ No cargó").sum()))},
                                           **{"Hojas de ruta": ("rutas_despues", "sum")}, Paquetes=("paquetes_despues", "sum"),
                                           Entregados=("entregados_despues", "sum")).reset_index()
        med = cy.Cargaron + cy["No cargaron"]
        cy["% cumplimiento"] = (cy.Cargaron / med.where(med > 0) * 100).round(1)
        cy = cy.rename(columns={"ciudad": "Ciudad"}).sort_values(["Cargaron", "Dijeron que cargarían"], ascending=False)
    else:
        cy = pd.DataFrame(columns=["Ciudad", "Dijeron que cargarían", "Cargaron", "No cargaron", "Hojas de ruta", "Paquetes", "Entregados", "% cumplimiento"])

    por_dia = llamadas.groupby(llamadas.fecha.dt.date).agg(Llamadas=("resultado", "size"),
                                                          Contestaron=("resultado", lambda s: int((s == "Sí contestó").sum()))).reset_index()
    por_dia.columns = ["Fecha", "Llamadas", "Contestaron"]

    kpis = {
        "llamadas": n, "dias": dias, "prom_dia": round(n / dias, 1) if dias else 0,
        "contactabilidad": round(contestadas / n * 100, 1) if n else 0,
        "llamadas_hora": round(n / horas, 1) if horas else 0,
        "interesados": int((llamadas.estado_final == ESTADO_COMPROMISO).sum()),
        "validados": int((h.estado_final == ESTADO_VALIDADO).sum()),
        "mejor_dia": mejor(dw, "Día"), "mejor_ciudad": mejor(cy, "Ciudad"),
        "comprometidos": len(detalle), "cargaron": cargaron, "no_cargaron": no_cargaron,
        "cumplimiento": round(cargaron / len(detalle) * 100, 1) if len(detalle) else None,
        "rutas": int(detalle.rutas_despues.sum()) if not detalle.empty else 0,
        "paquetes": int(detalle.paquetes_despues.sum()) if not detalle.empty else 0,
        "entregados": int(detalle.entregados_despues.sum()) if not detalle.empty else 0,
        "gestionados": int(detalle.gestionados_despues.sum()) if not detalle.empty else 0,
        "dias_prom_cargar": round(detalle.dias_hasta_cargar.dropna().astype(float).mean(), 1) if detalle.dias_hasta_cargar.notna().any() else None,
        "corte": c.fecha.max().date() if not c.empty else None,
    }
    return {"kpis": kpis, "detalle": detalle, "por_estado": por_estado, "por_semana": dw, "por_ciudad": cy, "por_dia": por_dia}


# =========================================================================
# LOGIN
# =========================================================================
st.title("🚚 Planeación de Aliados")

with st.sidebar:
    st.markdown("### 👤 Acceso")
    perfil = st.selectbox("Soy:", ["— Selecciona —", "Coordinador", "Analista"])
    if perfil == "Coordinador":
        pwd = st.text_input("Contraseña", type="password")
        if pwd != COORDINATOR_PASSWORD:
            if pwd:
                st.error("Contraseña incorrecta")
            st.stop()
        st.success("✅ Coordinador")
        nombre = "Coordinador"
    elif perfil == "Analista":
        nombre = st.selectbox("¿Quién eres?", NOMBRES_ANALISTAS)
        st.success(f"✅ {nombre.split()[0]}")
    else:
        st.info("Selecciona tu perfil para continuar.")
        st.stop()

# =========================================================================
# PERFIL: COORDINADOR
# =========================================================================
if perfil == "Coordinador":
    tab_tablero, tab_carga, tab_hoy, tab_hist, tab_reglas, tab_cumpl = st.tabs(
        ["📊 Tablero", "📥 Cargar Bases", "📋 Gestión de Hoy", "📅 Histórico", "⚙️ Reglas", "🚚 Cumplimiento de cargue"]
    )

    with tab_tablero:
        if st.button("🔄 Actualizar todo desde Google Sheets"):
            for k in ["plan_roster", "req_roster"]:
                _invalidar_roster(k)
            st.rerun()

        df_plan = _get_roster("plan_roster", "PLANEACION_ALIADOS", COLS_PLANEACION)
        df_req = _get_roster("req_roster", "REQUERIMIENTOS_ALIADOS", COLS_REQUERIMIENTOS)

        st.subheader("Gestión de Aliados (Planeación)")
        if df_plan.empty:
            st.info("Aún no hay aliados cargados en Planeación.")
        else:
            pend = df_plan[pd.to_datetime(df_plan.proxima_gestion, errors="coerce") <= pd.Timestamp(now_col().date())]
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total en Planeación", len(df_plan))
            c2.metric("Pendientes hoy", len(pend[~pend.bloqueado.apply(es_verdadero)]))
            c3.metric("Pausados", int((df_plan.estado_planeacion == "Pausado").sum()))
            c4.metric("Bloqueados permanentes", int(df_plan.bloqueado.apply(es_verdadero).sum()))
            dist = df_plan[df_plan.categoria != ""].categoria.value_counts().reset_index()
            if not dist.empty:
                dist.columns = ["Categoría", "N"]
                st.plotly_chart(px.bar(dist, x="Categoría", y="N", title="Aliados por categoría (Planeación)"), use_container_width=True)

        st.subheader("Requerimientos Supply")
        if df_req.empty:
            st.info("Aún no hay requerimientos cargados.")
        else:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total requerimientos", len(df_req))
            c2.metric("En validación pendiente", int((df_req.estado_gestion == "Validación pendiente").sum()))
            c3.metric("Pausados", int((df_req.estado_gestion == "Pausado").sum()))
            c4.metric("Bloqueados permanentes", int(df_req.bloqueado.apply(es_verdadero).sum()))
            dist2 = df_req[df_req.area_aliado != ""].area_aliado.value_counts().reset_index()
            if not dist2.empty:
                dist2.columns = ["Área del aliado", "N"]
                st.plotly_chart(px.pie(dist2, values="N", names="Área del aliado", title="Requerimientos por área responsable"), use_container_width=True)

    with tab_carga:
        st.caption("Todas las cargas son incrementales: si el aliado/requerimiento ya existe se actualizan sus datos de contacto, sin tocar el historial de gestión. Si es nuevo, se agrega.")

        with st.expander("📋 Base de Gestión de Aliados (Planeación)", expanded=True):
            st.caption("Columnas esperadas: Identificación, Nombre, Celular, Zona, Vehículo, Analista (opcional).")
            archivo_plan = st.file_uploader("Excel o CSV", type=["xlsx", "xls", "csv"], key="up_plan")
            if archivo_plan is not None and st.button("🚀 Cargar base de Planeación", key="btn_up_plan"):
                try:
                    with st.spinner("Procesando..."):
                        nn, na = cargar_incremental_planeacion(archivo_plan)
                    st.success(f"✅ {nn} aliados nuevos · {na} aliados actualizados")
                except Exception as e:
                    st.error(f"No se pudo procesar el archivo: {e}")

        with st.expander("📨 Base de Requerimientos", expanded=False):
            st.caption("Columnas esperadas: Nombre, TEL, VH, Número de requerimiento, cantidad de rutas.")
            archivo_req = st.file_uploader("Excel o CSV", type=["xlsx", "xls", "csv"], key="up_req")
            if archivo_req is not None and st.button("🚀 Cargar base de Requerimientos", key="btn_up_req"):
                try:
                    with st.spinner("Procesando..."):
                        nn, na = cargar_incremental_requerimientos(archivo_req)
                    st.success(f"✅ {nn} requerimientos nuevos · {na} actualizados")
                except Exception as e:
                    st.error(f"No se pudo procesar el archivo: {e}")

    with tab_hoy:
        st.subheader("Gestión de hoy — supervisión")
        st.caption("Vista de solo lectura: muestra lo que los analistas han gestionado hoy.")
        if st.button("🔄 Actualizar", key="btn_ref_hoy"):
            hist_plan = _get_historial("plan_hist", "PLANEACION_GESTIONES", COLS_PLANEACION_GESTIONES, forzar=True)
            hist_req = _get_historial("req_hist", "REQUERIMIENTOS_GESTIONES", COLS_REQUERIMIENTOS_GESTIONES, forzar=True)
        else:
            hist_plan = _get_historial("plan_hist", "PLANEACION_GESTIONES", COLS_PLANEACION_GESTIONES)
            hist_req = _get_historial("req_hist", "REQUERIMIENTOS_GESTIONES", COLS_REQUERIMIENTOS_GESTIONES)

        hoy = now_col().date()
        hp = hist_plan[hist_plan.fecha.dt.date == hoy] if not hist_plan.empty else hist_plan
        hr = hist_req[hist_req.fecha.dt.date == hoy] if not hist_req.empty else hist_req

        total_llamadas = len(hp) + len(hr)
        contactados = int((hp.resultado == "Sí contestó").sum() if not hp.empty else 0) + int((hr.resultado == "Sí contestó").sum() if not hr.empty else 0)
        no_resp = total_llamadas - contactados
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("📞 Llamadas hoy", total_llamadas)
        c2.metric("✅ Contactados", contactados)
        c3.metric("📵 No responden", no_resp)
        c4.metric("📊 Contactabilidad", f"{round(contactados / total_llamadas * 100, 1) if total_llamadas else 0}%")

        st.markdown("#### 🧑‍💼 Gestión de Aliados (Planeación)")
        if hp is None or hp.empty:
            st.info("Sin gestiones de Planeación hoy.")
        else:
            prod = hp.groupby("analista").size().reset_index(name="llamadas")
            st.plotly_chart(px.bar(prod, x="analista", y="llamadas", title="Llamadas por analista — hoy"), use_container_width=True)
            hp_show = hp.copy()
            hp_show["Hora"] = hp_show.fecha.dt.strftime("%I:%M %p")
            st.dataframe(
                hp_show[["Hora", "analista", "identificacion", "resultado", "estado_final", "razon", "proxima_gestion", "observaciones"]].sort_values("Hora", ascending=False),
                hide_index=True, use_container_width=True,
            )

        st.markdown("#### 📨 Requerimientos")
        if hr is None or hr.empty:
            st.info("Sin gestiones de Requerimientos hoy.")
        else:
            hr_show = hr.copy()
            hr_show["Hora"] = hr_show.fecha.dt.strftime("%I:%M %p")
            st.dataframe(
                hr_show[["Hora", "numero_requerimiento", "telefono", "nombre", "resultado", "estado_final", "razon", "proxima_gestion", "observaciones"]].sort_values("Hora", ascending=False),
                hide_index=True, use_container_width=True,
            )

    with tab_hist:
        st.subheader("Histórico de gestiones")
        st.caption("Trazabilidad completa de lo registrado por los analistas.")
        if st.button("🔄 Actualizar histórico", key="btn_ref_coord_hist"):
            hist_plan = _get_historial("plan_hist", "PLANEACION_GESTIONES", COLS_PLANEACION_GESTIONES, forzar=True)
            hist_req = _get_historial("req_hist", "REQUERIMIENTOS_GESTIONES", COLS_REQUERIMIENTOS_GESTIONES, forzar=True)
        else:
            hist_plan = _get_historial("plan_hist", "PLANEACION_GESTIONES", COLS_PLANEACION_GESTIONES)
            hist_req = _get_historial("req_hist", "REQUERIMIENTOS_GESTIONES", COLS_REQUERIMIENTOS_GESTIONES)

        c1, c2 = st.columns(2)
        f1 = c1.date_input("Desde", now_col().date() - timedelta(days=7), max_value=now_col().date(), key="ch_f1")
        f2 = c2.date_input("Hasta", now_col().date(), max_value=now_col().date(), key="ch_f2")

        dp = hist_plan[(hist_plan.fecha.dt.date >= f1) & (hist_plan.fecha.dt.date <= f2)] if not hist_plan.empty else hist_plan
        dr = hist_req[(hist_req.fecha.dt.date >= f1) & (hist_req.fecha.dt.date <= f2)] if not hist_req.empty else hist_req

        st.markdown("#### 🧑‍💼 Gestión de Aliados (Planeación)")
        if dp is None or dp.empty:
            st.info("Sin gestiones de Planeación en ese rango.")
        else:
            total = len(dp)
            cont = int((dp.resultado == "Sí contestó").sum())
            inter = int(dp.estado_final.isin(["Interesado Carga/Reserva"]).sum())
            rech = int((dp.estado_final == "Aliado Rechaza la oferta").sum())
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("📞 Llamadas", total)
            c2.metric("✅ Contactados", cont)
            c3.metric("🚗 Interesados", inter)
            c4.metric("❌ Rechazos", rech)

            kpi = dp.groupby("analista").agg(llamadas=("resultado", "size")).reset_index()
            cont_a = dp[dp.resultado == "Sí contestó"].groupby("analista").size().reset_index(name="contactados")
            kpi = kpi.merge(cont_a, on="analista", how="left").fillna(0)
            kpi["contactados"] = kpi.contactados.astype(int)
            kpi["% contacto"] = (kpi.contactados / kpi.llamadas * 100).round(1)
            st.markdown("##### KPIs por analista")
            st.dataframe(kpi, hide_index=True, use_container_width=True)

            razones = dp[dp.razon != ""].razon.value_counts().reset_index()
            if not razones.empty:
                razones.columns = ["Razón", "N"]
                st.plotly_chart(px.bar(razones, x="Razón", y="N", title="Principales razones"), use_container_width=True)

            dp_show = dp.copy()
            dp_show["Fecha"] = dp_show.fecha.dt.strftime("%d/%m/%Y %I:%M %p")
            st.dataframe(
                dp_show[["Fecha", "analista", "identificacion", "resultado", "estado_final", "razon", "proxima_gestion", "observaciones"]].sort_values("Fecha", ascending=False),
                hide_index=True, use_container_width=True,
            )
            st.download_button("📥 Descargar Planeación (CSV)", dp.to_csv(index=False).encode("utf-8"), f"planeacion_{f1}_{f2}.csv", "text/csv")

        st.markdown("#### 📨 Requerimientos")
        if dr is None or dr.empty:
            st.info("Sin gestiones de Requerimientos en ese rango.")
        else:
            total_r = len(dr)
            cont_r = int((dr.resultado == "Sí contestó").sum())
            c1, c2, c3 = st.columns(3)
            c1.metric("📞 Llamadas", total_r)
            c2.metric("✅ Contactados", cont_r)
            c3.metric("📊 Contactabilidad", f"{round(cont_r / total_r * 100, 1) if total_r else 0}%")
            dr_show = dr.copy()
            dr_show["Fecha"] = dr_show.fecha.dt.strftime("%d/%m/%Y %I:%M %p")
            st.dataframe(
                dr_show[["Fecha", "numero_requerimiento", "telefono", "nombre", "resultado", "estado_final", "razon", "proxima_gestion", "observaciones"]].sort_values("Fecha", ascending=False),
                hide_index=True, use_container_width=True,
            )
            st.download_button("📥 Descargar Requerimientos (CSV)", dr.to_csv(index=False).encode("utf-8"), f"requerimientos_{f1}_{f2}.csv", "text/csv")

    with tab_reglas:
        st.markdown("#### Reglas — Gestión de Aliados (Planeación)")
        st.markdown("""
| Situación | Acción | Espera |
|---|---|---|
| No contestó / Apagado / Fuera de servicio / Número errado | Recontacto | 1 día |
| 10–14 intentos sin contacto (acumulado) | Pausa larga | 15 días |
| 15+ intentos sin contacto (acumulado) | ❌ Bloqueo permanente | Nunca |
| Estado "Interesado en carga / reserva", o razón "Interesado carga hoy" / "Se reserva" | Validar si cargó; si no, recontacto | 1 día |
| Estado "Aliado Fleet/Delivery no acepta hub/Carga en otra operacion" | Pausa | 5 días |
| Estado "Interesado esporádico no fijo" | Recontacto | 3 días |
| Estado "Aliado Rechaza la oferta" / "Point", o razón "No le interesa" | ❌ Bloqueo permanente | Nunca |
""")
        st.markdown("#### Reglas — Requerimientos Supply")
        st.markdown("""
| Situación | Acción | Espera |
|---|---|---|
| No contestó / Apagado / Fuera de servicio / Número errado | Recontacto | 1 día |
| 10–14 intentos sin contacto (acumulado) | Pausa larga | 15 días |
| 15+ intentos sin contacto (acumulado) | ❌ Bloqueo permanente | Nunca |
| Razón "Interesado pendiente de cargue" | Validar si cargó; si no, recontacto | 1 día |
| Estado "Carga en otra operación" | Pausa | 5 días |
| Estado "Interesado esporádico, no fijo" | Recontacto | 3 días |
| Estado "Aliado rechaza la oferta", o razón "No le interesa" | ❌ Bloqueo permanente | Nunca |
""")

    # ------------------------------------------------------------------
    # CUMPLIMIENTO DE CARGUE (¿los que dijeron que cargaban, cargaron?)
    # ------------------------------------------------------------------
    with tab_cumpl:
        st.subheader("🚚 ¿Los aliados que dijeron que iban a cargar, cargaron?")

        with st.expander("📥 Subir hojas de ruta desde Looker", expanded=False):
            st.caption("Descarga la tabla del tablero 'Servicio - Operaciones Latam / Cumplimiento Diario Aliados' "
                       "(⋮ → Exportar → CSV o Excel) y súbela tal cual, sin quitar columnas. Se usan: identificacion, "
                       "HJRT ID, Creacion, Estado HR, Nombre Aliado, Ciudad, Tot Paq, Entregados y Paq Gestionados por Aliado. "
                       f"Cada carga se suma al historial (hoja {HOJA_CARGUES}); si una hoja de ruta ya estaba, se actualiza.")
            archivo_carg = st.file_uploader("Exportación de Looker (CSV o Excel)", type=["xlsx", "xls", "csv"], key="up_cargues")
            if archivo_carg is not None and st.button("🚀 Cargar hojas de ruta", key="btn_up_cargues"):
                try:
                    with st.spinner("Cruzando hojas de ruta..."):
                        r = cargar_incremental_cargues(archivo_carg)
                    if r:
                        st.success(f"✅ {r['nuevos']} hojas de ruta nuevas · {r['actualizados']} actualizadas "
                                   f"(de {r['filas']} filas, del {r['desde']} al {r['hasta']}).")
                        if r["descartadas"]:
                            st.warning(f"{r['descartadas']} filas no se tomaron por no tener identificacion, HJRT ID o Creacion válida.")
                except Exception as e:
                    st.error(f"No se pudo procesar el archivo: {e}")

        cf1, cf2, cf3 = st.columns([2, 1.2, 1.2])
        opciones_an = ["Todas"] + NOMBRES_ANALISTAS
        an_sel = cf1.selectbox("Analista", opciones_an,
                               index=opciones_an.index(ANALISTA_CUMPLIMIENTO_DEFECTO) if ANALISTA_CUMPLIMIENTO_DEFECTO in opciones_an else 0,
                               key="cum_an")
        hoy_c = now_col().date()
        c_f1 = cf2.date_input("Desde", hoy_c.replace(day=1), max_value=hoy_c, key="cum_f1")
        c_f2 = cf3.date_input("Hasta", hoy_c, max_value=hoy_c, key="cum_f2")

        if st.button("🔄 Actualizar datos", key="btn_ref_cum"):
            _invalidar_roster("cargues_roster")
            _invalidar_roster("plan_roster")
            hist_cum = _get_historial("plan_hist", "PLANEACION_GESTIONES", COLS_PLANEACION_GESTIONES, forzar=True)
        else:
            hist_cum = _get_historial("plan_hist", "PLANEACION_GESTIONES", COLS_PLANEACION_GESTIONES)
        cargues_cum = _get_roster("cargues_roster", HOJA_CARGUES, COLS_CARGUES_REALES)

        # Nombre y ciudad del aliado: COORDINADOR_ALIADOS (ciudad) + PLANEACION_ALIADOS (nombre/zona)
        coord = _get_roster("coord_roster", "COORDINADOR_ALIADOS", COLS_COORDINADOR)
        plan = _get_roster("plan_roster", "PLANEACION_ALIADOS", COLS_PLANEACION)
        aliados_cum = pd.concat([
            pd.DataFrame({"identificacion": plan.identificacion, "nombre": plan.nombre, "ciudad": plan.zona.map(_ciudad_corta)}),
            pd.DataFrame({"identificacion": coord.documento, "nombre": coord.nombre, "ciudad": coord.ciudad.map(_ciudad_corta)}),
        ], ignore_index=True)

        if hist_cum is None or hist_cum.empty:
            st.info("Aún no hay gestiones registradas en PLANEACION_GESTIONES.")
        else:
            R = construir_cumplimiento(hist_cum, cargues_cum, aliados_cum, an_sel, c_f1, c_f2)
            k = R["kpis"]

            st.markdown("#### Gestión de llamadas")
            m = st.columns(5)
            m[0].metric("📞 Llamadas", k["llamadas"])
            m[1].metric("📅 Días con gestión", k["dias"])
            m[2].metric("Prom. llamadas/día", k["prom_dia"])
            m[3].metric("% Contactabilidad", f"{k['contactabilidad']}%")
            m[4].metric("Llamadas/hora", k["llamadas_hora"])
            m2 = st.columns(5)
            m2[0].metric("🚗 Interesados", k["interesados"])
            m2[1].metric("✔️ Validados", k["validados"])
            m2[2].metric("Día más fructífero", k["mejor_dia"])
            m2[3].metric("Ciudad con más efecto", k["mejor_ciudad"])

            st.markdown("#### Compromiso vs cargue real")
            if cargues_cum.empty:
                st.warning("Todavía no hay hojas de ruta cargadas. Sube la exportación de Looker arriba para ver quién cargó.")
            else:
                st.caption((f"Hojas de ruta disponibles hasta el {k['corte']:%d/%m/%Y}. " if k["corte"] else "")
                           + "Cuenta como cargue una hoja de ruta (Abierta o Cerrada) creada desde el día siguiente a la llamada.")
            n1, n2, n3, n4 = st.columns(4)
            n1.metric("Dijeron que cargarían", k["comprometidos"])
            n2.metric("✅ Cargaron", k["cargaron"])
            n3.metric("❌ No cargaron", k["no_cargaron"])
            n4.metric("% Cumplimiento", f"{k['cumplimiento']}%" if k["cumplimiento"] is not None else "—",
                      help="Cargaron / Dijeron que cargarían.")
            n5, n6, n7, n8 = st.columns(4)
            n5.metric("🛣️ Hojas de ruta", k["rutas"], help="Solo HR en estado Abierto o Cerrado.")
            n6.metric("📦 Paquetes (Tot Paq)", k["paquetes"])
            n7.metric("✅ Entregados", k["entregados"])
            n8.metric("% Gestión", f"{round(k['gestionados'] / k['paquetes'] * 100, 1)}%" if k["paquetes"] else "—",
                      help="Paq Gestionados por Aliado / Tot Paq")
            if k["dias_prom_cargar"] is not None:
                st.caption(f"En promedio tardaron {k['dias_prom_cargar']} días desde la llamada hasta cargar.")

            g1, g2 = st.columns(2)
            with g1:
                sem = R["por_semana"]
                if not sem.empty:
                    larga = sem.melt(id_vars="Día", value_vars=["Llamadas", "Contestaron", "Dijeron que cargarían", "Cargaron"],
                                     var_name="Indicador", value_name="Cantidad")
                    st.plotly_chart(px.bar(larga, x="Día", y="Cantidad", color="Indicador", barmode="group",
                                           title="Gestión por día de la semana",
                                           category_orders={"Día": DIAS_SEMANA}), use_container_width=True)
            with g2:
                pdia = R["por_dia"]
                if not pdia.empty:
                    larga_d = pdia.melt(id_vars="Fecha", value_vars=["Llamadas", "Contestaron"], var_name="Indicador", value_name="Cantidad")
                    st.plotly_chart(px.bar(larga_d, x="Fecha", y="Cantidad", color="Indicador", barmode="group",
                                           title="Llamadas por día"), use_container_width=True)

            st.markdown("##### Cargue según lo que respondieron en la llamada (todos los que contestaron)")
            st.dataframe(R["por_estado"], hide_index=True, use_container_width=True)

            st.markdown("##### Cumplimiento por ciudad")
            st.dataframe(R["por_ciudad"], hide_index=True, use_container_width=True)

            st.markdown("##### Detalle por aliado comprometido")
            det = R["detalle"]
            filtro_res = st.multiselect("Resultado", ["✅ Cargó", "❌ No cargó"],
                                        default=["✅ Cargó", "❌ No cargó"], key="cum_filtro")
            det_f = det[det.resultado.isin(filtro_res)]
            st.dataframe(det_f.rename(columns={
                "cedula": "Cédula", "nombre": "Nombre", "ciudad": "Ciudad", "fecha_llamada": "Fecha llamada", "dia": "Día",
                "analista": "Analista", "estado_registrado": "Estado registrado", "razon": "Razón", "resultado": "Resultado",
                "rutas_despues": "Hojas de ruta después", "paquetes_despues": "Paquetes después",
                "entregados_despues": "Entregados después", "gestionados_despues": "Gestionados después", "primer_cargue": "Primer cargue",
                "dias_hasta_cargar": "Días hasta cargar", "ultimo_cargue_antes": "Último cargue antes de la llamada"}),
                hide_index=True, use_container_width=True)
            st.download_button("📥 Descargar detalle (CSV)", det.to_csv(index=False).encode("utf-8-sig"),
                               f"cumplimiento_cargue_{c_f1}_{c_f2}.csv", "text/csv")


# =========================================================================
# PERFIL: ANALISTA
# =========================================================================
if perfil == "Analista":
    tab_aliados, tab_req, tab_mihist = st.tabs(["🧑‍💼 Gestión de Aliados", "📨 Requerimientos", "📅 Mi Histórico"])

    # ------------------------------------------------------------------
    # GESTIÓN DE ALIADOS (PLANEACIÓN)
    # ------------------------------------------------------------------
    with tab_aliados:
        df_plan = _get_roster("plan_roster", "PLANEACION_ALIADOS", COLS_PLANEACION)
        if df_plan.empty:
            st.info("Coordinación todavía no ha cargado la base de Planeación.")

        st.markdown("### 🔎 Buscar aliado para gestionar")
        modo_busq = st.radio("Buscar por", ["Cédula", "Celular"], horizontal=True, key="modo_busq_plan")
        campo_busq = "identificacion" if modo_busq == "Cédula" else "celular"
        busqueda = st.text_input(f"Ingresa el {modo_busq.lower()}", key="buscar_planeacion")

        if busqueda.strip():
            df_plan = _get_roster("plan_roster", "PLANEACION_ALIADOS", COLS_PLANEACION)
            busqueda_norm = _normalizar_tel(busqueda)
            coincidencias = df_plan[df_plan[campo_busq].apply(_normalizar_tel) == busqueda_norm] if busqueda_norm else df_plan.iloc[0:0]
            if coincidencias.empty:
                st.warning("No se encontró ningún aliado con ese dato. Coordinación debe cargarlo en la base de Planeación.")
            elif len(coincidencias) > 1:
                st.info(f"Hay {len(coincidencias)} aliados con ese dato. Elige cuál vas a gestionar.")
                opciones = {f"{r.identificacion} — {r.nombre}": r.identificacion for _, r in coincidencias.iterrows()}
                elegido = st.selectbox("Aliado", list(opciones.keys()), key="elegido_plan")
                fila = coincidencias[coincidencias.identificacion.astype(str) == str(opciones[elegido])].iloc[0]
            else:
                fila = coincidencias.iloc[0]

            if not coincidencias.empty:
                st.markdown(f"""
- **Identificación:** {fila.identificacion}
- **Vehículo:** {fila.vehiculo}
- **Estado Aliado:** {fila.estado_planeacion or "Nuevo"}
- **Categoría:** {fila.categoria or "—"}
- **Nombre del mensajero:** {fila.nombre}
- **Celular:** {fila.celular}
- **Zona:** {fila.zona}
- **Intentos de llamada:** {a_entero(fila.intentos_llamada)}
- **Último resultado:** {fila.ultimo_resultado or "—"}
- **Último estado:** {fila.categoria or "—"}
- **Próxima gestión:** {fila.proxima_gestion or "—"}
""")
                st.markdown("#### 📋 Historial de gestiones")
                hist_plan_full = _get_historial("plan_hist", "PLANEACION_GESTIONES", COLS_PLANEACION_GESTIONES)
                hist_aliado = (
                    hist_plan_full[hist_plan_full.identificacion.astype(str) == str(fila.identificacion)].copy()
                    if hist_plan_full is not None and not hist_plan_full.empty else hist_plan_full
                )
                if hist_aliado is None or hist_aliado.empty:
                    st.info("Sin gestiones registradas para este aliado.")
                else:
                    hist_aliado["Hora"] = hist_aliado.fecha.dt.strftime("%d/%m/%Y %I:%M %p")
                    st.dataframe(
                        hist_aliado[["Hora", "analista", "resultado", "estado_final", "razon", "observaciones"]]
                        .rename(columns={"analista": "Analista", "resultado": "Resultado", "estado_final": "Estado", "razon": "Razón", "observaciones": "Obs"})
                        .sort_values("Hora", ascending=False),
                        hide_index=True, use_container_width=True,
                    )
                st.markdown("---")
                if es_verdadero(fila.bloqueado):
                    st.error("🚫 Este aliado está **bloqueado permanentemente** para Planeación.")
                elif fila.estado_planeacion == "Validación pendiente":
                    st.info("Este aliado dijo estar interesado en cargar. Confirma si llegó a hacer el cargue.")
                    with st.form("form_validacion_planeacion"):
                        cargo = st.radio("¿El aliado cargó?", ["Sí", "No"], horizontal=True)
                        nota = st.text_area("Observación", key="nota_validacion_plan")
                        enviar = st.form_submit_button("Guardar validación")
                    if enviar:
                        cambios, log = procesar_validacion_planeacion(fila, cargo == "Sí", nota)
                        actualizar_fila_por_id("PLANEACION_ALIADOS", "identificacion", fila.identificacion, cambios)
                        _actualizar_roster_local("plan_roster", "identificacion", fila.identificacion, cambios)
                        agregar_filas("PLANEACION_GESTIONES", [[_safe_str(log.get(c, "")) for c in COLS_PLANEACION_GESTIONES]])
                        _agregar_local("plan_hist", log, COLS_PLANEACION_GESTIONES)
                        st.success("Guardado.")
                        st.rerun()
                else:
                    st.markdown("#### 📞 Registrar gestión para este aliado")
                    with st.form("form_gestion_planeacion"):
                        c1, c2 = st.columns(2)
                        with c1:
                            resultado = st.selectbox("Resultado de la llamada", RESULTADOS, key="res_plan")
                        with c2:
                            estado_final = st.selectbox("Estado final (si contestó)", ["—"] + ESTADOS_FINALES_ALIADOS, key="estf_plan")
                        razon = st.selectbox("Razón (si contestó)", RAZONES, key="razon_plan")
                        nota = st.text_area("Observaciones", key="nota_plan")
                        enviar = st.form_submit_button("Guardar gestión")
                    if enviar:
                        if resultado == "Sí contestó" and estado_final == "—":
                            st.error("Selecciona el estado final.")
                        else:
                            # Estado final y razón solo cuentan si de verdad contestó; si no, se ignoran aunque queden seleccionados.
                            estado_final_final = estado_final if (resultado == "Sí contestó" and estado_final != "—") else ""
                            razon_final = razon if (resultado == "Sí contestó" and razon != "—") else ""
                            fila_ctx = dict(fila); fila_ctx["analista"] = nombre
                            cambios, log = procesar_gestion_planeacion(fila_ctx, resultado, estado_final_final, razon_final, nota)
                            actualizar_fila_por_id("PLANEACION_ALIADOS", "identificacion", fila.identificacion, cambios)
                            _actualizar_roster_local("plan_roster", "identificacion", fila.identificacion, cambios)
                            agregar_filas("PLANEACION_GESTIONES", [[_safe_str(log.get(c, "")) for c in COLS_PLANEACION_GESTIONES]])
                            _agregar_local("plan_hist", log, COLS_PLANEACION_GESTIONES)
                            st.success("Guardado.")
                            st.rerun()

        st.markdown("### 📋 Pendientes de gestión")
        df_plan = _get_roster("plan_roster", "PLANEACION_ALIADOS", COLS_PLANEACION)
        if not df_plan.empty:
            vista = df_plan[~df_plan.bloqueado.apply(es_verdadero)]
            st.dataframe(
                vista[["identificacion", "nombre", "celular", "zona", "analista", "estado_planeacion", "categoria", "intentos_llamada", "proxima_gestion"]],
                hide_index=True, use_container_width=True,
            )

    # ------------------------------------------------------------------
    # REQUERIMIENTOS SUPPLY
    # ------------------------------------------------------------------
    with tab_req:
        df_req = _get_roster("req_roster", "REQUERIMIENTOS_ALIADOS", COLS_REQUERIMIENTOS)
        if df_req.empty:
            st.info("Coordinación todavía no ha cargado la base de Requerimientos.")

        st.markdown("### 🔎 Buscar requerimiento para gestionar")
        modo_busq_r = st.radio("Buscar por", ["Número de requerimiento", "Teléfono"], horizontal=True, key="modo_busq_req")
        campo_busq_r = "numero_requerimiento" if modo_busq_r == "Número de requerimiento" else "telefono"
        busqueda_r = st.text_input(f"Ingresa el {modo_busq_r.lower()}", key="buscar_requerimiento")

        if busqueda_r.strip():
            df_req = _get_roster("req_roster", "REQUERIMIENTOS_ALIADOS", COLS_REQUERIMIENTOS)
            if campo_busq_r == "telefono":
                busq_norm = _normalizar_tel(busqueda_r)
                coincidencias = df_req[df_req.telefono.apply(_normalizar_tel) == busq_norm] if busq_norm else df_req.iloc[0:0]
            else:
                coincidencias = df_req[df_req.numero_requerimiento.astype(str).str.strip() == busqueda_r.strip()]
            if coincidencias.empty:
                st.warning("No se encontró ningún requerimiento con ese dato.")
            elif len(coincidencias) > 1:
                # Un mismo número de requerimiento puede traer varios aliados: hay que elegir cuál.
                st.info(f"Ese requerimiento tiene {len(coincidencias)} aliados asociados. Elige a cuál vas a gestionar.")
                opciones = {f"{r.telefono} — {r.nombre}": r.telefono for _, r in coincidencias.iterrows()}
                elegido = st.selectbox("Aliado", list(opciones.keys()), key="elegido_req")
                fila = coincidencias[coincidencias.telefono.astype(str) == str(opciones[elegido])].iloc[0]
            else:
                fila = coincidencias.iloc[0]

            if not coincidencias.empty:
                st.markdown(f"""
- **Número de requerimiento:** {fila.numero_requerimiento}
- **Nombre:** {fila.nombre}
- **Teléfono:** {fila.telefono}
- **Vehículo:** {fila.vehiculo}
- **Cantidad de rutas:** {fila.cantidad_rutas}
- **Área del aliado:** {fila.area_aliado or "—"}
- **Estado de gestión:** {fila.estado_gestion or "Nuevo"}
- **Último estado:** {fila.ultimo_estado or "—"}
- **Última razón:** {fila.razon or "—"}
- **Intentos de llamada:** {a_entero(fila.intentos_llamada)}
- **Próxima gestión:** {fila.proxima_gestion or "—"}
""")
                st.markdown("#### 📋 Historial de gestiones")
                hist_req_full = _get_historial("req_hist", "REQUERIMIENTOS_GESTIONES", COLS_REQUERIMIENTOS_GESTIONES)
                hist_req_aliado = (
                    hist_req_full[hist_req_full.telefono.astype(str) == str(fila.telefono)].copy()
                    if hist_req_full is not None and not hist_req_full.empty else hist_req_full
                )
                if hist_req_aliado is None or hist_req_aliado.empty:
                    st.info("Sin gestiones registradas para este aliado.")
                else:
                    hist_req_aliado["Hora"] = hist_req_aliado.fecha.dt.strftime("%d/%m/%Y %I:%M %p")
                    st.dataframe(
                        hist_req_aliado[["Hora", "analista", "resultado", "estado_final", "razon", "observaciones"]]
                        .rename(columns={"analista": "Analista", "resultado": "Resultado", "estado_final": "Estado", "razon": "Razón", "observaciones": "Obs"})
                        .sort_values("Hora", ascending=False),
                        hide_index=True, use_container_width=True,
                    )
                st.markdown("---")
                if es_verdadero(fila.bloqueado):
                    st.error("🚫 Este requerimiento está **bloqueado permanentemente**.")
                elif fila.estado_gestion == "Cerrado":
                    st.success("Este requerimiento ya está cerrado.")
                elif fila.estado_gestion == "Validación pendiente":
                    st.info("El aliado dijo que iba a usar el cupo. Confirma si lo usó.")
                    with st.form("form_validacion_requerimiento"):
                        uso = st.radio("¿El aliado usó el cupo / hizo el cargue?", ["Sí", "No"], horizontal=True)
                        nota = st.text_area("Observación", key="nota_validacion_req")
                        enviar = st.form_submit_button("Guardar validación")
                    if enviar:
                        fila_ctx = dict(fila); fila_ctx["analista"] = nombre
                        cambios, log = procesar_validacion_requerimiento(fila_ctx, uso == "Sí", nota)
                        actualizar_fila_por_id("REQUERIMIENTOS_ALIADOS", "telefono", fila.telefono, cambios)
                        _actualizar_roster_local("req_roster", "telefono", fila.telefono, cambios)
                        agregar_filas("REQUERIMIENTOS_GESTIONES", [[_safe_str(log.get(c, "")) for c in COLS_REQUERIMIENTOS_GESTIONES]])
                        _agregar_local("req_hist", log, COLS_REQUERIMIENTOS_GESTIONES)
                        st.success("Guardado.")
                        st.rerun()
                else:
                    st.markdown("#### 📞 Registrar gestión para este aliado")
                    with st.form("form_gestion_requerimiento"):
                        c1, c2 = st.columns(2)
                        with c1:
                            resultado = st.selectbox("Resultado de la llamada", RESULTADOS, key="res_req")
                        with c2:
                            estado_final = st.selectbox("Estado final (si contestó)", ["—"] + ESTADOS_FINALES_REQ, key="estf_req")
                        razon = st.selectbox("Razón (si contestó)", RAZONES_REQ, key="razon_req")
                        area_idx = AREA_ALIADO_OPCIONES.index(fila.area_aliado) if fila.area_aliado in AREA_ALIADO_OPCIONES else 0
                        area_sel = st.selectbox("Área del aliado", AREA_ALIADO_OPCIONES, index=area_idx, key="area_req")
                        nota = st.text_area("Observaciones", key="nota_req")
                        enviar = st.form_submit_button("Guardar gestión")
                    if enviar:
                        if resultado == "Sí contestó" and estado_final == "—":
                            st.error("Selecciona el estado final.")
                        else:
                            estado_final_final = estado_final if (resultado == "Sí contestó" and estado_final != "—") else ""
                            razon_final = razon if (resultado == "Sí contestó" and razon != "—") else ""
                            fila_ctx = dict(fila); fila_ctx["analista"] = nombre
                            cambios, log = procesar_gestion_requerimiento(fila_ctx, resultado, estado_final_final, razon_final, nota)
                            if area_sel != "—":
                                cambios["area_aliado"] = area_sel
                            actualizar_fila_por_id("REQUERIMIENTOS_ALIADOS", "telefono", fila.telefono, cambios)
                            _actualizar_roster_local("req_roster", "telefono", fila.telefono, cambios)
                            agregar_filas("REQUERIMIENTOS_GESTIONES", [[_safe_str(log.get(c, "")) for c in COLS_REQUERIMIENTOS_GESTIONES]])
                            _agregar_local("req_hist", log, COLS_REQUERIMIENTOS_GESTIONES)
                            st.success("Guardado.")
                            st.rerun()

        st.markdown("### 📋 Requerimientos pendientes de gestión")
        df_req = _get_roster("req_roster", "REQUERIMIENTOS_ALIADOS", COLS_REQUERIMIENTOS)
        if not df_req.empty:
            vista = df_req[~df_req.bloqueado.apply(es_verdadero) & (df_req.estado_gestion != "Cerrado")]
            st.dataframe(
                vista[["numero_requerimiento", "nombre", "telefono", "vehiculo", "cantidad_rutas", "area_aliado", "estado_gestion", "ultimo_estado", "razon", "intentos_llamada", "proxima_gestion"]],
                hide_index=True, use_container_width=True,
            )

    # ------------------------------------------------------------------
    # MI HISTÓRICO
    # ------------------------------------------------------------------
    with tab_mihist:
        st.subheader(f"Historial de {nombre}")
        if st.button("🔄 Actualizar mi historial"):
            hist_plan = _get_historial("plan_hist", "PLANEACION_GESTIONES", COLS_PLANEACION_GESTIONES, forzar=True)
            hist_req = _get_historial("req_hist", "REQUERIMIENTOS_GESTIONES", COLS_REQUERIMIENTOS_GESTIONES, forzar=True)
        else:
            hist_plan = _get_historial("plan_hist", "PLANEACION_GESTIONES", COLS_PLANEACION_GESTIONES)
            hist_req = _get_historial("req_hist", "REQUERIMIENTOS_GESTIONES", COLS_REQUERIMIENTOS_GESTIONES)

        c1, c2 = st.columns(2)
        f1 = c1.date_input("Desde", now_col().date() - timedelta(days=7), max_value=now_col().date(), key="mh_f1")
        f2 = c2.date_input("Hasta", now_col().date(), max_value=now_col().date(), key="mh_f2")

        st.markdown("#### Gestión de Aliados")
        mp = hist_plan[(hist_plan.analista == nombre) & (hist_plan.fecha.dt.date >= f1) & (hist_plan.fecha.dt.date <= f2)] if not hist_plan.empty else hist_plan
        if mp is None or mp.empty:
            st.info("Sin gestiones de Planeación en ese rango.")
        else:
            c1, c2, c3 = st.columns(3)
            c1.metric("Llamadas", len(mp))
            c2.metric("Contactados", int((mp.resultado == "Sí contestó").sum()))
            c3.metric("Interesados", int((mp.estado_final == "Interesado Carga/Reserva").sum()))
            st.dataframe(mp.sort_values("fecha", ascending=False), hide_index=True, use_container_width=True)
            if len(mp) >= 3:
                rr = mp.resultado.value_counts().reset_index()
                rr.columns = ["Resultado", "N"]
                st.plotly_chart(px.pie(rr, values="N", names="Resultado", title="Distribución de resultados — Planeación"), use_container_width=True)

        st.markdown("#### Requerimientos")
        mr = hist_req[(hist_req.fecha.dt.date >= f1) & (hist_req.fecha.dt.date <= f2)] if not hist_req.empty else hist_req
        if mr is None or mr.empty:
            st.info("Sin gestiones de Requerimientos en ese rango.")
        else:
            st.dataframe(mr.sort_values("fecha", ascending=False), hide_index=True, use_container_width=True)
