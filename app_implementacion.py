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
    "Point",
]

RAZONES = [
    "—",
    "Interesado carga hoy",
    "No le interesa / cuestiones personales",
    "No tiene Vh / Vh dañado",
    "Peso / Volumen / recorrido",
    "Tarifa/pago",
    "Tiene trabajo fijo",
    "Fuera de la ciudad",
    "Aliado no carga en HUB",
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
    "Interesado esporádico, no fijo",
]
RAZONES_REQ = [
    "—",
    "Interesado pendiente de cargue",
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
COLS_REQUERIMIENTOS_GESTIONES = ["fecha", "telefono", "numero_requerimiento", "nombre", "resultado", "estado_final", "razon", "proxima_gestion", "observaciones"]

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
        "fecha": now_col(), "telefono": fila.get("telefono"), "numero_requerimiento": fila.get("numero_requerimiento"),
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
        "fecha": now_col(), "telefono": fila.get("telefono"), "numero_requerimiento": fila.get("numero_requerimiento"),
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
            existente = pd.concat([existente, pd.DataFrame([nueva_fila])], ignore_index=True)
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
            existente = pd.concat([existente, pd.DataFrame([nueva_fila])], ignore_index=True)
            existente_norm = pd.concat([existente_norm, pd.Series([tel_norm], index=[existente.index[-1]])])
            nuevos_n += 1
    reemplazar_hoja("REQUERIMIENTOS_ALIADOS", existente)
    st.session_state["req_roster"] = existente
    st.session_state["req_roster_stale"] = False
    return nuevos_n, actualizados_n


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
    tab_tablero, tab_carga, tab_hoy, tab_hist, tab_reglas = st.tabs(
        ["📊 Tablero", "📥 Cargar Bases", "📋 Gestión de Hoy", "📅 Histórico", "⚙️ Reglas"]
    )

    with tab_tablero:
        if st.button("🔄 Actualizar todo desde Google Sheets"):
            for k in ["plan_roster", "req_roster", "coord_roster"]:
                _invalidar_roster(k)
            st.rerun()

        df_plan = _get_roster("plan_roster", "PLANEACION_ALIADOS", COLS_PLANEACION)
        df_req = _get_roster("req_roster", "REQUERIMIENTOS_ALIADOS", COLS_REQUERIMIENTOS)
        df_coord = _get_roster("coord_roster", "COORDINADOR_ALIADOS", COLS_COORDINADOR)

        st.subheader("Gestión de Aliados (Planeación)")
        if df_plan.empty:
            st.info("Aún no hay aliados registrados en Planeación.")
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
            st.info("Aún no hay requerimientos registrados.")
        else:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total requerimientos", len(df_req))
            c2.metric("Cerrados", int((df_req.estado_gestion == "Cerrado").sum()))
            c3.metric("Pendientes área encargada", int((df_req.estado_gestion == "Pendiente área encargada").sum()))
            c4.metric("Bloqueados permanentes", int(df_req.bloqueado.apply(es_verdadero).sum()))

        st.subheader("Coordinador")
        if df_coord.empty:
            st.info("Aún no se ha cargado ninguna base de Implementación.")
        else:
            rutas_num = df_coord.rutas.apply(a_entero)
            c1, c2, c3 = st.columns(3)
            c1.metric("Aliados en seguimiento", len(df_coord))
            c2.metric("Superaron la meta (20 rutas)", int((rutas_num >= META).sum()))
            c3.metric("Activos clicOH", int((df_coord.estado_clicoh == "Activo").sum()))
            dist2 = df_coord.estado_implementacion[df_coord.estado_implementacion != ""].value_counts().reset_index()
            if not dist2.empty:
                dist2.columns = ["Estado Implementación", "N"]
                st.plotly_chart(px.pie(dist2, values="N", names="Estado Implementación", title="Distribución Estado Implementación"), use_container_width=True)

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

        st.markdown("#### 🧑‍🏭 Base de Implementación (seguimiento a 20 rutas)")
        st.caption("Columnas esperadas: Nombre, Documento, Celular, Ciudad, Vehículo, # Rutas, Estado clicOH, Estado Implementación, fecha último cargue.")
        archivo = st.file_uploader("Excel o CSV", type=["xlsx", "xls", "csv"], key="up_impl")
        if archivo is not None:
            try:
                nuevos = pd.read_csv(archivo) if archivo.name.lower().endswith("csv") else pd.read_excel(archivo)
                nuevos.columns = [str(c).strip().lower() for c in nuevos.columns]
                nuevos = nuevos.rename(columns={k: v for k, v in ALIAS_COORDINADOR.items() if k in nuevos.columns})
                nuevos = nuevos.astype(str).replace("nan", "")
                faltantes = {"nombre", "documento"} - set(nuevos.columns)
                if faltantes:
                    st.error(f"Faltan columnas mínimas: {', '.join(faltantes)}.")
                else:
                    existente = _get_roster("coord_roster", "COORDINADOR_ALIADOS", COLS_COORDINADOR, forzar=True)
                    existentes_doc = set(existente.documento.astype(str))
                    for _, fn in nuevos.iterrows():
                        doc = str(fn.get("documento", "")).strip()
                        if not doc:
                            continue
                        rutas_n = a_entero(fn.get("rutas", 0))
                        estado_impl = str(fn.get("estado_implementacion", "") or "").strip()
                        if rutas_n >= META and estado_impl not in {"Rechazado por Clicoh", "Deserta"}:
                            estado_impl = "Supera Implementacion"
                        datos = {
                            "nombre": str(fn.get("nombre", "")), "celular": str(fn.get("celular", "")),
                            "ciudad": str(fn.get("ciudad", "")), "vehiculo": str(fn.get("vehiculo", "")),
                            "rutas": rutas_n, "estado_clicoh": str(fn.get("estado_clicoh", "")),
                            "estado_implementacion": estado_impl, "fecha_ultimo_cargue": str(fn.get("fecha_ultimo_cargue", "")),
                        }
                        if doc in existentes_doc:
                            idx = existente[existente.documento.astype(str) == doc].index[0]
                            for campo, valor in datos.items():
                                existente.loc[idx, campo] = valor
                        else:
                            nueva_fila = {c: "" for c in COLS_COORDINADOR}
                            nueva_fila.update({"documento": doc, **datos})
                            existente = pd.concat([existente, pd.DataFrame([nueva_fila])], ignore_index=True)
                            existentes_doc.add(doc)
                    reemplazar_hoja("COORDINADOR_ALIADOS", existente)
                    st.session_state["coord_roster"] = existente
                    st.session_state["coord_roster_stale"] = False
                    st.success(f"Base cargada: {len(nuevos)} registros procesados.")
                    st.rerun()
            except Exception as e:
                st.error(f"No se pudo leer el archivo: {e}")

    with tab_hoy:
        st.subheader("Gestión de hoy — Coordinador")
        df_coord = _get_roster("coord_roster", "COORDINADOR_ALIADOS", COLS_COORDINADOR)
        if df_coord.empty:
            st.info("Todavía no se ha cargado ninguna base.")
        else:
            hoy_str = str(now_col().date())
            hoy_df = df_coord[df_coord.ultima_gestion.astype(str).str.startswith(hoy_str)]
            st.dataframe(
                hoy_df[["documento", "nombre", "ciudad", "vehiculo", "rutas", "estado_implementacion", "proxima_gestion"]],
                hide_index=True, use_container_width=True,
            )
            st.markdown("#### ✏️ Actualizar un aliado")
            doc_sel = st.selectbox("Documento", df_coord.documento.astype(str).tolist(), key="doc_sel_coord")
            fila = df_coord[df_coord.documento.astype(str) == doc_sel].iloc[0]
            with st.form("form_actualizar_coordinador"):
                c1, c2 = st.columns(2)
                rutas_nueva = c1.number_input("Rutas actuales", min_value=0, value=a_entero(fila.rutas))
                estado_nuevo = c2.selectbox(
                    "Estado Implementación", ESTADO_IMPLEMENTACION,
                    index=ESTADO_IMPLEMENTACION.index(fila.estado_implementacion) if fila.estado_implementacion in ESTADO_IMPLEMENTACION else 0,
                )
                nota = st.text_area("Observación")
                enviar = st.form_submit_button("Guardar gestión")
            if enviar:
                if rutas_nueva >= META and estado_nuevo not in {"Rechazado por Clicoh", "Deserta"}:
                    estado_nuevo = "Supera Implementacion"
                cambios = {"rutas": rutas_nueva, "estado_implementacion": estado_nuevo,
                           "ultima_gestion": now_col(), "observaciones": nota or fila.observaciones}
                actualizar_fila_por_id("COORDINADOR_ALIADOS", "documento", doc_sel, cambios)
                _actualizar_roster_local("coord_roster", "documento", doc_sel, cambios)
                agregar_filas("COORDINADOR_GESTIONES", [[
                    _safe_str(now_col()), doc_sel, fila.nombre, estado_nuevo, str(rutas_nueva), nota,
                ]])
                _agregar_local("coord_hist", {
                    "fecha": now_col(), "documento": doc_sel, "nombre": fila.nombre,
                    "estado_implementacion": estado_nuevo, "rutas": rutas_nueva, "observaciones": nota,
                }, COLS_COORDINADOR_GESTIONES)
                st.success("Gestión guardada.")
                st.rerun()

    with tab_hist:
        st.subheader("Histórico Coordinador")
        if st.button("🔄 Actualizar histórico", key="btn_ref_coord_hist"):
            hist_coord = _get_historial("coord_hist", "COORDINADOR_GESTIONES", COLS_COORDINADOR_GESTIONES, forzar=True)
        else:
            hist_coord = _get_historial("coord_hist", "COORDINADOR_GESTIONES", COLS_COORDINADOR_GESTIONES)
        if hist_coord.empty:
            st.info("Sin gestiones registradas aún.")
        else:
            c1, c2 = st.columns(2)
            f1 = c1.date_input("Desde", now_col().date() - timedelta(days=7), max_value=now_col().date(), key="ch_f1")
            f2 = c2.date_input("Hasta", now_col().date(), max_value=now_col().date(), key="ch_f2")
            d = hist_coord[(hist_coord.fecha.dt.date >= f1) & (hist_coord.fecha.dt.date <= f2)]
            st.dataframe(d.sort_values("fecha", ascending=False), hide_index=True, use_container_width=True)
            st.download_button("📥 Descargar (CSV)", d.to_csv(index=False).encode("utf-8"), f"coordinador_{f1}_{f2}.csv", "text/csv")

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
- **Área del aliado:** {fila.area_aliado or "—"}
- **Intentos de llamada:** {a_entero(fila.intentos_llamada)}
- **Último resultado:** {fila.ultimo_resultado or "—"}
- **Último estado:** {fila.categoria or "—"}
- **Próxima gestión:** {fila.proxima_gestion or "—"}
""")
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
                    with st.form("form_gestion_planeacion"):
                        resultado = st.selectbox("Resultado de la llamada", RESULTADOS, key="res_plan")
                        estado_final, razon = "", ""
                        if resultado == "Sí contestó":
                            estado_final = st.selectbox("Estado final", ESTADOS_FINALES_ALIADOS, key="estf_plan")
                            razon = st.selectbox("Razón", RAZONES, key="razon_plan")
                        area_idx = AREA_ALIADO_OPCIONES.index(fila.area_aliado) if fila.area_aliado in AREA_ALIADO_OPCIONES else 0
                        area_sel = st.selectbox("Área del aliado", AREA_ALIADO_OPCIONES, index=area_idx, key="area_plan")
                        nota = st.text_area("Observación", key="nota_plan")
                        enviar = st.form_submit_button("Guardar gestión")
                    if enviar:
                        if resultado == "Sí contestó" and not estado_final:
                            st.error("Selecciona el estado final.")
                        else:
                            razon_final = "" if razon == "—" else razon
                            fila_ctx = dict(fila); fila_ctx["analista"] = nombre
                            cambios, log = procesar_gestion_planeacion(fila_ctx, resultado, estado_final, razon_final, nota)
                            if area_sel != "—":
                                cambios["area_aliado"] = area_sel
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
                vista[["identificacion", "nombre", "celular", "zona", "analista", "area_aliado", "estado_planeacion", "categoria", "intentos_llamada", "proxima_gestion"]],
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
                        cambios, log = procesar_validacion_requerimiento(fila, uso == "Sí", nota)
                        actualizar_fila_por_id("REQUERIMIENTOS_ALIADOS", "telefono", fila.telefono, cambios)
                        _actualizar_roster_local("req_roster", "telefono", fila.telefono, cambios)
                        agregar_filas("REQUERIMIENTOS_GESTIONES", [[_safe_str(log.get(c, "")) for c in COLS_REQUERIMIENTOS_GESTIONES]])
                        _agregar_local("req_hist", log, COLS_REQUERIMIENTOS_GESTIONES)
                        st.success("Guardado.")
                        st.rerun()
                else:
                    with st.form("form_gestion_requerimiento"):
                        resultado = st.selectbox("Resultado de la llamada", RESULTADOS, key="res_req")
                        estado_final, razon = "", ""
                        if resultado == "Sí contestó":
                            estado_final = st.selectbox("Estado final", ESTADOS_FINALES_REQ, key="estf_req")
                            razon = st.selectbox("Razón", RAZONES_REQ, key="razon_req")
                        area_idx = AREA_ALIADO_OPCIONES.index(fila.area_aliado) if fila.area_aliado in AREA_ALIADO_OPCIONES else 0
                        area_sel = st.selectbox("Área del aliado", AREA_ALIADO_OPCIONES, index=area_idx, key="area_req")
                        nota = st.text_area("Observación", key="nota_req")
                        enviar = st.form_submit_button("Guardar gestión")
                    if enviar:
                        if resultado == "Sí contestó" and not estado_final:
                            st.error("Selecciona el estado final.")
                        else:
                            razon_final = "" if razon == "—" else razon
                            cambios, log = procesar_gestion_requerimiento(fila, resultado, estado_final, razon_final, nota)
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
