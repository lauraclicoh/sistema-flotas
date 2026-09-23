import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, date
import gspread
from google.oauth2.service_account import Credentials
import plotly.express as px
import time
import re
import unicodedata
from zoneinfo import ZoneInfo
st.set_page_config(layout="wide", page_title="🚚 Gestión Aliados Programación", page_icon="🚚")
TZ_COL = ZoneInfo("America/Bogota")
def now_col():
    return datetime.now(TZ_COL).replace(tzinfo=None)
ANALISTAS = {
    "Deisy Liliana Garcia":  "dgarcia@clicoh.com",
    "Erica Tatiana Garzon":  "etgarzon@clicoh.com",
    "Dayan Stefany Suarez":  "dsuarez@clicoh.com",
    "Carlos Andres Loaiza":  "cloaiza@clicoh.com",
    "Diana Paola Rueda Jimenez":  "drueda@clicoh.com",
}
NOMBRES_ANALISTAS = list(ANALISTAS.keys())
RESULTADOS       = ["Apagado","Fuera de servicio","No contestó","Número errado","Sí contestó"]
ESTADOS_FINALES  = [
    "Aliado Rechaza la oferta",
    "Aliado Fleet/Delivery no acepta hub",
    "Interesado llega a cargue/ Programado",
    "Interesado esporádico",
    "Empleado",
    "Point",
]
RAZONES = [
    "Interesado carga hoy/ reserva",
    "No le interesa / cuestiones personales",
    "No tiene Vh / Vh dañado",
    "Peso / Volumen / recorrido",
    "Tarifa",
    "Tiene trabajo fijo",
    "Fuera de la ciudad",
    "Aliado no carga en HUB",
    "Ocasional",
    "Empleado",
    "Point",
]
NO_RESPONDEN      = ["Apagado","Fuera de servicio","No contestó","Número errado"]
NO_VOLVER_ESTADOS = ["Aliado Rechaza la oferta","Empleado","Point"]
NO_VOLVER_RAZONES = ["No le interesa / cuestiones personales"]
COLS_CRM = ["intentos","ultimo_resultado","ultimo_estado","ultima_razon","fecha_gestion","proxima_gestion"]

# =========================================================================
# CUMPLIMIENTO DE CARGUE: ¿los aliados que dijeron que iban a cargar, cargaron?
# Se alimenta con la exportación del tablero de Looker "Servicio - Operaciones
# Latam / Cumplimiento Diario Aliados" (una fila por hoja de ruta), que se va
# acumulando en la hoja CARGUES_REALES del spreadsheet "GestionAliados".
# Mismo patrón que en Planeación de Aliados (app_implementacion.py), adaptado
# a las columnas de HISTORICO/BASE de Programación: aquí no existe un paso de
# "validación" separado, así que toda fila de HISTORICO cuenta como llamada,
# y el estado de compromiso es "Interesado llega a cargue/ Programado"
# (el que ya usa ESTADOS_FINALES de este archivo).
# ASUNCIÓN: la hoja CARGUES_REALES debe existir como pestaña en el spreadsheet
# "GestionAliados"; si no existe, créala manualmente con estos encabezados en
# la fila 1: cedula, nombre, ciudad, hoja_ruta, fecha_cargue, estado_hr,
# paquetes, entregados, gestionados, fecha_subida.
# =========================================================================
HOJA_CARGUES = "CARGUES_REALES"
COLS_CARGUES_REALES = ["cedula", "nombre", "ciudad", "hoja_ruta", "fecha_cargue", "estado_hr",
                        "paquetes", "entregados", "gestionados", "fecha_subida"]
# Solo cuentan como cargue las hojas de ruta que el aliado sí sacó a la calle.
# "Disponible" (nadie la tomó) y "Tomado" (asignada pero sin salir) no cuentan.
ESTADOS_HR_CARGUE = {"abierto", "cerrado"}
ESTADO_COMPROMISO = "Interesado llega a cargue/ Programado"
RAZONES_VALIDACION = {"Interesado carga hoy/ reserva"}
DIAS_SEMANA = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
ANALISTA_CUMPLIMIENTO_DEFECTO = "Diana Paola Rueda Jimenez"
# Una única fuente de verdad para el acceso de coordinación. Configure
# `coordinator_password` en secrets.toml; ambos módulos la consumen aquí.
COORDINATOR_PASSWORD = st.secrets.get("coordinator_password", "clicoh")
def clasificar_estado_aliado(valor) -> str:
    """
    Clasifica el valor de la columna 'Estado' de BASE en Activo/Inactivo.
    ASUNCIÓN 1: 'Validación finalizada' cuenta como Activo (aliado operable).
    ASUNCIÓN 2: si la celda está vacía, se asume Activo (no hay dato de baja).
    Si alguna de estas dos asunciones no aplica en tu operación, dímelo y
    ajusto la función — están aisladas aquí a propósito para que sea un
    cambio de una sola línea.
    """
    v = str(valor).strip().casefold()
    if v in ("", "nan", "none"):
        return "🟢 Activo"
    if "inactivo" in v:
        return "🔴 Inactivo"
    return "🟢 Activo"  # cubre "activo", "validación finalizada" y cualquier otro valor
def anotar_estado_aliado(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega la columna 'estado_aliado' (🟢 Activo / 🔴 Inactivo) SIN eliminar
    filas. Antes, esta lógica vivía en excluir_aliados_inactivos() y borraba
    al aliado inactivo de la base operativa; ahora se conserva para poder
    gestionarlo igual, y solo se muestra su estado.
    """
    if df is None or df.empty:
        return df
    df = df.copy()
    if "estado" in df.columns:
        df["estado_aliado"] = df["estado"].apply(clasificar_estado_aliado)
    else:
        df["estado_aliado"] = "🟢 Activo"
    return df
def excluir_aliados_inactivos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Alias de compatibilidad: se mantiene el nombre para no tocar ninguno de
    los puntos del código que ya la invocan (_get_base, filtrar_pool, la
    carga de base, el cruce incremental). El comportamiento cambió por
    pedido explícito: ya NO excluye inactivos, solo los deja marcados.
    """
    return anotar_estado_aliado(df)
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
@st.cache_resource
def conectar_sheets():
    try:
        creds = Credentials.from_service_account_info(
            dict(st.secrets["gcp_service_account"]), scopes=SCOPES
        )
        return gspread.authorize(creds).open("GestionAliados")
    except Exception as e:
        st.error(f"Error conexión Sheets: {e}")
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
        if isinstance(val, (np.integer,)): return str(int(val))
        if isinstance(val, (np.floating,)): return str(float(val))
        if isinstance(val, np.bool_): return str(bool(val))
    except ImportError:
        pass
    try:
        return str(val)
    except Exception:
        return ""
def _df_to_rows(df: pd.DataFrame) -> list:
    return [[_safe_str(v) for v in row] for row in df.values]
def leer_hoja(nombre_hoja, esperado_cols=None):
    try:
        sh = conectar_sheets()
        if sh is None:
            return pd.DataFrame(columns=esperado_cols or [])
        ws   = sh.worksheet(nombre_hoja)
        vals = ws.get_all_values()
        if not vals:
            return pd.DataFrame(columns=esperado_cols or [])
        headers = vals[0]
        clean, seen = [], {}
        for h in headers:
            h = str(h).strip()
            if not h or h.lower() == "none":
                h = f"_x{len(clean)}"
            if h in seen:
                seen[h] += 1; h = f"{h}_{seen[h]}"
            else:
                seen[h] = 0
            clean.append(h)
        if len(vals) < 2:
            return pd.DataFrame(columns=clean)
        n    = len(clean)
        rows = [r + [""]*(n-len(r)) if len(r)<n else r[:n] for r in vals[1:]]
        df   = pd.DataFrame(rows, columns=clean)
        df   = df[[c for c in df.columns if not c.startswith("_x")]]
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
        if sh is None: return
        ws = sh.worksheet(nombre_hoja)
        ws.clear()
        if not df.empty:
            cols_subir = [c for c in df.columns if not c.startswith("_")]
            df_clean   = df[cols_subir].copy()
            df_clean   = df_clean.loc[:, ~df_clean.columns.duplicated()]
            data = [df_clean.columns.tolist()] + _df_to_rows(df_clean)
            ws.update(data)
    except Exception as e:
        st.error(f"Error reemplazando {nombre_hoja}: {e}")
def _norm_vh(v):
    v = str(v).lower()
    if any(k in v for k in ["carry","largenvan","large van","small van","van"]): return "Carry / Van"
    if "moto" in v: return "Moto"
    if any(k in v for k in ["camion","camión","truck","npr"]): return "Camión"
    return str(v).title()
def _prio(dias):
    try: dias = int(float(str(dias)))
    except: return "🟢 BAJA"
    if dias > 5: return "🔴 ALTA"
    if dias > 1: return "🟡 MEDIA"
    return "🟢 BAJA"
def _parse_fecha_cargue(serie: pd.Series) -> pd.Series:
    resultados = []
    for raw in serie:
        val = str(raw).strip()
        if not val or val.lower() in ("sin fecha", "nan", "none", ""):
            resultados.append(pd.NaT)
            continue
        if val.isdigit() and len(val) == 8:
            try:
                resultados.append(datetime.strptime(val, "%Y%m%d"))
                continue
            except ValueError:
                pass
        parsed = pd.to_datetime(val, dayfirst=True, errors="coerce")
        resultados.append(parsed)
    return pd.Series(resultados, index=serie.index)
def _get_base():
    if "base_df" not in st.session_state or st.session_state.get("base_stale", True):
        df = leer_hoja("BASE")
        if df.empty:
            st.session_state["base_df"] = None
        else:
            df.columns = df.columns.str.strip().str.lower()
            if "identificacion" not in df.columns:
                for a in ["id_aliado","id","cedula","documento"]:
                    if a in df.columns: df["identificacion"] = df[a]; break
            if "identificacion" not in df.columns:
                st.session_state["base_df"] = None
                st.session_state["base_stale"] = False
                return None
            if "celular" not in df.columns:
                for a in ["telefono","tel","phone"]:
                    if a in df.columns: df["celular"] = df[a]; break
            if "zona" not in df.columns and "municipio" in df.columns:
                df["zona"] = df["municipio"]
            if "zona" not in df.columns:
                df["zona"] = "Sin zona"
            # El filtro se aplica inmediatamente después de consultar BASE,
            # antes de que el aliado pueda llegar a pools, búsquedas o KPI.
            df = excluir_aliados_inactivos(df)
            df["vehiculo_norm"] = df["vehiculo"].apply(_norm_vh) if "vehiculo" in df.columns else "Sin vehículo"
            df["dias"] = 0
            col_f = next((c for c in ["fecha_ultimo_cargue","fecha ultimo cargue","fechaultimocargue"]
                          if c in df.columns), None)
            if col_f:
                _serie = df[col_f]
                if isinstance(_serie, pd.DataFrame): _serie = _serie.iloc[:, 0]
                df["_fc"] = _parse_fecha_cargue(_serie.astype(str))
                df["dias"] = (now_col() - df["_fc"]).dt.days.fillna(0).astype(int)
            elif "dias_desde_ult_srv." in df.columns:
                df["dias"] = pd.to_numeric(df["dias_desde_ult_srv."], errors="coerce").fillna(0).astype(int)
            for col in COLS_CRM:
                if col not in df.columns: df[col] = 0 if col=="intentos" else ""
            df["intentos"] = pd.to_numeric(df["intentos"], errors="coerce").fillna(0).astype(int)
            df = df.loc[:, ~df.columns.duplicated()]
            st.session_state["base_df"] = df
        st.session_state["base_stale"] = False
    return st.session_state.get("base_df")
def _invalidar_base():
    st.session_state["base_stale"] = True
def _get_hist(force_reload=False):
    ahora  = time.time()
    ultima = st.session_state.get("hist_last_load", 0)
    if force_reload or "hist_df" not in st.session_state or (ahora - ultima) > 30:
        cols = ["fecha","analista","identificacion","resultado","estado","razon","obs"]
        df   = leer_hoja("HISTORICO", cols)
        if df.empty:
            df = pd.DataFrame(columns=cols)
        else:
            for c in cols:
                if c not in df.columns: df[c] = ""
            df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
            df = df.dropna(subset=["fecha"])
        st.session_state["hist_df"]        = df
        st.session_state["hist_last_load"] = ahora
    return st.session_state["hist_df"]
def _hist_agregar_local(row_dict):
    nuevo = pd.DataFrame([{
        "fecha":          pd.to_datetime(row_dict.get("fecha")),
        "analista":       _safe_str(row_dict.get("analista")),
        "identificacion": _safe_str(row_dict.get("identificacion")),
        "resultado":      _safe_str(row_dict.get("resultado")),
        "estado":         _safe_str(row_dict.get("estado")),
        "razon":          _safe_str(row_dict.get("razon")),
        "obs":            _safe_str(row_dict.get("obs")),
    }])
    if "hist_df" in st.session_state and isinstance(st.session_state["hist_df"], pd.DataFrame):
        st.session_state["hist_df"] = pd.concat(
            [st.session_state["hist_df"], nuevo], ignore_index=True
        )
    else:
        st.session_state["hist_df"] = nuevo
    st.session_state["hist_last_load"] = time.time()
def calcular_proxima(resultado, estado, razon, intentos):
    hoy    = now_col()
    estado = str(estado or "")
    razon  = str(razon or "")
    if estado in NO_VOLVER_ESTADOS or razon in NO_VOLVER_RAZONES:
        return "NO_VOLVER"
    if resultado in NO_RESPONDEN:
        if intentos >= 15:
            return "NO_VOLVER"
        if intentos >= 10:
            return hoy + timedelta(days=30)
        return hoy + timedelta(days=5)
    if estado in ["Interesado llega a cargue","Aliado Fleet/Delivery no acepta hub"]:
        return hoy + timedelta(days=5)
    return hoy + timedelta(days=3)
def filtrar_pool(df):
    df = excluir_aliados_inactivos(df)
    if df is None:
        return df
    if "proxima_gestion" not in df.columns: return df
    df = df.copy()
    df = df[df["proxima_gestion"].astype(str).str.upper() != "NO_VOLVER"]
    def disponible(v):
        v = str(v).strip()
        if v in ("","nan","None","0"): return True
        f = pd.to_datetime(v, errors="coerce")
        return pd.isna(f) or f <= now_col()
    return df[df["proxima_gestion"].apply(disponible)]
def _sincronizar_rechazado(sh, identificacion, fila_base: dict):
    try:
        ws = sh.worksheet("RECHAZADO")
        vals = ws.get_all_values()
        cols_rec = ["identificacion","mensajero","celular","zona","vehiculo",
                    "ultimo_resultado","ultimo_estado","ultima_razon","intentos","fecha_gestion"]
        if not vals or len(vals) < 1:
            ws.append_rows([cols_rec, [_safe_str(fila_base.get(c,"")) for c in cols_rec]],
                           value_input_option="USER_ENTERED")
            return
        headers = vals[0]
        for c in cols_rec:
            if c not in headers:
                headers.append(c)
                ws.update_cell(1, len(headers), c)
        try:
            col_id_idx = headers.index("identificacion") + 1
        except ValueError:
            return
        id_vals = ws.col_values(col_id_idx)
        nueva_fila = [_safe_str(fila_base.get(c,"")) for c in headers]
        if str(identificacion) in id_vals:
            fila_idx = id_vals.index(str(identificacion)) + 1
            ws.update(f"A{fila_idx}", [nueva_fila])
        else:
            ws.append_rows([nueva_fila], value_input_option="USER_ENTERED")
    except Exception as e:
        st.warning(f"No se pudo actualizar RECHAZADO: {e}")
def _sincronizar_pausado(sh, identificacion, fila_base: dict, es_pausa: bool):
    try:
        ws = sh.worksheet("PAUSADO")
        vals = ws.get_all_values()
        cols_pau = ["identificacion","mensajero","celular","zona","vehiculo",
                    "ultimo_resultado","ultimo_estado","ultima_razon",
                    "intentos","proxima_gestion","fecha_gestion"]
        if not vals or len(vals) < 1:
            if es_pausa:
                ws.append_rows([cols_pau, [_safe_str(fila_base.get(c,"")) for c in cols_pau]],
                               value_input_option="USER_ENTERED")
            return
        headers = vals[0]
        for c in cols_pau:
            if c not in headers:
                headers.append(c)
                ws.update_cell(1, len(headers), c)
        try:
            col_id_idx = headers.index("identificacion") + 1
        except ValueError:
            return
        id_vals = ws.col_values(col_id_idx)
        if es_pausa:
            nueva_fila = [_safe_str(fila_base.get(c,"")) for c in headers]
            if str(identificacion) in id_vals:
                fila_idx = id_vals.index(str(identificacion)) + 1
                ws.update(f"A{fila_idx}", [nueva_fila])
            else:
                ws.append_rows([nueva_fila], value_input_option="USER_ENTERED")
        else:
            if str(identificacion) in id_vals:
                fila_idx = id_vals.index(str(identificacion)) + 1
                ws.delete_rows(fila_idx)
    except Exception as e:
        st.warning(f"No se pudo actualizar PAUSADO: {e}")
def guardar_gestion(row):
    fila = [_safe_str(row.get(k,"")) for k in
            ["fecha","analista","identificacion","resultado","estado","razon","obs"]]
    agregar_filas("HISTORICO", [fila])
    _hist_agregar_local(row)
def actualizar_base_crm(identificacion, resultado, estado, razon):
    try:
        sh = conectar_sheets()
        if sh is None: return
        ws = sh.worksheet("BASE")
        headers = ws.row_values(1)
        if "identificacion" not in headers: return
        col_id_idx  = headers.index("identificacion") + 1
        col_id_vals = ws.col_values(col_id_idx)
        try:
            fila_idx = col_id_vals.index(str(identificacion)) + 1
        except ValueError:
            return
        intentos_n = 1
        if "intentos" in headers:
            col_int_idx  = headers.index("intentos") + 1
            val_intentos = ws.cell(fila_idx, col_int_idx).value
            try: intentos_n = int(str(val_intentos or "0")) + 1
            except: intentos_n = 1
        proxima = calcular_proxima(resultado, estado, razon, intentos_n)
        crm_vals = {
            "ultimo_resultado": _safe_str(resultado),
            "ultimo_estado":    _safe_str(estado),
            "ultima_razon":     _safe_str(razon),
            "fecha_gestion":    _safe_str(now_col()),
            "intentos":         str(intentos_n),
            "proxima_gestion":  _safe_str(proxima),
        }
        updates = []
        for col_name, val in crm_vals.items():
            if col_name in headers:
                col_idx = headers.index(col_name) + 1
                celda   = gspread.utils.rowcol_to_a1(fila_idx, col_idx)
                updates.append({"range": celda, "values": [[val]]})
        if updates:
            ws.batch_update(updates)
        _invalidar_base()
        fila_base = {
            "identificacion":   str(identificacion),
            "mensajero":        "",
            "celular":          "",
            "zona":             "",
            "vehiculo":         "",
            "ultimo_resultado": _safe_str(resultado),
            "ultimo_estado":    _safe_str(estado),
            "ultima_razon":     _safe_str(razon),
            "intentos":         str(intentos_n),
            "proxima_gestion":  _safe_str(proxima),
            "fecha_gestion":    _safe_str(now_col()),
        }
        proxima_str = _safe_str(proxima).upper()
        es_no_volver = proxima_str == "NO_VOLVER"
        es_pausa = False
        if not es_no_volver:
            f_prox = pd.to_datetime(_safe_str(proxima), errors="coerce")
            es_pausa = not pd.isna(f_prox) and f_prox > now_col()
        if es_no_volver:
            _sincronizar_rechazado(sh, identificacion, fila_base)
            _sincronizar_pausado(sh, identificacion, fila_base, es_pausa=False)
        elif es_pausa:
            _sincronizar_pausado(sh, identificacion, fila_base, es_pausa=True)
        else:
            _sincronizar_pausado(sh, identificacion, fila_base, es_pausa=False)
    except Exception as e:
        st.warning(f"CRM no actualizado en BASE: {e}")
def _celda_a_str(x):
    if x is None: return ""
    try:
        if pd.isna(x): return ""
    except Exception: pass
    if hasattr(x, "strftime"):
        return x.strftime("%Y-%m-%d")
    try:
        import numpy as np
        if isinstance(x, np.integer): return str(int(x))
        if isinstance(x, np.floating):
            if pd.isna(x): return ""
            return str(int(x)) if x == int(x) else str(x)
    except Exception: pass
    return str(x)
def _df_safe_str(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    for col in df.columns:
        out[col] = df[col].map(_celda_a_str)
    return out
def procesar_incremental(df_nuevo):
    base_actual = leer_hoja("BASE")
    df_nuevo = df_nuevo.copy()
    df_nuevo.columns = (df_nuevo.columns.str.strip().str.lower()
                        .str.replace(r"\s+", "_", regex=True))
    df_nuevo = df_nuevo[[c for c in df_nuevo.columns
                          if c and not c.startswith("unnamed") and c != "_"]]
    df_nuevo = df_nuevo.loc[:, ~df_nuevo.columns.duplicated()]
    ALIAS_ID = ["identificacion", "id_aliado", "id aliado", "id", "cedula",
                "documento", "nro_identificacion", "numero_identificacion"]
    col_id = next((a for a in ALIAS_ID if a in df_nuevo.columns), None)
    if not col_id:
        st.error(f"No se encontró columna de identificación. Columnas detectadas: {list(df_nuevo.columns)}")
        return 0, 0
    df_nuevo = df_nuevo.rename(columns={col_id: "identificacion"})
    # Alias de columnas: permite subir el archivo con los encabezados que usa
    # Lau (Nombre, Número telefónico, Vehículo, Rutas, Estado Implementación...)
    # y que se acomoden solos a los nombres internos de BASE.
    ALIAS_COLUMNAS_BASE = {
        "nombre": "mensajero",
        "numero_telefonico": "celular",
        "número_telefónico": "celular",
        "estado_implementacion": "estado_pipeline",
        "estado_implementación": "estado_pipeline",
    }
    df_nuevo = df_nuevo.rename(columns={k: v for k, v in ALIAS_COLUMNAS_BASE.items()
                                         if k in df_nuevo.columns and v not in df_nuevo.columns})
    df_nuevo = _df_safe_str(df_nuevo)
    df_nuevo["identificacion"] = df_nuevo["identificacion"].str.strip()
    df_nuevo = df_nuevo.fillna("")
    # Si el archivo trae la misma cédula en más de una fila, nos quedamos con
    # la última (la más reciente) para evitar el error de pandas
    # "Update not allowed with duplicate indexes on other".
    dup_en_archivo = int(df_nuevo["identificacion"].duplicated().sum())
    if dup_en_archivo > 0:
        st.warning(f"⚠️ El archivo tenía {dup_en_archivo} cédula(s) repetida(s); se conservó el registro más reciente de cada una.")
        df_nuevo = df_nuevo.drop_duplicates(subset="identificacion", keep="last")
    if base_actual.empty:
        df_nuevo = excluir_aliados_inactivos(df_nuevo)
        for col in COLS_CRM:
            df_nuevo[col] = "0" if col == "intentos" else ""
        reemplazar_hoja("BASE", df_nuevo)
        _invalidar_base()
        return len(df_nuevo), 0
    base_actual.columns = (base_actual.columns.str.strip().str.lower()
                           .str.replace(r"\s+", "_", regex=True))
    base_actual = base_actual.loc[:, ~base_actual.columns.duplicated()]
    base_actual = _df_safe_str(base_actual)
    if "identificacion" not in base_actual.columns:
        col_id_b = next((a for a in ALIAS_ID if a in base_actual.columns), None)
        if col_id_b:
            base_actual = base_actual.rename(columns={col_id_b: "identificacion"})
        else:
            st.error("La BASE guardada no tiene columna de identificación.")
            return 0, 0
    base_actual["identificacion"] = base_actual["identificacion"].str.strip()
    base_actual = base_actual.fillna("")
    dup_en_base = int(base_actual["identificacion"].duplicated().sum())
    if dup_en_base > 0:
        st.warning(f"⚠️ La base guardada tenía {dup_en_base} cédula(s) repetida(s); se conservó el registro más reciente de cada una.")
        base_actual = base_actual.drop_duplicates(subset="identificacion", keep="last")
    ids_viejos = set(base_actual["identificacion"].unique())
    nuevos = df_nuevo[~df_nuevo["identificacion"].isin(ids_viejos)].copy()
    for col in COLS_CRM:
        nuevos[col] = "0" if col == "intentos" else ""
    cols_operativas = [c for c in df_nuevo.columns if c not in COLS_CRM and c != "identificacion"]
    existentes_mask = df_nuevo["identificacion"].isin(ids_viejos)
    cols_sel = ["identificacion"] + cols_operativas
    existentes_datos = (df_nuevo[existentes_mask][cols_sel]
                        .loc[:, ~pd.Index(cols_sel).duplicated()]
                        .set_index("identificacion"))
    base_idx = base_actual.set_index("identificacion")
    for col in cols_operativas:
        if col not in existentes_datos.columns:
            continue
        col_data = existentes_datos[[col]]
        if col in base_idx.columns:
            base_idx.update(col_data)
        else:
            base_idx = base_idx.join(col_data, how="left")
    base_actualizada = base_idx.reset_index()
    base_final = pd.concat([base_actualizada, nuevos], ignore_index=True)
    base_final = base_final.fillna("")
    base_final = base_final.loc[:, ~base_final.columns.duplicated()]
    # También se excluyen de la persistencia durante la sincronización:
    # los nuevos inactivos no ingresan y los que cambiaron a Inactivo salen.
    base_final = excluir_aliados_inactivos(base_final)
    reemplazar_hoja("BASE", base_final)
    _invalidar_base()
    return len(nuevos), len(existentes_datos)
def leer_config(analista):
    if "config_df" not in st.session_state:
        st.session_state["config_df"] = leer_hoja("CONFIG", ["analista","modo","zona","vehiculo"])
    df = st.session_state["config_df"]
    if df.empty or "analista" not in df.columns: return "Analista decide", None, None
    fila = df[df["analista"]==analista]
    if not fila.empty:
        r = fila.iloc[-1]; return r.get("modo","Analista decide"), r.get("zona"), r.get("vehiculo")
    fila = df[df["analista"]=="TODOS"]
    if not fila.empty:
        r = fila.iloc[-1]; return r.get("modo","Analista decide"), r.get("zona"), r.get("vehiculo")
    return "Analista decide", None, None
def cargar_reparto():
    if "reparto_df" not in st.session_state or st.session_state.get("reparto_stale", True):
        st.session_state["reparto_df"] = leer_hoja("REPARTO",["fecha","analista","identificacion"])
        st.session_state["reparto_stale"] = False
    return st.session_state["reparto_df"]
def guardar_reparto(df):
    reemplazar_hoja("REPARTO", df)
    st.session_state["reparto_df"] = df
    st.session_state["reparto_stale"] = False

# =========================================================================
# CUMPLIMIENTO DE CARGUE — utilidades
# (portado desde app_implementacion.py, adaptado a las columnas de
# HISTORICO/BASE de Programación: "estado" en vez de "estado_final")
# =========================================================================

def _normalizar_tel(valor):
    """
    Deja solo dígitos y quita el '.0' que Excel pudo haber dejado en un
    teléfono o cédula. Se usa para cruzar identificaciones entre BASE,
    HISTORICO y la exportación de Looker aunque el formato no sea idéntico.
    """
    v = str(valor).strip()
    if v.endswith(".0"):
        v = v[:-2]
    return "".join(ch for ch in v if ch.isdigit())

def _norm_encabezado(c):
    """'Hoja de Ruta', 'hoja_ruta' y 'HOJA DE RUTA ' quedan igual: sin tildes, minúsculas, espacios simples."""
    s = "".join(ch for ch in unicodedata.normalize("NFD", str(c)) if unicodedata.category(ch) != "Mn")
    return " ".join(s.lower().replace("_", " ").split())

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

_MESES_ES = {"ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6, "jul": 7, "ago": 8,
             "sep": 9, "set": 9, "oct": 10, "nov": 11, "dic": 12}

def _parse_fecha_cargue_looker(serie: pd.Series) -> pd.Series:
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
        if m and m.group(1) in _MESES_ES:
            out.append(pd.Timestamp(int(m.group(3)), _MESES_ES[m.group(1)], int(m.group(2))))
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

# Columnas de la exportación de Looker (ya normalizadas con _norm_encabezado).
# Las demás columnas del archivo se ignoran.
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

def _leer_archivo_subido(archivo):
    return pd.read_csv(archivo) if archivo.name.lower().endswith("csv") else pd.read_excel(archivo)

def _get_cargues(forzar=False):
    """Mismo patrón de caché que _get_base(): vive en session_state y solo se relee de Sheets al invalidar o forzar."""
    if forzar or "cargues_df" not in st.session_state or st.session_state.get("cargues_stale", True):
        df = leer_hoja(HOJA_CARGUES, COLS_CARGUES_REALES)
        for c in COLS_CARGUES_REALES:
            if c not in df.columns:
                df[c] = ""
        st.session_state["cargues_df"] = df[COLS_CARGUES_REALES].fillna("")
        st.session_state["cargues_stale"] = False
    return st.session_state["cargues_df"]

def _invalidar_cargues():
    st.session_state["cargues_stale"] = True

def cargar_incremental_cargues(archivo):
    """
    Suma a CARGUES_REALES la exportación de Looker tal como sale del tablero.
    La llave es la hoja de ruta (HJRT ID): si ya existía, se reemplaza con lo
    que trae el archivo nuevo, así se actualizan estado, entregados y
    gestionados sin duplicar filas.
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
        "fecha": _parse_fecha_cargue_looker(df["fecha_cargue"]),
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

    existente = _get_cargues(forzar=True)
    llave_exist = existente.hoja_ruta.astype(str).str.strip()
    actualizados = int(d.hoja_ruta.isin(set(llave_exist)).sum())
    nuevos = len(d) - actualizados
    combinado = pd.concat([existente[~llave_exist.isin(set(d.hoja_ruta))], d], ignore_index=True)
    combinado = combinado.sort_values(["fecha_cargue", "cedula"]).astype(str).replace("nan", "")
    reemplazar_hoja(HOJA_CARGUES, combinado)
    st.session_state["cargues_df"] = combinado
    st.session_state["cargues_stale"] = False
    rango = (d.fecha_cargue.min(), d.fecha_cargue.max()) if len(d) else (None, None)
    return {"filas": len(df), "nuevos": nuevos, "actualizados": actualizados, "descartadas": descartadas,
            "desde": rango[0], "hasta": rango[1]}

def construir_cumplimiento(hist, cargues, aliados, analista, f1, f2):
    """
    hist: HISTORICO de Programación · cargues: CARGUES_REALES · aliados: tabla
    con identificacion/nombre/ciudad (de BASE). Devuelve KPIs, detalle por
    aliado comprometido, cruce por resultado de todos los que contestaron y
    tablas por día de la semana, por día y por ciudad.
    Regla: cuenta como cargue una hoja de ruta creada desde el DÍA SIGUIENTE
    a la llamada; si no la hay, es "No cargó".
    """
    h = hist.copy()
    h["identificacion"] = h.identificacion.map(_normalizar_tel)
    for c in ["analista", "resultado", "estado", "razon"]:
        h[c] = h[c].astype(str).str.strip().replace({"nan": "", "None": ""})
    h = h.sort_values("fecha")
    h = h[(h.fecha.dt.date >= f1) & (h.fecha.dt.date <= f2)]
    if analista != "Todas":
        h = h[h.analista == analista]
    llamadas = h.copy()  # en Programación toda fila de HISTORICO es una llamada (no hay paso de validación separado)

    # ---- cargues reales
    c = cargues.copy()
    c["cedula"] = c.cedula.map(_normalizar_tel)
    c["fecha"] = pd.to_datetime(c.fecha_cargue, errors="coerce")
    for col in ["paquetes", "entregados", "gestionados"]:
        c[col] = pd.to_numeric(c[col], errors="coerce").fillna(0)
    c = c.dropna(subset=["fecha"])
    estado_hr = c.estado_hr.astype(str).str.strip().str.lower()
    c = c[estado_hr.isin(ESTADOS_HR_CARGUE) | (estado_hr == "")]  # solo HR que sí salieron
    por_cedula = {k: g for k, g in c.groupby("cedula")}
    salto = timedelta(days=1)  # el cargue se cuenta desde el día siguiente a la llamada

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

    # Nombre y ciudad: se toma el último dato no vacío de BASE o de la exportación de Looker
    info = pd.concat([aliados, cargues.rename(columns={"cedula": "identificacion"})[["identificacion", "nombre", "ciudad"]]
                      .assign(ciudad=lambda t: t.ciudad.map(_ciudad_corta))], ignore_index=True)
    info["identificacion"] = info.identificacion.map(_normalizar_tel)
    info = info.replace({"": pd.NA, "Sin ciudad": pd.NA, "nan": pd.NA}).groupby("identificacion").last()
    nombre_de = lambda i: info.nombre.get(i) if pd.notna(info.nombre.get(i)) else ""
    ciudad_de = lambda i: info.ciudad.get(i) if pd.notna(info.ciudad.get(i)) else "Sin ciudad"

    # ---- aliados comprometidos (dijeron que iban a cargar)
    marca = h.estado.isin([ESTADO_COMPROMISO]) | h.razon.isin(RAZONES_VALIDACION)
    comp = h[marca].sort_values("fecha").groupby("identificacion", as_index=False).first()
    filas = []
    for r in comp.itertuples():
        x = cruce(r.identificacion, r.fecha)
        filas.append({
            "cedula": r.identificacion, "nombre": nombre_de(r.identificacion), "ciudad": ciudad_de(r.identificacion),
            "fecha_llamada": r.fecha.date(), "dia": DIAS_SEMANA[r.fecha.weekday()], "analista": r.analista,
            "estado_registrado": r.estado, "razon": r.razon, **x,
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
        ultima_resp = cont.groupby("identificacion").estado.last().replace("", "Sin estado")
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
        "interesados": int((llamadas.estado == ESTADO_COMPROMISO).sum()),
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


# ================================================================
# UI
# ================================================================
st.title("🚚 Gestión Aliados Programación")
with st.sidebar:
    st.markdown("### 👤 Acceso")
    perfil = st.selectbox("Soy:", ["— Selecciona —","Coordinador","Analista"])
    if perfil == "Coordinador":
        pwd = st.text_input("Contraseña", type="password")
        if pwd != COORDINATOR_PASSWORD:
            if pwd: st.error("Contraseña incorrecta")
            st.stop()
        st.success("✅ Coordinador")
        nombre = "Coordinador"
    elif perfil == "Analista":
        nombre = st.selectbox("¿Quién eres?", NOMBRES_ANALISTAS)
        st.success(f"✅ {nombre.split()[0]}")
    else:
        st.info("Selecciona tu perfil para continuar.")
        st.stop()
# ================================================================
# COORDINADOR PRINCIPAL
# ================================================================
if perfil == "Coordinador":
    base = _get_base()
    hist = _get_hist()
    tab1,tab2,tab3,tab4,tab5,tab6,tab7,tab8,tab9 = st.tabs([
        "📊 Hoy","📅 Histórico & KPIs","🔍 Buscar Aliado",
        "🔥 Estado CRM","📤 Cargar Base","🎯 Asignación","⚙️ Reglas","🗺️ Cobertura por Zona",
        "🚚 Cumplimiento de cargue",
    ])
    with tab1:
        st.subheader("Auditoría de Gestión")
        if st.button("🔄 Actualizar gestiones", key="btn_ref_hoy"):
            hist = _get_hist(force_reload=True)
            st.rerun()
        if hist.empty:
            st.info("Sin gestiones registradas aún.")
        else:
            hv = hist.dropna(subset=["fecha"])
            col_fd, col_bt = st.columns([3,1])
            with col_fd:
                valor_fecha = (now_col().date() if st.session_state.pop("_reset_fecha_aud", False) else now_col().date())
                fecha_aud = st.date_input("📅 Fecha a auditar", value=valor_fecha, max_value=now_col().date(), key="coord_fecha_aud")
            with col_bt:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("📅 Hoy"):
                    st.session_state["_reset_fecha_aud"] = True
                    st.rerun()
            hf = hv[hv["fecha"].dt.date==fecha_aud].sort_values("fecha",ascending=False)
            if hf.empty:
                st.warning(f"Sin gestiones el {fecha_aud.strftime('%d/%m/%Y')}.")
            else:
                label = "hoy" if fecha_aud==now_col().date() else fecha_aud.strftime("%d/%m/%Y")
                t=len(hf); sc=len(hf[hf["resultado"]=="Sí contestó"])
                it=len(hf[hf["estado"]=="Interesado llega a cargue"])
                rc=len(hf[hf["estado"]=="Aliado Rechaza la oferta"])
                nr=len(hf[hf["resultado"].isin(NO_RESPONDEN)])
                c1,c2,c3,c4,c5=st.columns(5)
                c1.metric("📞 Llamadas",t); c2.metric("✅ Contactados",sc)
                c3.metric("🚗 Interesados",it); c4.metric("❌ Rechazados",rc); c5.metric("📵 No resp.",nr)
                st.markdown("---")
                prod=hf.groupby("analista").size().reset_index(name="llamadas")
                ia=(hf[hf["estado"]=="Interesado llega a cargue"].groupby("analista").size().reset_index(name="interesados"))
                tp=prod.merge(ia,on="analista",how="left").fillna(0)
                tp["interesados"]=tp["interesados"].astype(int)
                tp["% efectividad"]=(tp["interesados"]/tp["llamadas"]*100).round(1)
                tp["🚦"]=tp.apply(lambda r:"🟢" if r["llamadas"]>=30 and r["interesados"]>=3 else("🟡" if r["llamadas"]>=15 else "🔴"),axis=1)
                st.dataframe(tp,use_container_width=True,hide_index=True)
                st.plotly_chart(px.bar(tp,x="analista",y="llamadas",color="% efectividad",title=f"Llamadas — {label}"),use_container_width=True)
                st.markdown("---")
                fa,fr,fb=st.columns(3)
                with fa: af=st.multiselect("Analista",NOMBRES_ANALISTAS,default=NOMBRES_ANALISTAS,key="af_c")
                with fr: rf=st.multiselect("Resultado",RESULTADOS,default=RESULTADOS,key="rf_c")
                with fb: bus=st.text_input("Buscar cédula","",key="bus_c")
                df_f=hf[hf["analista"].isin(af)&hf["resultado"].isin(rf)]
                if bus: df_f=df_f[df_f["identificacion"].astype(str).str.contains(bus,na=False)]
                df_show=df_f.copy(); df_show["Hora"]=df_show["fecha"].dt.strftime("%I:%M %p")
                st.dataframe(df_show[["Hora","analista","identificacion","resultado","estado","razon","obs"]].rename(
                    columns={"analista":"Analista","identificacion":"Cédula","resultado":"Resultado","estado":"Estado","razon":"Razón","obs":"Obs"}
                ),use_container_width=True,hide_index=True)
                st.download_button("📥 Descargar día (CSV)",df_f.to_csv(index=False).encode("utf-8"),f"gestion_{fecha_aud}.csv","text/csv")
    with tab2:
        st.subheader("Histórico & KPIs")
        if st.button("🔄 Actualizar historial", key="btn_ref_hist"):
            hist = _get_hist(force_reload=True); st.rerun()
        if hist.empty:
            st.info("Sin historial aún.")
        else:
            hv2=hist.dropna(subset=["fecha"])
            c1,c2=st.columns(2)
            with c1: f1=st.date_input("Desde",now_col().date()-timedelta(days=7),max_value=now_col().date(),key="h_f1")
            with c2: f2=st.date_input("Hasta",now_col().date(),max_value=now_col().date(),key="h_f2")
            d=hv2[(hv2["fecha"].dt.date>=f1)&(hv2["fecha"].dt.date<=f2)]
            if d.empty:
                st.warning("Sin registros en ese rango.")
            else:
                tot=len(d); sr=d[d["resultado"]=="Sí contestó"]; nr=d[d["resultado"].isin(NO_RESPONDEN)]
                g=len(sr); it=len(d[d["estado"]=="Interesado llega a cargue"]); rc=len(d[d["estado"]=="Aliado Rechaza la oferta"])
                c1,c2,c3,c4,c5=st.columns(5)
                c1.metric("📞 Total",tot); c2.metric("✅ Contactados",g)
                c3.metric("% No resp",f"{round(len(nr)/tot*100,1) if tot else 0}%")
                c4.metric("% Gestión",f"{round(g/tot*100,1) if tot else 0}%")
                c5.metric("% Interesados",f"{round(it/tot*100,1) if tot else 0}%")
                c6,c7=st.columns(2)
                c6.metric("% Rechazados",f"{round(rc/tot*100,1) if tot else 0}%")
                c7.metric("% Rechazo/contacto",f"{round(rc/g*100,1) if g else 0}%")
                st.markdown("---")
                emb=pd.DataFrame({"Etapa":["Llamados","Contactados","Interesados"],"Cantidad":[tot,g,it],
                                  "%":[100,round(g/tot*100,1) if tot else 0,round(it/tot*100,1) if tot else 0]})
                st.dataframe(emb,use_container_width=True)
                st.plotly_chart(px.funnel(emb,x="Cantidad",y="Etapa",title="Embudo"),use_container_width=True)
                st.markdown("---")
                de=[[e,len(sr[sr["estado"]==e]),round(len(sr[sr["estado"]==e])/g*100,1) if g else 0] for e in ESTADOS_FINALES]
                st.markdown("#### Estado final"); st.dataframe(pd.DataFrame(de,columns=["Estado","N","%"]),use_container_width=True)
                dr=[[r,len(sr[sr["razon"]==r]),round(len(sr[sr["razon"]==r])/g*100,1) if g else 0] for r in RAZONES]
                st.markdown("#### Razones"); st.dataframe(pd.DataFrame(dr,columns=["Razón","N","%"]),use_container_width=True)
                st.markdown("---"); st.markdown("#### KPIs por analista")
                pa=d.groupby("analista").size().reset_index(name="llamadas")
                ga=d[d["resultado"]=="Sí contestó"].groupby("analista").size().reset_index(name="gest")
                ia=d[d["estado"]=="Interesado llega a cargue"].groupby("analista").size().reset_index(name="inter")
                ra=d[d["estado"]=="Aliado Rechaza la oferta"].groupby("analista").size().reset_index(name="rech")
                na=d[d["resultado"].isin(NO_RESPONDEN)].groupby("analista").size().reset_index(name="noresp")
                ta=(pa.merge(ga,on="analista",how="left").merge(ia,on="analista",how="left")
                      .merge(ra,on="analista",how="left").merge(na,on="analista",how="left").fillna(0))
                for c in ["gest","inter","rech","noresp"]: ta[c]=ta[c].astype(int)
                ta["% gest"]=(ta["gest"]/ta["llamadas"]*100).round(1); ta["% inter"]=(ta["inter"]/ta["llamadas"]*100).round(1)
                ta["% rech"]=(ta["rech"]/ta["llamadas"]*100).round(1); ta["% noresp"]=(ta["noresp"]/ta["llamadas"]*100).round(1)
                st.dataframe(ta,use_container_width=True)
                st.plotly_chart(px.bar(ta,x="analista",y=["% gest","% inter"],barmode="group",title="KPIs por Analista"),use_container_width=True)
                tend=d.groupby(d["fecha"].dt.date).size().reset_index(name="llamadas"); tend.columns=["fecha","llamadas"]
                st.plotly_chart(px.line(tend,x="fecha",y="llamadas",title="Tendencia diaria",markers=True),use_container_width=True)
                d_show=d.copy(); d_show["Hora"]=d_show["fecha"].dt.strftime("%I:%M %p")
                if base is not None:
                    cols_extra=[c for c in ["identificacion","vehiculo","municipio","zona"] if c in base.columns]
                    base_mini=base[cols_extra].copy(); base_mini["identificacion"]=base_mini["identificacion"].astype(str)
                    d_show["identificacion"]=d_show["identificacion"].astype(str); d_show=d_show.merge(base_mini,on="identificacion",how="left")
                cols_hist=["Hora","analista","identificacion","resultado","estado","razon"]
                for extra in ["vehiculo","municipio","zona"]:
                    if extra in d_show.columns: cols_hist.append(extra)
                cols_hist.append("obs")
                st.dataframe(d_show[cols_hist].rename(columns={"analista":"Analista","identificacion":"Cédula","resultado":"Resultado",
                    "estado":"Estado","razon":"Razón","vehiculo":"Vehículo","municipio":"Ciudad","zona":"Zona","obs":"Obs"}),use_container_width=True,hide_index=True)
                st.download_button("📥 Descargar (CSV)",d_show.to_csv(index=False).encode("utf-8"),f"historico_{f1}_{f2}.csv","text/csv")
    with tab3:
        st.subheader("🔍 Buscar Aliado")
        modo_busq_c = st.radio("Buscar por", ["Cédula","Celular"], horizontal=True, key="modo_busq_coord")
        campo_busq_c = "identificacion" if modo_busq_c == "Cédula" else "celular"
        valor_busq_c = st.text_input(f"Ingresa el {modo_busq_c.lower()} del aliado", "", key="busq_valor_coord")
        if valor_busq_c.strip() and base is not None:
            if campo_busq_c not in base.columns:
                st.error(f"La base no tiene la columna '{campo_busq_c}'.")
            else:
                resultado_b = base[base[campo_busq_c].astype(str).str.strip()==valor_busq_c.strip()]
                if resultado_b.empty:
                    st.warning(f"No se encontró ningún aliado con {modo_busq_c.lower()} **{valor_busq_c}**.")
                else:
                    fila_b = resultado_b.iloc[0]
                    estado_badge = fila_b.get("estado_aliado", "🟢 Activo")
                    st.success(f"✅ Aliado encontrado — Estado: **{estado_badge}**")
                    cedula_encontrada = str(fila_b.get("identificacion",""))
                    cols_info = [c for c in ["identificacion","estado_aliado","estado_pipeline","rutas","mensajero","celular","correo","zona","municipio","vehiculo","categoria",
                                              "dias","intentos","ultimo_resultado","ultimo_estado","proxima_gestion"] if c in fila_b.index]
                    c1,c2 = st.columns(2); mitad = len(cols_info)//2
                    with c1:
                        for col in cols_info[:mitad]: st.metric(col.replace("_"," ").title(), str(fila_b[col]))
                    with c2:
                        for col in cols_info[mitad:]: st.metric(col.replace("_"," ").title(), str(fila_b[col]))
                    st.markdown("---"); st.markdown("#### 📋 Historial de gestiones")
                    hist_aliado = hist[hist["identificacion"].astype(str)==cedula_encontrada].copy()
                    if hist_aliado.empty:
                        st.info("Sin gestiones registradas para este aliado.")
                    else:
                        hist_aliado["Hora"] = hist_aliado["fecha"].dt.strftime("%d/%m/%Y %I:%M %p")
                        st.dataframe(hist_aliado[["Hora","analista","resultado","estado","razon","obs"]].rename(
                            columns={"analista":"Analista","resultado":"Resultado","estado":"Estado","razon":"Razón","obs":"Obs"}
                        ),use_container_width=True,hide_index=True)
        elif valor_busq_c.strip() and base is None:
            st.warning("Carga la base primero.")
    with tab4:
        if base is None:
            st.warning("Carga la base primero.")
        else:
            nv=base[base["proxima_gestion"].astype(str).str.upper()=="NO_VOLVER"]; disp=filtrar_pool(base)
            def en_pausa_fn(v):
                v=str(v).strip()
                if v in ("","nan","None","NO_VOLVER","0"): return False
                f=pd.to_datetime(v,errors="coerce")
                return not pd.isna(f) and f>now_col()
            paus=base[base["proxima_gestion"].apply(en_pausa_fn)]
            c1,c2,c3,c4=st.columns(4)
            c1.metric("📦 Total",len(base)); c2.metric("✅ Disponibles",len(disp))
            c3.metric("⏸ En pausa",len(paus)); c4.metric("🚫 Bloqueados",len(nv))
            if "estado_pipeline" in base.columns:
                fidelizados = base[base["estado_pipeline"]=="🏅 Fidelizado"]
                col_estado_aliado = base["estado_aliado"] if "estado_aliado" in base.columns else pd.Series([""]*len(base), index=base.index)
                inactivos_n = base[col_estado_aliado=="🔴 Inactivo"]
                c5,c6=st.columns(2)
                c5.metric("🏅 Fidelizados (≥20 rutas)",len(fidelizados))
                c6.metric("🔴 Marcados Inactivo",len(inactivos_n))
            st.markdown("---")
            disp2=disp.copy(); disp2["PRIORIDAD"]=disp2["dias"].apply(_prio)
            c1,c2,c3=st.columns(3)
            c1.metric("🔴 ALTA",len(disp2[disp2["PRIORIDAD"]=="🔴 ALTA"]))
            c2.metric("🟡 MEDIA",len(disp2[disp2["PRIORIDAD"]=="🟡 MEDIA"]))
            c3.metric("🟢 BAJA",len(disp2[disp2["PRIORIDAD"]=="🟢 BAJA"]))
            if not paus.empty:
                st.markdown("---"); st.markdown("#### ⏸ En pausa / recontacto programado")
                cp=[c for c in ["identificacion","mensajero","celular","zona","vehiculo","intentos","ultimo_resultado","ultimo_estado","proxima_gestion"] if c in paus.columns]
                st.dataframe(paus[cp].sort_values("proxima_gestion"),use_container_width=True)
            if not nv.empty:
                st.markdown("---"); st.markdown("#### 🚫 Bloqueados permanentemente")
                cnv=[c for c in ["identificacion","mensajero","celular","ultimo_estado","ultima_razon"] if c in nv.columns]
                st.dataframe(nv[cnv],use_container_width=True)
    with tab5:
        st.subheader("📤 Carga de Base")
        st.info("La base permanece en Google Sheets indefinidamente. Usa Incremental para conservar el historial CRM.")
        modo=st.radio("Modo de carga",["🔄 Incremental (recomendado) — conserva historial CRM","♻️ Reemplazar toda la base — borra historial CRM"])
        archivo=st.file_uploader("Excel (.xlsx)",type=["xlsx"])
        if archivo:
            try:
                df_s = pd.read_excel(archivo, engine="openpyxl")
                df_s = df_s[[c for c in df_s.columns if not str(c).startswith("Unnamed")]]
                df_s = _df_safe_str(df_s); df_s = df_s.fillna("")
                st.success(f"{len(df_s):,} registros leídos"); st.dataframe(df_s.head(5),use_container_width=True)
                if "Incremental" in modo:
                    if st.button("🚀 Ejecutar Cruce Incremental"):
                        with st.spinner("Procesando cruce..."):
                            nn,na=procesar_incremental(df_s)
                        st.success(f"✅ {nn} aliados nuevos añadidos · {na} aliados actualizados")
                else:
                    st.warning("⚠️ Esto borrará TODA la base actual incluyendo el historial CRM.")
                    confirmar=st.checkbox("Entiendo que se borrará todo el historial CRM")
                    if confirmar and st.button("♻️ Reemplazar base completa"):
                        with st.spinner("Subiendo..."):
                            base_operable = excluir_aliados_inactivos(df_s)  # ahora clasifica, no excluye
                            if base_operable is not None and "estado_aliado" in base_operable.columns:
                                n_inactivos = int((base_operable["estado_aliado"]=="🔴 Inactivo").sum())
                            else:
                                n_inactivos = 0
                            reemplazar_hoja("BASE", base_operable); _invalidar_base()
                        st.success(f"✅ {len(base_operable):,} aliados subidos · {n_inactivos:,} marcados como 🔴 Inactivo (se pueden gestionar igual).")
            except Exception as e:
                st.error(f"Error leyendo el archivo: {e}")
        if base is not None:
            st.info(f"Base activa en Google Sheets: **{len(base):,} aliados**")
    with tab6:
        if base is None:
            st.warning("Carga la base primero.")
        else:
            zonas=sorted(base["zona"].dropna().unique()); vhs=sorted(base["vehiculo_norm"].dropna().unique())
            modo_a=st.selectbox("Modo",["Analista decide","Asignación general (todos igual)","Asignación por analista"])
            dc=[]
            if modo_a=="Asignación general (todos igual)":
                zg=st.selectbox("Zona",zonas); vg=st.selectbox("Vehículo",vhs)
                dc=[{"analista":"TODOS","modo":modo_a,"zona":zg,"vehiculo":vg}]
            elif modo_a=="Asignación por analista":
                for a in NOMBRES_ANALISTAS:
                    st.markdown(f"**{a}**"); col1,col2=st.columns(2)
                    with col1: z=st.selectbox("Zona",zonas,key=f"z_{a}")
                    with col2: v=st.selectbox("Vehículo",vhs,key=f"v_{a}")
                    dc.append({"analista":a,"modo":modo_a,"zona":z,"vehiculo":v})
            else:
                dc=[{"analista":"TODOS","modo":"Analista decide","zona":"","vehiculo":""}]
            if st.button("💾 Guardar asignación"):
                reemplazar_hoja("CONFIG",pd.DataFrame(dc)); st.session_state["config_df"] = pd.DataFrame(dc); st.success("Guardado.")
            cf = st.session_state.get("config_df", pd.DataFrame())
            if not cf.empty:
                st.markdown("---"); st.markdown("##### Configuración activa:"); st.dataframe(cf,use_container_width=True)
    with tab7:
        st.subheader("⚙️ Reglas de recontacto automático")
        st.markdown("""
| Resultado / Estado | Acción | Días espera |
|---|---|---|
| No contestó | Recontacto | **5 días** |
| Apagado / Fuera de servicio | Recontacto | **5 días** |
| Número errado | Recontacto | **5 días** |
| 10 - 14 intentos sin contacto | Pausa larga | **30 días** |
| 15+ intentos sin contacto | ❌ Bloqueo permanente | Nunca |
| Interesado llega a cargue | Pausa | **5 días** |
| Fleet no acepta HUB | Pausa | **5 días** |
| Interesado esporádico | Recontacto | **3 días** |
| Aliado Rechaza la oferta | ❌ Bloqueo permanente | Nunca |
| Empleado / Point | ❌ Bloqueo permanente | Nunca |
| No le interesa | ❌ Bloqueo permanente | Nunca |
        """)
        st.info("Reglas automáticas: los aliados en pausa vuelven solos al cumplirse el tiempo.")
    with tab8:
        st.subheader("🗺️ Cobertura de Gestión por Zona")
        if base is None:
            st.warning("Carga la base primero.")
        elif hist.empty:
            st.warning("Aún no hay gestiones registradas.")
        else:
            col_f1, col_f2, col_f3 = st.columns(3)
            with col_f1: fz1 = st.date_input("Desde", now_col().date()-timedelta(days=7), max_value=now_col().date(), key="cob_f1")
            with col_f2: fz2 = st.date_input("Hasta", now_col().date(), max_value=now_col().date(), key="cob_f2")
            with col_f3:
                vhs_cob = ["Todos"] + sorted(base["vehiculo_norm"].dropna().unique().tolist())
                vh_filtro = st.selectbox("Vehículo", vhs_cob, key="cob_vh")
            hv_cob = hist.dropna(subset=["fecha"])
            hv_cob = hv_cob[(hv_cob["fecha"].dt.date >= fz1) & (hv_cob["fecha"].dt.date <= fz2)]
            gestionados_ids = set(hv_cob["identificacion"].astype(str).unique())
            base_cob = base.copy()
            if vh_filtro != "Todos": base_cob = base_cob[base_cob["vehiculo_norm"] == vh_filtro]
            resumen = []
            for zona in sorted(base_cob["zona"].dropna().unique()):
                aliados_zona  = base_cob[base_cob["zona"] == zona]; total = len(aliados_zona)
                gestionados   = len(aliados_zona[aliados_zona["identificacion"].astype(str).isin(gestionados_ids)])
                pendientes    = total - gestionados
                pct           = round(gestionados / total * 100, 1) if total > 0 else 0
                resumen.append({"Zona": zona,"Total aliados": total,"Gestionados": gestionados,"Pendientes": pendientes,"% Cobertura": pct})
            df_res = pd.DataFrame(resumen).sort_values("% Cobertura", ascending=False)
            tot_g=df_res["Total aliados"].sum(); gest_g=df_res["Gestionados"].sum()
            pend_g=df_res["Pendientes"].sum(); pct_g=round(gest_g/tot_g*100,1) if tot_g>0 else 0
            c1,c2,c3,c4=st.columns(4)
            c1.metric("📦 Total aliados",f"{tot_g:,}"); c2.metric("✅ Gestionados",f"{gest_g:,}")
            c3.metric("⏳ Pendientes",f"{pend_g:,}"); c4.metric("📊 Cobertura global",f"{pct_g}%")
            st.markdown("---"); st.markdown("#### Detalle por zona")
            df_display=df_res.copy(); df_display["Cobertura"]=df_display["% Cobertura"].apply(lambda x: f"{x}%")
            st.dataframe(df_display[["Zona","Total aliados","Gestionados","Pendientes","Cobertura"]],use_container_width=True,hide_index=True)
            st.markdown("---")
            fig_cob=px.bar(df_res,x="Zona",y=["Gestionados","Pendientes"],barmode="stack",
                           title=f"Cobertura por Zona · {fz1.strftime('%d/%m')} al {fz2.strftime('%d/%m/%Y')}",
                           color_discrete_map={"Gestionados":"#28a745","Pendientes":"#dc3545"},labels={"value":"Aliados","variable":"Estado"})
            fig_cob.update_layout(xaxis_tickangle=-45,legend_title_text=""); st.plotly_chart(fig_cob,use_container_width=True)
            fig_pct=px.bar(df_res.sort_values("% Cobertura"),x="% Cobertura",y="Zona",orientation="h",title="% Cobertura por Zona",
                           color="% Cobertura",color_continuous_scale=["#dc3545","#ffc107","#28a745"],range_color=[0,100])
            fig_pct.update_layout(coloraxis_showscale=False,yaxis_title=""); st.plotly_chart(fig_pct,use_container_width=True)
            st.download_button("📥 Descargar reporte (CSV)",df_res.to_csv(index=False).encode("utf-8"),f"cobertura_{fz1}_{fz2}.csv","text/csv")

    with tab9:
        st.subheader("🚚 ¿Los aliados que dijeron que iban a cargar, cargaron?")

        with st.expander("📥 Subir hojas de ruta desde Looker", expanded=False):
            st.caption("Descarga la tabla del tablero 'Servicio - Operaciones Latam / Cumplimiento Diario Aliados' "
                       "(⋮ → Exportar → CSV o Excel) y súbela tal cual, sin quitar columnas. Se usan: identificacion, "
                       "HJRT ID, Creacion, Estado HR, Nombre Aliado, Ciudad, Tot Paq, Entregados y Paq Gestionados por Aliado. "
                       f"Cada carga se suma al historial (hoja {HOJA_CARGUES}); si una hoja de ruta ya estaba, se actualiza.")
            archivo_carg = st.file_uploader("Exportación de Looker (CSV o Excel)", type=["xlsx", "xls", "csv"], key="up_cargues_prog")
            if archivo_carg is not None and st.button("🚀 Cargar hojas de ruta", key="btn_up_cargues_prog"):
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
                               key="cum_an_prog")
        hoy_c = now_col().date()
        c_f1 = cf2.date_input("Desde", hoy_c.replace(day=1), max_value=hoy_c, key="cum_f1_prog")
        c_f2 = cf3.date_input("Hasta", hoy_c, max_value=hoy_c, key="cum_f2_prog")

        if st.button("🔄 Actualizar datos", key="btn_ref_cum_prog"):
            _invalidar_cargues()
            hist_cum = _get_hist(force_reload=True)
        else:
            hist_cum = _get_hist()
        cargues_cum = _get_cargues()

        if base is not None:
            aliados_cum = pd.DataFrame({
                "identificacion": base["identificacion"],
                "nombre": base["mensajero"] if "mensajero" in base.columns else "",
                "ciudad": base["zona"].map(_ciudad_corta) if "zona" in base.columns else "Sin ciudad",
            })
        else:
            aliados_cum = pd.DataFrame(columns=["identificacion", "nombre", "ciudad"])

        if hist_cum is None or hist_cum.empty:
            st.info("Aún no hay gestiones registradas en HISTORICO.")
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
            m2 = st.columns(3)
            m2[0].metric("🚗 Interesados", k["interesados"])
            m2[1].metric("Día más fructífero", k["mejor_dia"])
            m2[2].metric("Ciudad con más efecto", k["mejor_ciudad"])

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
                                        default=["✅ Cargó", "❌ No cargó"], key="cum_filtro_prog")
            det_f = det[det.resultado.isin(filtro_res)]
            st.dataframe(det_f.rename(columns={
                "cedula": "Cédula", "nombre": "Nombre", "ciudad": "Ciudad", "fecha_llamada": "Fecha llamada", "dia": "Día",
                "analista": "Analista", "estado_registrado": "Estado registrado", "razon": "Razón", "resultado": "Resultado",
                "rutas_despues": "Hojas de ruta después", "paquetes_despues": "Paquetes después",
                "entregados_despues": "Entregados después", "gestionados_despues": "Gestionados después", "primer_cargue": "Primer cargue",
                "dias_hasta_cargar": "Días hasta cargar", "ultimo_cargue_antes": "Último cargue antes de la llamada"}),
                hide_index=True, use_container_width=True)
            st.download_button("📥 Descargar detalle (CSV)", det.to_csv(index=False).encode("utf-8-sig"),
                               f"cumplimiento_cargue_{c_f1}_{c_f2}.csv", "text/csv", key="dl_cum_prog")

# ================================================================
# ANALISTA
# ================================================================
if perfil == "Analista":
    base = _get_base(); hist = _get_hist()
    if base is None:
        st.warning("⚠️ La coordinadora aún no ha cargado la base. Espera un momento.")
        st.stop()
    tab_g, tab_h, tab_his, tab_bus = st.tabs(["📞 Gestión del Día","📊 Mi Resumen de Hoy","📅 Mi Histórico","🔍 Buscar Aliado"])
    with tab_g:
        modo_c,zona_c,vh_c=leer_config(nombre)
        if modo_c in ("Asignación general (todos igual)","Asignación por analista") and zona_c and vh_c:
            zona_sel=str(zona_c); vh_sel=str(vh_c)
            st.success(f"🎯 Hoy: **{zona_sel}** — **{vh_sel}**")
        else:
            zonas=sorted(base["zona"].dropna().unique()); vhs=sorted(base["vehiculo_norm"].dropna().unique())
            zona_sel=st.selectbox("Zona",zonas); vh_sel=st.selectbox("Vehículo",vhs)
        pool=base[(base["zona"].astype(str)==zona_sel)&(base["vehiculo_norm"].astype(str)==vh_sel)].copy()
        pool=filtrar_pool(pool)
        if pool.empty:
            st.info("No hay aliados disponibles. Los aliados en pausa volverán cuando se cumpla su tiempo.")
            st.stop()
        pool["PRIORIDAD"]=pool["dias"].apply(_prio)
        op={"🔴 ALTA":0,"🟡 MEDIA":1,"🟢 BAJA":2}
        pool["_o"]=pool["PRIORIDAD"].map(op).fillna(3)
        pool=pool.sort_values("_o").drop(columns=["_o"]).reset_index(drop=True)
        _hist_pool = st.session_state.get("hist_df", pd.DataFrame())
        if not _hist_pool.empty:
            hv = _hist_pool.copy(); hv["fecha"] = pd.to_datetime(hv["fecha"], errors="coerce"); hv = hv.dropna(subset=["fecha"])
            gh = hv[hv["fecha"].dt.date==now_col().date()]["identificacion"].astype(str).tolist()
            pool = pool[~pool["identificacion"].astype(str).isin(gh)]
        c1,c2=st.columns(2)
        with c1: cant=st.number_input("Cantidad de aliados",min_value=10,max_value=300,value=30)
        with c2: fp=st.selectbox("Prioridad",["Todas (ALTA + MEDIA + BAJA)","Solo 🔴 ALTA","Solo 🟡 MEDIA","Solo 🟢 BAJA"])
        if fp=="Solo 🔴 ALTA":    pool=pool[pool["PRIORIDAD"]=="🔴 ALTA"]
        elif fp=="Solo 🟡 MEDIA": pool=pool[pool["PRIORIDAD"]=="🟡 MEDIA"]
        elif fp=="Solo 🟢 BAJA":  pool=pool[pool["PRIORIDAD"]=="🟢 BAJA"]
        st.caption(f"Disponibles en este filtro: **{len(pool)}**")
        if st.button("🚀 Generar mis llamadas"):
            hoy_s=now_col().date().isoformat(); rep=cargar_reparto()
            if not rep.empty and "fecha" in rep.columns:
                if len(rep)>0 and str(rep["fecha"].iloc[0])!=hoy_s:
                    rep=pd.DataFrame(columns=["fecha","analista","identificacion"])
            else:
                rep=pd.DataFrame(columns=["fecha","analista","identificacion"])
            ya=rep[rep["fecha"]==hoy_s]["identificacion"].astype(str).tolist()
            bloque=pool[~pool["identificacion"].astype(str).isin(ya)].head(int(cant)).reset_index(drop=True)
            if bloque.empty:
                st.warning("Sin aliados disponibles en este filtro.")
            else:
                nf=pd.DataFrame({"fecha":[hoy_s]*len(bloque),"analista":[nombre]*len(bloque),"identificacion":bloque["identificacion"].astype(str).tolist()})
                guardar_reparto(pd.concat([rep,nf],ignore_index=True))
                st.session_state["pool_activo"]=bloque; st.session_state["hechas"]=0
                st.success(f"✅ {len(bloque)} aliados asignados."); st.rerun()
        hoy_s=now_col().date().isoformat(); rep_act=cargar_reparto(); mis_ids=[]
        if not rep_act.empty and "fecha" in rep_act.columns and "analista" in rep_act.columns:
            mis_ids=rep_act[(rep_act["fecha"]==hoy_s)&(rep_act["analista"]==nombre)]["identificacion"].astype(str).tolist()
        hist_fresco = st.session_state.get("hist_df", pd.DataFrame())
        if mis_ids and not hist_fresco.empty:
            hv2=hist_fresco.copy(); hv2["fecha"]=pd.to_datetime(hv2["fecha"],errors="coerce"); hv2=hv2.dropna(subset=["fecha"])
            gh2=hv2[hv2["fecha"].dt.date==now_col().date()]["identificacion"].astype(str).tolist()
            mis_ids=[i for i in mis_ids if i not in gh2]
        if mis_ids:
            hechas=st.session_state.get("hechas",0); rest=len(mis_ids)
            pct=int(hechas/(hechas+rest)*100) if (hechas+rest)>0 else 0
            st.progress(pct,text=f"Progreso: {hechas} gestionados / {rest} pendientes")
            mis_datos=base[base["identificacion"].astype(str).isin(mis_ids)].copy()
            if "PRIORIDAD" not in mis_datos.columns: mis_datos["PRIORIDAD"]=mis_datos["dias"].apply(_prio)
            cols_v=[c for c in ["identificacion","mensajero","celular","zona","vehiculo","dias","intentos","PRIORIDAD"] if c in mis_datos.columns]
            st.markdown(f"#### 📋 Pendientes ({rest})")
            st.dataframe(mis_datos[cols_v],use_container_width=True,hide_index=True)
            st.markdown("---"); st.markdown("#### 📞 Registrar gestión")
            with st.form("form_g",clear_on_submit=True):
                c1,c2=st.columns(2)
                with c1: ali=st.selectbox("Cédula del aliado",mis_ids); res=st.selectbox("Resultado de la llamada",RESULTADOS)
                with c2: est=st.selectbox("Estado final (si contestó)",["-"]+ESTADOS_FINALES); raz=st.selectbox("Razón (si contestó)",["-"]+RAZONES)
                fd=mis_datos[mis_datos["identificacion"].astype(str)==str(ali)]
                if not fd.empty:
                    f=fd.iloc[0]; ic=[c for c in ["mensajero","celular","intentos","PRIORIDAD"] if c in f.index]
                    ci=st.columns(max(len(ic),1))
                    for i,cn in enumerate(ic): ci[i].metric(cn.capitalize(),str(f[cn]))
                obs=st.text_area("Observaciones"); sub=st.form_submit_button("💾 GUARDAR GESTIÓN")
            if sub:
                er=None if est=="-" else est; rr=None if raz=="-" else raz
                if res=="Sí contestó" and er is None:
                    st.error("Selecciona un Estado final.")
                else:
                    guardar_gestion({"fecha":now_col(),"analista":nombre,"identificacion":ali,"resultado":res,"estado":er,"razon":rr,"obs":obs})
                    with st.spinner("Actualizando CRM..."): actualizar_base_crm(ali,res,er,rr)
                    st.session_state["hechas"]=st.session_state.get("hechas",0)+1
                    st.success("✅ Guardado. Próximo recontacto calculado automáticamente."); st.rerun()
        else:
            st.info("✅ Sin aliados pendientes. Genera un nuevo bloque arriba.")
    with tab_h:
        st.subheader(f"Tus gestiones de hoy — {now_col().strftime('%d/%m/%Y')}")
        if hist.empty:
            st.info("Sin gestiones hoy.")
        else:
            hv3=hist.copy(); hv3["fecha"]=pd.to_datetime(hv3["fecha"],errors="coerce"); hv3=hv3.dropna(subset=["fecha"])
            mh=hv3[(hv3["analista"]==nombre)&(hv3["fecha"].dt.date==now_col().date())].copy()
            if mh.empty:
                st.info("Sin gestiones hoy. ¡Empieza en Gestión del Día!")
            else:
                t=len(mh); sc=len(mh[mh["resultado"]=="Sí contestó"]); it=len(mh[mh["estado"]=="Interesado llega a cargue"]); nr=len(mh[mh["resultado"].isin(NO_RESPONDEN)])
                c1,c2,c3,c4=st.columns(4)
                c1.metric("📞 Llamadas",t); c2.metric("✅ Contactados",sc); c3.metric("🚗 Interesados",it); c4.metric("📵 No resp.",nr)
                if t>0: st.metric("% Efectividad",f"{round(it/t*100,1)}%")
                st.markdown("---"); mh["Hora"]=mh["fecha"].dt.strftime("%I:%M %p")
                st.dataframe(mh[["Hora","identificacion","resultado","estado","razon","obs"]].rename(
                    columns={"identificacion":"Cédula","resultado":"Resultado","estado":"Estado","razon":"Razón","obs":"Obs"}),use_container_width=True,hide_index=True)
                st.download_button("📥 Descargar",mh.to_csv(index=False).encode("utf-8"),f"hoy_{now_col().date()}.csv","text/csv")
                if t>=3:
                    rr=mh.groupby("resultado").size().reset_index(name="n")
                    st.plotly_chart(px.pie(rr,values="n",names="resultado",title="Distribución de resultados"),use_container_width=True)
    with tab_his:
        st.subheader("Mi Histórico de Gestiones")
        if hist.empty:
            st.info("Sin historial.")
        else:
            hv4=hist.copy(); hv4["fecha"]=pd.to_datetime(hv4["fecha"],errors="coerce"); hv4=hv4.dropna(subset=["fecha"])
            mhist=hv4[hv4["analista"]==nombre].copy()
            if mhist.empty:
                st.info("Sin gestiones registradas aún.")
            else:
                c1,c2=st.columns(2)
                with c1: fd=st.date_input("Desde",now_col().date()-timedelta(days=7),max_value=now_col().date(),key="mh_d")
                with c2: fh=st.date_input("Hasta",now_col().date(),max_value=now_col().date(),key="mh_h")
                mf=mhist[(mhist["fecha"].dt.date>=fd)&(mhist["fecha"].dt.date<=fh)].copy()
                if mf.empty:
                    st.warning("Sin gestiones en ese rango.")
                else:
                    t=len(mf); sc=len(mf[mf["resultado"]=="Sí contestó"]); it=len(mf[mf["estado"]=="Interesado llega a cargue"]); nr=len(mf[mf["resultado"].isin(NO_RESPONDEN)])
                    c1,c2,c3,c4=st.columns(4)
                    c1.metric("📞 Total",t); c2.metric("✅ Contactados",sc); c3.metric("🚗 Interesados",it); c4.metric("📵 No resp.",nr)
                    if t>0:
                        c5,c6=st.columns(2); c5.metric("% Contacto",f"{round(sc/t*100,1)}%"); c6.metric("% Interesados",f"{round(it/t*100,1)}%")
                    td=mf.groupby(mf["fecha"].dt.date).size().reset_index(name="llamadas"); td.columns=["fecha","llamadas"]
                    st.plotly_chart(px.bar(td,x="fecha",y="llamadas",title="Mis llamadas por día"),use_container_width=True)
                    st.markdown("---")
                    for dia in sorted(mf["fecha"].dt.date.unique(),reverse=True):
                        rd=mf[mf["fecha"].dt.date==dia].copy()
                        lbl="🟢 Hoy" if dia==now_col().date() else dia.strftime("%A %d/%m/%Y").capitalize()
                        with st.expander(f"{lbl} — {len(rd)} gestiones"):
                            rd["Hora"]=rd["fecha"].dt.strftime("%I:%M %p")
                            if base is not None:
                                cols_extra=[c for c in ["identificacion","vehiculo","municipio"] if c in base.columns]
                                base_mini=base[cols_extra].copy(); base_mini["identificacion"]=base_mini["identificacion"].astype(str)
                                rd["identificacion"]=rd["identificacion"].astype(str); rd=rd.merge(base_mini,on="identificacion",how="left")
                            cols_rd=["Hora","identificacion","resultado","estado","razon"]
                            for extra in ["vehiculo","municipio"]:
                                if extra in rd.columns: cols_rd.append(extra)
                            cols_rd.append("obs")
                            st.dataframe(rd[cols_rd].rename(columns={"identificacion":"Cédula","resultado":"Resultado","estado":"Estado","razon":"Razón","vehiculo":"Vehículo","municipio":"Ciudad","obs":"Obs"}),use_container_width=True,hide_index=True)
                    st.download_button("📥 Descargar historial",mf.to_csv(index=False).encode("utf-8"),f"historial_{fd}_{fh}.csv","text/csv")
    with tab_bus:
        st.subheader("🔍 Buscar Aliado")
        st.caption("Consulta datos, historial y registra una gestión para cualquier aliado.")
        modo_busq_a = st.radio("Buscar por", ["Cédula","Celular"], horizontal=True, key="modo_busq_ana")
        campo_busq_a = "identificacion" if modo_busq_a == "Cédula" else "celular"
        valor_bus = st.text_input(f"Ingresa el {modo_busq_a.lower()}", "", key="ana_busq_valor")
        if valor_bus.strip() and base is not None:
            if campo_busq_a not in base.columns:
                st.error(f"La base no tiene la columna '{campo_busq_a}'.")
            else:
                res_b = base[base[campo_busq_a].astype(str).str.strip() == valor_bus.strip()]
                if res_b.empty:
                    st.warning(f"No se encontró ningún aliado con {modo_busq_a.lower()} **{valor_bus}**.")
                else:
                    fila_b = res_b.iloc[0]
                    estado_badge_a = fila_b.get("estado_aliado", "🟢 Activo")
                    st.success(f"✅ Aliado encontrado — Estado: **{estado_badge_a}**")
                    cedula_bus = str(fila_b.get("identificacion",""))
                    cols_info = [c for c in ["identificacion","estado_aliado","estado_pipeline","rutas","mensajero","celular","zona","municipio","vehiculo","categoria",
                                              "dias","intentos","ultimo_resultado","ultimo_estado","proxima_gestion"] if c in fila_b.index]
                    c1, c2 = st.columns(2); mitad = len(cols_info) // 2
                    with c1:
                        for col in cols_info[:mitad]: st.metric(col.replace("_"," ").title(), str(fila_b[col]))
                    with c2:
                        for col in cols_info[mitad:]: st.metric(col.replace("_"," ").title(), str(fila_b[col]))
                    st.markdown("---"); st.markdown("#### 📋 Historial de gestiones")
                    hist_ali = hist[hist["identificacion"].astype(str) == cedula_bus].copy()
                    if hist_ali.empty:
                        st.info("Sin gestiones registradas para este aliado.")
                    else:
                        hist_ali["Hora"] = hist_ali["fecha"].dt.strftime("%d/%m/%Y %I:%M %p")
                        st.dataframe(hist_ali[["Hora","analista","resultado","estado","razon","obs"]].rename(
                            columns={"analista":"Analista","resultado":"Resultado","estado":"Estado","razon":"Razón","obs":"Obs"}),use_container_width=True,hide_index=True)
                    st.markdown("---"); st.markdown("#### 📞 Registrar gestión para este aliado")
                    ya_gestionado_hoy = False
                    if not hist.empty:
                        hv_bus=hist.copy(); hv_bus["fecha"]=pd.to_datetime(hv_bus["fecha"],errors="coerce"); hv_bus=hv_bus.dropna(subset=["fecha"])
                        gest_hoy_bus=hv_bus[(hv_bus["identificacion"].astype(str)==cedula_bus)&(hv_bus["fecha"].dt.date==now_col().date())]
                        ya_gestionado_hoy = not gest_hoy_bus.empty
                    if ya_gestionado_hoy:
                        st.info("✅ Este aliado ya fue gestionado hoy. Puedes gestionar de nuevo si es necesario.")
                    form_key = f"form_busq_{cedula_bus}"
                    with st.form(form_key, clear_on_submit=True):
                        cb1, cb2 = st.columns(2)
                        with cb1: res_b2 = st.selectbox("Resultado de la llamada", RESULTADOS, key=f"res_{cedula_bus}")
                        with cb2: est_b  = st.selectbox("Estado final (si contestó)", ["-"]+ESTADOS_FINALES, key=f"est_{cedula_bus}")
                        raz_b  = st.selectbox("Razón (si contestó)", ["-"]+RAZONES, key=f"raz_{cedula_bus}")
                        obs_b  = st.text_area("Observaciones", key=f"obs_{cedula_bus}")
                        sub_b  = st.form_submit_button("💾 GUARDAR GESTIÓN")
                    if sub_b:
                        er_b = None if est_b == "-" else est_b; rr_b = None if raz_b == "-" else raz_b
                        if res_b2 == "Sí contestó" and er_b is None:
                            st.error("Selecciona un Estado final.")
                        else:
                            guardar_gestion({"fecha":now_col(),"analista":nombre,"identificacion":cedula_bus,"resultado":res_b2,"estado":er_b,"razon":rr_b,"obs":obs_b})
                            with st.spinner("Actualizando CRM..."): actualizar_base_crm(cedula_bus, res_b2, er_b, rr_b)
                            st.success(f"✅ Gestión guardada para {cedula_bus}. Próximo recontacto calculado."); st.rerun()
        elif valor_bus.strip() and base is None:
            st.warning("Base no disponible.")
