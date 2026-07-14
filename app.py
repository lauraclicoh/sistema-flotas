import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
import plotly.express as px
import time
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
}
NOMBRES_ANALISTAS = list(ANALISTAS.keys())
RESULTADOS       = ["Apagado","Fuera de servicio","No contestó","Número errado","Sí contestó"]
ESTADOS_FINALES  = [
    "Aliado Rechaza la oferta",
    "Aliado Fleet/Delivery no acepta hub",
    "Interesado llega a cargue",
    "Interesado esporádico",
    "Empleado",
    "Point",
]
RAZONES = [
    "Interesado carga hoy",
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

def excluir_aliados_inactivos(df: pd.DataFrame) -> pd.DataFrame:
    """Excluye aliados cuyo Estado en BASE sea Inactivo de toda operación."""
    if df is None or df.empty or "estado" not in df.columns:
        return df
    estado = df["estado"].fillna("").astype(str).str.strip().str.casefold()
    return df.loc[estado != "inactivo"].copy()

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

# ================================================================
# NUEVO: calcular_estado_aliado
# KPI principal del sistema — calculado automáticamente
# Cargando / No ubicable / Deserta
# ================================================================
def calcular_estado_aliado(row) -> str:
    """
    Determina el estado operativo del aliado a partir de sus datos CRM.
    - Deserta   : bloqueado permanente o rechazó/empleado/point
    - No ubicable: no contesta repetidamente (en pausa activa por no contacto)
    - Cargando  : tiene actividad de cargue reciente (dias > 0 y <= 30)
    """
    proxima        = str(row.get("proxima_gestion", "")).strip().upper()
    ultimo_res     = str(row.get("ultimo_resultado", "")).strip()
    ultimo_estado  = str(row.get("ultimo_estado", "")).strip()
    intentos       = 0
    try: intentos  = int(float(str(row.get("intentos", 0))))
    except: pass
    dias           = 0
    try: dias      = int(float(str(row.get("dias", 0))))
    except: pass

    # ── Deserta ─────────────────────────────────────────────
    if proxima == "NO_VOLVER":
        return "Deserta"
    if ultimo_estado in NO_VOLVER_ESTADOS:
        return "Deserta"

    # ── Cargando ─────────────────────────────────────────────
    # Tiene cargue reciente (último cargue hace 30 días o menos)
    if 0 < dias <= 30:
        return "Cargando"

    # ── No ubicable ──────────────────────────────────────────
    # No contesta en varios intentos y está en pausa activa
    if ultimo_res in NO_RESPONDEN and intentos >= 3:
        f_prox = pd.to_datetime(proxima, errors="coerce")
        if not pd.isna(f_prox) and f_prox > now_col():
            return "No ubicable"

    # Sin gestión previa o caso borde → No ubicable
    return "No ubicable"


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
            # El descarte se realiza al consultar BASE, antes de poblar vistas,
            # pools, indicadores, búsquedas o asignaciones.
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
            # ── NUEVO: calcular estado_aliado al cargar ──────
            df["estado_aliado"] = df.apply(calcular_estado_aliado, axis=1)
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
        # ── NUEVO: actualizar estado_aliado en caché local ──
        if "base_df" in st.session_state and st.session_state["base_df"] is not None:
            base_local = st.session_state["base_df"]
            mask = base_local["identificacion"].astype(str) == str(identificacion)
            if mask.any():
                base_local.loc[mask, "ultimo_resultado"] = _safe_str(resultado)
                base_local.loc[mask, "ultimo_estado"]    = _safe_str(estado)
                base_local.loc[mask, "intentos"]         = intentos_n
                base_local.loc[mask, "proxima_gestion"]  = _safe_str(proxima)
                for idx in base_local[mask].index:
                    base_local.at[idx, "estado_aliado"] = calcular_estado_aliado(base_local.loc[idx])
                st.session_state["base_df"] = base_local
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
        proxima_str  = _safe_str(proxima).upper()
        es_no_volver = proxima_str == "NO_VOLVER"
        es_pausa     = False
        if not es_no_volver:
            f_prox   = pd.to_datetime(_safe_str(proxima), errors="coerce")
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
    df_nuevo = _df_safe_str(df_nuevo)
    df_nuevo["identificacion"] = df_nuevo["identificacion"].str.strip()
    df_nuevo = df_nuevo.fillna("")
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
    # Impide que nuevos inactivos entren y elimina los que cambien a Inactivo.
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

# ================================================================
# MÓDULO IMPLEMENTACIÓN
# ================================================================
CARGUES_META_IMPL = 20  # Meta final: pasa a Programación
CARGUES_MIN_IMPL  = 7   # Los analistas reciben desde el cargue 7
# Implementación se accede desde el mismo perfil autenticado de Programación;
# no conserva credenciales ni usuarios independientes.

RESULTADOS_IMPL = ["Apagado","Fuera de servicio","No contestó","Número errado","Sí contestó"]
ESTADOS_IMPL = [
    "Comprometido a cargar",
    "Interesado pero sin fecha",
    "Necesita seguimiento",
    "Abandona — tarifa",
    "Abandona — zona",
    "Abandona — vehículo averiado",
    "Abandona — trabaja fijo",
    "Abandona — no le interesa",
    "Llegó al 20mo cargue",
]
RAZONES_IMPL = [
    "Tarifa baja","Zona no le conviene","Vehículo averiado","Trabaja fijo",
    "No disponibilidad de tiempo","Prefiere otra operación","No responde repetidamente","Cargó hoy / sigue activo",
]
NO_RESP_IMPL = ["Apagado","Fuera de servicio","No contestó","Número errado"]

def _prio_impl(cargues):
    """
    Prioridad para aliados en rango 7-20 cargues.
    7-10  → ALTA  (recién llegados, mayor riesgo de abandono)
    11-15 → MEDIA
    16-19 → BAJA  (cerca de la meta)
    """
    try: cargues = int(cargues)
    except: return "🟢 BAJA"
    if cargues <= 10: return "🔴 ALTA"
    if cargues <= 15: return "🟡 MEDIA"
    return "🟢 BAJA"

def calcular_proxima_impl(resultado, estado, intentos):
    hoy = now_col()
    estado = str(estado or "")
    if "Abandona" in estado: return "NO_VOLVER"
    if resultado in NO_RESP_IMPL:
        if intentos >= 10: return hoy + timedelta(days=30)
        return hoy + timedelta(days=3)
    if estado == "Comprometido a cargar": return hoy + timedelta(days=2)
    if estado == "Interesado pero sin fecha": return hoy + timedelta(days=3)
    return hoy + timedelta(days=4)

def _get_impl(force=False):
    ahora  = time.time()
    ultima = st.session_state.get("impl_last_load", 0)
    if force or "impl_df" not in st.session_state or (ahora - ultima) > 30:
        df = leer_hoja("BASE_IMPLEMENTACION")
        if df.empty:
            st.session_state["impl_df"] = None
        else:
            df.columns = df.columns.str.strip().str.lower()

            # Evita errores posteriores al operar con una base que no trae ID.
            if "identificacion" not in df.columns:
                alias_id = ["id_aliado", "id aliado", "id", "cedula", "cédula", "documento",
                            "nro_identificacion", "numero_identificacion"]
                col_id = next((col for col in alias_id if col in df.columns), None)
                if col_id:
                    df = df.rename(columns={col_id: "identificacion"})
                else:
                    st.error("BASE_IMPLEMENTACION debe incluir 'identificacion' (o ID, cédula o documento).")
                    st.session_state["impl_df"] = None
                    st.session_state["impl_last_load"] = ahora
                    return None
            df["identificacion"] = df["identificacion"].astype(str).str.strip()

            # FIX zona: alias múltiples
            if "zona" not in df.columns:
                for alias_z in ["municipio","ciudad","city"]:
                    if alias_z in df.columns:
                        df["zona"] = df[alias_z]; break
            if "zona" not in df.columns:
                df["zona"] = "Sin zona"

            # FIX vehiculo: alias múltiples
            vh_col = next((c for c in ["vehiculo","tipo_vehiculo","vehicle","tipo_vh"] if c in df.columns), None)
            df["vehiculo_norm"] = df[vh_col].apply(_norm_vh) if vh_col else "Sin vehículo"

            # FIX total_cargues: alias múltiples
            tc_col = next((c for c in ["total_cargues","cargues","num_cargues","cargues_totales"] if c in df.columns), None)
            if tc_col:
                df["total_cargues"] = pd.to_numeric(df[tc_col], errors="coerce").fillna(0).astype(int)
            else:
                df["total_cargues"] = 0

            df["intentos_impl"]    = pd.to_numeric(df.get("intentos_impl", pd.Series(0, index=df.index)), errors="coerce").fillna(0).astype(int)
            # Solo aliados en rango 7-19 (los de 20+ ya pasaron a Programación)
            df = df[(df["total_cargues"] >= CARGUES_MIN_IMPL) &
                    (df["total_cargues"] < CARGUES_META_IMPL)].copy() if len(df) > 0 else df
            df["cargues_faltantes"] = (CARGUES_META_IMPL - df["total_cargues"]).clip(lower=0)
            df["prioridad_impl"]    = df["total_cargues"].apply(_prio_impl)

            # FIX: asegurar columna identificacion
            if "identificacion" not in df.columns:
                for alias_id in ["cedula","id_aliado","id","documento"]:
                    if alias_id in df.columns:
                        df["identificacion"] = df[alias_id]; break

            st.session_state["impl_df"] = df
        st.session_state["impl_last_load"] = ahora
    return st.session_state.get("impl_df")

def _get_hist_impl(force=False):
    ahora  = time.time()
    ultima = st.session_state.get("hist_impl_last", 0)
    if force or "hist_impl_df" not in st.session_state or (ahora - ultima) > 30:
        cols = ["fecha","analista","identificacion","resultado","estado","razon","obs","total_cargues_momento"]
        df   = leer_hoja("HIST_IMPLEMENTACION", cols)
        if df.empty:
            df = pd.DataFrame(columns=cols)
        else:
            df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
            df = df.dropna(subset=["fecha"])
        st.session_state["hist_impl_df"]   = df
        st.session_state["hist_impl_last"] = ahora
    return st.session_state["hist_impl_df"]

def _agregar_hist_impl_local(row):
    nuevo = pd.DataFrame([{
        "fecha": pd.to_datetime(row.get("fecha", now_col())),
        "analista": _safe_str(row.get("analista")),
        "identificacion": _safe_str(row.get("identificacion")),
        "resultado": _safe_str(row.get("resultado")),
        "estado": _safe_str(row.get("estado")),
        "razon": _safe_str(row.get("razon")),
        "obs": _safe_str(row.get("obs")),
        "total_cargues_momento": _safe_str(row.get("total_cargues_momento")),
    }])
    if "hist_impl_df" in st.session_state and isinstance(st.session_state["hist_impl_df"], pd.DataFrame):
        st.session_state["hist_impl_df"] = pd.concat([st.session_state["hist_impl_df"], nuevo], ignore_index=True)
    else:
        st.session_state["hist_impl_df"] = nuevo
    st.session_state["hist_impl_last"] = time.time()

def _get_gestionados_hoy_todos():
    gestionados = set()
    try:
        df_g = leer_hoja("GESTIONADOS_HOY")
        if not df_g.empty:
            df_g.columns = df_g.columns.str.lower()
            hoy = str(now_col().date())
            hoy_g = df_g[df_g["fecha"].astype(str).str.startswith(hoy)]
            gestionados.update(hoy_g["identificacion"].astype(str).tolist())
    except Exception:
        pass
    try:
        hist = leer_hoja("HISTORICO")
        if not hist.empty:
            hist.columns = hist.columns.str.lower()
            hist["fecha"] = pd.to_datetime(hist["fecha"], errors="coerce")
            hist = hist.dropna(subset=["fecha"])
            hoy_h = hist[hist["fecha"].dt.date == now_col().date()]
            gestionados.update(hoy_h["identificacion"].astype(str).tolist())
    except Exception:
        pass
    return gestionados

def guardar_gestion_impl(row):
    fila = [
        _safe_str(now_col()), _safe_str(row.get("analista")),
        _safe_str(row.get("identificacion")), _safe_str(row.get("resultado")),
        _safe_str(row.get("estado")), _safe_str(row.get("razon")),
        _safe_str(row.get("obs")), _safe_str(row.get("total_cargues_momento")),
    ]
    agregar_filas("HIST_IMPLEMENTACION", [fila])
    _agregar_hist_impl_local(row)
    try:
        agregar_filas("GESTIONADOS_HOY", [[_safe_str(now_col().date()),
                                            str(row.get("identificacion","")), "IMPLEMENTACION"]])
    except Exception:
        pass
    _actualizar_crm_impl(row.get("identificacion"), row.get("resultado"),
                          row.get("estado"), row.get("razon"), row.get("total_cargues_momento", 0))

def _actualizar_crm_impl(identificacion, resultado, estado, razon, total_cargues):
    try:
        sh = conectar_sheets()
        if sh is None: return
        ws      = sh.worksheet("BASE_IMPLEMENTACION")
        headers = ws.row_values(1)
        if "identificacion" not in headers: return
        col_id  = headers.index("identificacion") + 1
        ids     = ws.col_values(col_id)
        if str(identificacion) not in ids: return
        fila    = ids.index(str(identificacion)) + 1
        intentos_n = 1
        if "intentos_impl" in headers:
            col_int = headers.index("intentos_impl") + 1
            val_int = ws.cell(fila, col_int).value
            try: intentos_n = int(str(val_int or "0")) + 1
            except: intentos_n = 1
        proxima = calcular_proxima_impl(resultado, estado, intentos_n)
        proxima_str = _safe_str(proxima) if not isinstance(proxima, str) else proxima
        estado_pipeline = "Completó 20 cargues — Pasa a Programación" if (
            str(estado) == "Llegó al 20mo cargue" or int(total_cargues or 0) >= CARGUES_META_IMPL
        ) else str(estado or "")
        updates = []
        for col_name, val in [
            ("ultimo_resultado_impl", _safe_str(resultado)),
            ("ultimo_estado_impl",    _safe_str(estado)),
            ("ultima_razon_impl",     _safe_str(razon)),
            ("proxima_gestion_impl",  proxima_str),
            ("intentos_impl",         str(intentos_n)),
            ("estado_impl",           estado_pipeline),
        ]:
            if col_name in headers:
                col_idx = headers.index(col_name) + 1
                celda   = gspread.utils.rowcol_to_a1(fila, col_idx)
                updates.append({"range": celda, "values": [[val]]})
        if updates:
            ws.batch_update(updates)
        if "impl_df" in st.session_state:
            del st.session_state["impl_df"]
    except Exception as e:
        st.warning(f"CRM Implementación no actualizado: {e}")

def cargar_base_implementacion(df_nuevo, modo="incremental"):
    """
    Carga base de Implementación.
    modo='incremental': conserva CRM de existentes, agrega nuevos.
    modo='reemplazar': borra todo y sube desde cero.
    """
    df_nuevo = df_nuevo.copy()
    # Normalizar columnas
    df_nuevo.columns = (df_nuevo.columns.str.strip().str.lower()
                        .str.replace(r"\s+","_",regex=True))
    df_nuevo = df_nuevo[[c for c in df_nuevo.columns
                          if c and not c.startswith("unnamed") and c != "_"]]
    df_nuevo = df_nuevo.loc[:, ~df_nuevo.columns.duplicated()]

    # Detectar columna ID con alias ampliados
    ALIAS_ID = ["identificacion","id_aliado","id_aliado","cedula","id","documento",
                "nro_identificacion","numero_identificacion"]
    col_id = next((a for a in ALIAS_ID if a in df_nuevo.columns), None)
    if not col_id:
        st.error(f"No se encontró columna de ID. Columnas detectadas: {list(df_nuevo.columns)}")
        return 0, 0, 0

    df_nuevo = df_nuevo.rename(columns={col_id: "identificacion"})
    # Convertir todo a string seguro antes de subir
    df_nuevo = _df_safe_str(df_nuevo)
    df_nuevo["identificacion"] = df_nuevo["identificacion"].str.strip()
    df_nuevo = df_nuevo.fillna("")

    CRM_COLS = ["estado_impl","analista_impl","intentos_impl","proxima_gestion_impl",
                "ultimo_resultado_impl","ultimo_estado_impl","ultima_razon_impl","fecha_ingreso_impl"]

    # ── Modo REEMPLAZAR ──────────────────────────────────────────────────────
    if modo == "reemplazar":
        for col in CRM_COLS:
            if col not in df_nuevo.columns:
                df_nuevo[col] = "0" if col == "intentos_impl" else ""
        df_nuevo["fecha_ingreso_impl"] = _safe_str(now_col())
        df_nuevo = df_nuevo.fillna("")
        reemplazar_hoja("BASE_IMPLEMENTACION", df_nuevo)
        if "impl_df" in st.session_state: del st.session_state["impl_df"]
        return len(df_nuevo), 0, 0

    # ── Modo INCREMENTAL ─────────────────────────────────────────────────────
    base = leer_hoja("BASE_IMPLEMENTACION")

    if base.empty:
        # Primera carga
        for col in CRM_COLS:
            if col not in df_nuevo.columns:
                df_nuevo[col] = "0" if col == "intentos_impl" else ""
        df_nuevo["fecha_ingreso_impl"] = _safe_str(now_col())
        df_nuevo = df_nuevo.fillna("")
        reemplazar_hoja("BASE_IMPLEMENTACION", df_nuevo)
        if "impl_df" in st.session_state: del st.session_state["impl_df"]
        return len(df_nuevo), 0, 0

    # Normalizar base guardada
    base.columns = (base.columns.str.strip().str.lower()
                    .str.replace(r"\s+","_",regex=True))
    base = base.loc[:, ~base.columns.duplicated()]
    # Alias de ID en base guardada
    if "identificacion" not in base.columns:
        col_id_b = next((a for a in ALIAS_ID if a in base.columns), None)
        if col_id_b:
            base = base.rename(columns={col_id_b: "identificacion"})
        else:
            st.error("BASE_IMPLEMENTACION no tiene columna de identificación.")
            return 0, 0, 0
    base = _df_safe_str(base)
    base["identificacion"] = base["identificacion"].str.strip()
    base = base.fillna("")

    # total_cargues numérico para contar completados
    if "total_cargues" not in base.columns:
        base["total_cargues"] = "0"

    ids_existentes = set(base["identificacion"].unique())

    # Nuevos registros
    nuevos = df_nuevo[~df_nuevo["identificacion"].isin(ids_existentes)].copy()
    for col in CRM_COLS:
        if col not in nuevos.columns:
            nuevos[col] = "0" if col == "intentos_impl" else ""
    nuevos["fecha_ingreso_impl"] = _safe_str(now_col())
    nuevos = nuevos.fillna("")

    # Actualizar existentes — SOLO columnas operativas (no CRM)
    cols_op = [c for c in df_nuevo.columns if c not in CRM_COLS and c != "identificacion"]
    existentes_mask = df_nuevo["identificacion"].isin(ids_existentes)
    cols_sel = ["identificacion"] + cols_op
    exist_df = (df_nuevo[existentes_mask][cols_sel]
                .loc[:, ~pd.Index(cols_sel).duplicated()]
                .set_index("identificacion"))

    base_idx = base.set_index("identificacion")
    for col in cols_op:
        if col not in exist_df.columns: continue
        col_data = exist_df[[col]]
        if col in base_idx.columns:
            base_idx.update(col_data)
        else:
            base_idx = base_idx.join(col_data, how="left")

    # Contar completados
    tc_serie = pd.to_numeric(
        base_idx.get("total_cargues", pd.Series(dtype=str)),
        errors="coerce"
    ).fillna(0)
    completados = int((tc_serie >= CARGUES_META_IMPL).sum())

    base_actualizada = base_idx.reset_index()
    base_final = pd.concat([base_actualizada, nuevos], ignore_index=True)
    base_final = base_final.fillna("")
    base_final = base_final.loc[:, ~base_final.columns.duplicated()]

    # Última conversión segura antes de subir
    base_final = _df_safe_str(base_final)

    reemplazar_hoja("BASE_IMPLEMENTACION", base_final)
    if "impl_df" in st.session_state: del st.session_state["impl_df"]
    return len(nuevos), len(exist_df), completados

# ================================================================
# HELPER VISUAL: badge estado_aliado
# ================================================================
ESTADO_EMOJI = {"Cargando": "🟢", "No ubicable": "🟡", "Deserta": "🔴"}

def _badge_estado(estado: str) -> str:
    """Retorna texto con emoji para mostrar en tablas."""
    return f"{ESTADO_EMOJI.get(estado,'⚪')} {estado}"

# ================================================================
# UI
# ================================================================
st.title("🚚 Gestión Aliados Programación")

with st.sidebar:
    st.markdown("### 👤 Acceso")
    perfil = st.selectbox("Soy:", ["— Selecciona —","Coordinador","Analista"])

    if perfil == "Coordinador":
        pwd = st.text_input("Contraseña", type="password")
        if pwd != "clicoh":
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
# COORDINADOR
# ================================================================
if perfil == "Coordinador":
    base = _get_base()
    hist = _get_hist()

    tab1,tab2,tab3,tab4,tab5,tab6,tab7,tab8,tab9 = st.tabs([
        "📊 Hoy","📅 Histórico & KPIs","🔍 Buscar Aliado",
        "🔥 Estado CRM","📤 Cargar Base","🎯 Asignación","⚙️ Reglas","🗺️ Cobertura por Zona",
        "⚙️ Implementación",
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
        st.subheader("🔍 Buscar Aliado por Cédula o Teléfono")
        cedula_buscar = st.text_input("Ingresa la cédula o teléfono", "", key="busq_cedula")
        if cedula_buscar.strip() and base is not None:
            termino_c = cedula_buscar.strip()
            resultado_b = base[base["identificacion"].astype(str) == termino_c]
            if resultado_b.empty and "celular" in base.columns:
                resultado_b = base[base["celular"].astype(str).str.replace(r"\D","",regex=True) ==
                                   termino_c.replace(" ","")]
            if resultado_b.empty:
                st.warning(f"No se encontró ningún aliado con cédula **{cedula_buscar}**.")
            else:
                fila_b = resultado_b.iloc[0]; st.success("✅ Aliado encontrado")
                cols_info = [c for c in ["identificacion","mensajero","celular","correo","zona","municipio",
                                          "vehiculo","categoria","estado_aliado",
                                          "dias","intentos","ultimo_resultado","ultimo_estado","proxima_gestion"]
                             if c in fila_b.index]
                c1,c2 = st.columns(2); mitad = len(cols_info)//2
                with c1:
                    for col in cols_info[:mitad]: st.metric(col.replace("_"," ").title(), str(fila_b[col]))
                with c2:
                    for col in cols_info[mitad:]: st.metric(col.replace("_"," ").title(), str(fila_b[col]))
                st.markdown("---"); st.markdown("#### 📋 Historial de gestiones")
                hist_aliado = hist[hist["identificacion"].astype(str)==cedula_buscar.strip()].copy()
                if hist_aliado.empty:
                    st.info("Sin gestiones registradas para este aliado.")
                else:
                    hist_aliado["Hora"] = hist_aliado["fecha"].dt.strftime("%d/%m/%Y %I:%M %p")
                    st.dataframe(hist_aliado[["Hora","analista","resultado","estado","razon","obs"]].rename(
                        columns={"analista":"Analista","resultado":"Resultado","estado":"Estado","razon":"Razón","obs":"Obs"}
                    ),use_container_width=True,hide_index=True)
        elif cedula_buscar.strip() and base is None:
            st.warning("Carga la base primero.")

    # ── TAB 4: ESTADO CRM — NUEVO BLOQUE DE ESTADO_ALIADO ──────────────────
    with tab4:
        if base is None:
            st.warning("Carga la base primero.")
        else:
            # ── KPI PRINCIPAL: ESTADO ALIADO ─────────────────────────────────
            st.markdown("### 🎯 Estado Aliado — KPI Principal")
            n_cargando  = len(base[base["estado_aliado"] == "Cargando"])
            n_no_ubic   = len(base[base["estado_aliado"] == "No ubicable"])
            n_deserta   = len(base[base["estado_aliado"] == "Deserta"])
            total_base  = len(base)
            pct_carg    = round(n_cargando / max(total_base,1) * 100, 1)
            pct_noub    = round(n_no_ubic  / max(total_base,1) * 100, 1)
            pct_des     = round(n_deserta  / max(total_base,1) * 100, 1)

            # Cards grandes con color
            ea1, ea2, ea3 = st.columns(3)
            ea1.metric("🟢 Cargando",    f"{n_cargando:,}", f"{pct_carg}% del total")
            ea2.metric("🟡 No ubicable", f"{n_no_ubic:,}",  f"{pct_noub}% del total")
            ea3.metric("🔴 Deserta",     f"{n_deserta:,}",  f"{pct_des}% del total", delta_color="inverse")

            # Gráfico de distribución de estado_aliado
            st.markdown("---")
            df_estados = base["estado_aliado"].value_counts().reset_index()
            df_estados.columns = ["Estado", "Aliados"]
            fig_est = px.bar(
                df_estados, x="Estado", y="Aliados",
                color="Estado",
                color_discrete_map={"Cargando":"#639922","No ubicable":"#BA7517","Deserta":"#A32D2D"},
                title="Distribución de Estado Aliado en la base completa",
            )
            fig_est.update_layout(showlegend=False)
            st.plotly_chart(fig_est, use_container_width=True)

            # Filtro para ver detalle por estado
            st.markdown("---")
            st.markdown("#### Detalle por estado")
            estado_sel = st.selectbox(
                "Ver aliados en estado:",
                ["Todos","🟢 Cargando","🟡 No ubicable","🔴 Deserta"],
                key="sel_estado_crm"
            )
            base_filtrada = base.copy()
            if "Cargando" in estado_sel:
                base_filtrada = base[base["estado_aliado"] == "Cargando"]
            elif "No ubicable" in estado_sel:
                base_filtrada = base[base["estado_aliado"] == "No ubicable"]
            elif "Deserta" in estado_sel:
                base_filtrada = base[base["estado_aliado"] == "Deserta"]

            cols_crm_show = [c for c in ["identificacion","mensajero","celular","zona",
                                          "vehiculo","dias","intentos","estado_aliado",
                                          "ultimo_resultado","ultimo_estado","proxima_gestion"]
                             if c in base_filtrada.columns]
            base_show = base_filtrada[cols_crm_show].copy()
            if "estado_aliado" in base_show.columns:
                base_show["estado_aliado"] = base_show["estado_aliado"].apply(_badge_estado)
            st.caption(f"Mostrando {len(base_show):,} aliados")
            st.dataframe(base_show, use_container_width=True, hide_index=True)

            # ── CRM clásico debajo ───────────────────────────────────────────
            st.markdown("---")
            st.markdown("#### Vista CRM detallada")
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
                            base_operable = excluir_aliados_inactivos(df_s)
                            excluidos = len(df_s) - len(base_operable)
                            reemplazar_hoja("BASE", base_operable); _invalidar_base()
                        st.success(f"✅ {len(base_operable):,} aliados subidos · {excluidos:,} inactivos excluidos.")
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
        st.markdown("---")
        st.markdown("#### 🎯 Lógica de Estado Aliado")
        st.markdown("""
| Estado | Condición |
|---|---|
| 🟢 **Cargando** | `dias_ultimo_cargue` entre 1 y 30 días |
| 🟡 **No ubicable** | No contesta (3+ intentos) y está en pausa activa |
| 🔴 **Deserta** | `proxima_gestion = NO_VOLVER` o estado en lista de bloqueo |
        """)

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

    # ── TAB 9: IMPLEMENTACIÓN (dentro del módulo Coordinador) ──────────────
    with tab9:
        st.subheader("⚙️ Implementación — Dashboard Coordinador")
        df_impl_c  = _get_impl()
        hist_impl_c = _get_hist_impl()

        if st.button("🔄 Actualizar Implementación", key="impl_ref_coord_tab9"):
            _get_impl(force=True); _get_hist_impl(force=True); st.rerun()

        subtab_res, subtab_kpi, subtab_carga = st.tabs([
            "📊 Resumen","📈 KPIs y análisis","📤 Cargar base"
        ])

        with subtab_res:
            if df_impl_c is None:
                st.warning("Carga la base de Implementación primero (pestaña 📤 Cargar base).")
            else:
                # Segmentar la base
                completados_c = df_impl_c[df_impl_c["total_cargues"] >= CARGUES_META_IMPL]
                activo_impl_c = df_impl_c[
                    (df_impl_c["total_cargues"] >= CARGUES_MIN_IMPL) &
                    (df_impl_c["total_cargues"] < CARGUES_META_IMPL)
                ]
                total_activo  = len(activo_impl_c)
                abandonaron_c = df_impl_c[df_impl_c.get("estado_impl", pd.Series(dtype=str)).astype(str).str.contains("Abandona", na=False)]
                sin_contacto_c = df_impl_c[
                    df_impl_c.get("ultimo_resultado_impl", pd.Series(dtype=str)).astype(str).isin(NO_RESP_IMPL) &
                    (df_impl_c.get("intentos_impl", pd.Series(0, index=df_impl_c.index)).astype(int) >= 3)
                ] if "ultimo_resultado_impl" in df_impl_c.columns else df_impl_c.iloc[0:0]
                activos_c = df_impl_c[
                    ~df_impl_c.index.isin(completados_c.index) &
                    ~df_impl_c.index.isin(abandonaron_c.index)
                ]
                total_impl   = len(df_impl_c)
                # total_activo ya definido arriba
                pct_grad   = round(len(completados_c)/max(total_impl,1)*100,1)
                pct_churn  = round(len(abandonaron_c)/max(total_impl,1)*100,1)
                pct_noresp = round(len(sin_contacto_c)/max(total_impl,1)*100,1)

                # ── KPIs estilo dashboard adjunto ─────────────────────────
                st.markdown("---")
                st.markdown("##### Seguimiento Implementación")
                kc1,kc2,kc3,kc4,kc5 = st.columns(5)
                kc1.metric("👥 Total Drivers",  f"{total_impl}")
                kc2.metric("🔄 Activos R7–R19", f"{total_activo:,}", "En seguimiento")
                kc3.metric("📈 % Conversión a Programación",
                           f"{pct_grad}%", f"{len(completados_c)} Drivers")
                kc4.metric("⚠️ Tasa de Abandono",
                           f"{pct_churn}%", f"-{len(abandonaron_c)} Drivers", delta_color="inverse")
                kc5.metric("📵 No contesta / Sin resp.",
                           f"{pct_noresp}%", f"{len(sin_contacto_c)} Drivers", delta_color="inverse")

                st.markdown("---")

                # ── Funnel R1→R7 + Barras de conversión ───────────────────
                col_funnel, col_barras = st.columns(2)

                with col_funnel:
                    st.markdown("##### Funnel de retención R1 → R7")
                    # Contar aliados por cargue acumulado
                    funnel_data = []
                    for r in range(CARGUES_MIN_IMPL, CARGUES_META_IMPL + 1):
                        n = len(df_impl_c[
                            (df_impl_c["total_cargues"] >= r) &
                            (df_impl_c["total_cargues"] < CARGUES_META_IMPL)
                        ])
                        funnel_data.append({"Ruta": f"R{r}", "Aliados": n})
                    df_funnel = pd.DataFrame(funnel_data)
                    fig_funnel = px.bar(
                        df_funnel, x="Ruta", y="Aliados",
                        text="Aliados",
                        color="Ruta",
                        color_discrete_sequence=["#1565C0","#1976D2","#42A5F5","#64B5F6","#90CAF9","#BBDEFB","#E3F2FD"][:CARGUES_META_IMPL],
                        title=f"Funnel retención R{CARGUES_MIN_IMPL} → R{CARGUES_META_IMPL}"
                    )
                    fig_funnel.update_traces(textposition="outside")
                    fig_funnel.update_layout(showlegend=False, plot_bgcolor="#f8f9fa",
                                             paper_bgcolor="#f8f9fa", font_color="#333")
                    st.plotly_chart(fig_funnel, use_container_width=True)

                with col_barras:
                    st.markdown("##### % Churn por tramo (abandono entre rutas)")
                    churn_data = []
                    for r in range(CARGUES_MIN_IMPL, CARGUES_META_IMPL):
                        n_ini = len(df_impl_c[df_impl_c["total_cargues"] == r])
                        n_sig = len(df_impl_c[df_impl_c["total_cargues"] == r + 1])
                        pct_ch = round((1 - n_sig/max(n_ini,1))*100, 2) if n_ini > 0 else 0
                        churn_data.append({"Tramo": f"R{r}→R{r+1}", "% Churn": pct_ch})
                    df_churn = pd.DataFrame(churn_data)
                    CHURN_COLORS = ["#C62828","#E53935","#EF9A9A","#FFCC02","#66BB6A"]
                    fig_churn = px.bar(
                        df_churn, x="Tramo", y="% Churn",
                        text="% Churn",
                        color="Tramo",
                        color_discrete_sequence=CHURN_COLORS * 3,
                        title="% Abandono por tramo (R7 → R20)"
                    )
                    fig_churn.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
                    fig_churn.update_layout(showlegend=False, plot_bgcolor="#f8f9fa",
                                            paper_bgcolor="#f8f9fa", font_color="#333",
                                            yaxis_ticksuffix="%")
                    st.plotly_chart(fig_churn, use_container_width=True)

                # ── Alta prioridad ─────────────────────────────────────────
                alta_c = df_impl_c[df_impl_c["prioridad_impl"] == "🔴 ALTA"].copy()
                if not alta_c.empty:
                    st.markdown("---")
                    st.markdown(f"#### 🔴 Alta prioridad — {len(alta_c)} aliados (2do cargue, mayor riesgo de abandono)")
                    cols_ac = [c for c in ["identificacion","nombre","celular","zona","vehiculo_norm",
                                            "total_cargues","cargues_faltantes","analista_impl","estado_impl"]
                               if c in alta_c.columns]
                    st.dataframe(alta_c[cols_ac], use_container_width=True, hide_index=True)

                # ── Listos para Programación ───────────────────────────────
                listos_c = completados_c.copy()
                if not listos_c.empty:
                    st.markdown("---")
                    st.markdown("#### ✅ " + str(len(listos_c)) + f" aliados con R{CARGUES_META_IMPL}+ — listos para Programación")
                    cols_lc = [c for c in ["identificacion","nombre","celular","zona","vehiculo_norm","total_cargues"]
                               if c in listos_c.columns]
                    st.dataframe(listos_c[cols_lc], use_container_width=True, hide_index=True)
                    st.download_button("📥 Descargar listos para Programación",
                                       listos_c.to_csv(index=False).encode("utf-8"),
                                       "listos_programacion.csv", "text/csv")

                # ── Tabla completa filtrable ───────────────────────────────
                st.markdown("---")
                st.markdown("#### 📋 Base completa Implementación")
                col_fe1, col_fe2, col_fe3 = st.columns(3)
                with col_fe1:
                    est_fil_c = st.selectbox(
                        "Estado impl",
                        ["Todos"] + sorted(df_impl_c.get("estado_impl", pd.Series(dtype=str)).dropna().unique().tolist()),
                        key="est_fil_impl_coord"
                    )
                with col_fe2:
                    zonas_impl = ["Todas"] + sorted(df_impl_c["zona"].dropna().unique().tolist()) if "zona" in df_impl_c.columns else ["Todas"]
                    zona_fil_c = st.selectbox("Zona", zonas_impl, key="zona_fil_impl_coord")
                with col_fe3:
                    vhs_impl = ["Todos"] + sorted(df_impl_c["vehiculo_norm"].dropna().unique().tolist()) if "vehiculo_norm" in df_impl_c.columns else ["Todos"]
                    vh_fil_c = st.selectbox("Vehículo", vhs_impl, key="vh_fil_impl_coord")

                df_impl_show = df_impl_c.copy()
                if est_fil_c != "Todos":
                    df_impl_show = df_impl_show[df_impl_show["estado_impl"].astype(str) == est_fil_c]
                if zona_fil_c != "Todas" and "zona" in df_impl_show.columns:
                    df_impl_show = df_impl_show[df_impl_show["zona"].astype(str) == zona_fil_c]
                if vh_fil_c != "Todos" and "vehiculo_norm" in df_impl_show.columns:
                    df_impl_show = df_impl_show[df_impl_show["vehiculo_norm"].astype(str) == vh_fil_c]

                cols_tbl_c = [c for c in ["identificacion","nombre","celular","zona","vehiculo_norm",
                                           "total_cargues","cargues_faltantes","prioridad_impl",
                                           "estado_impl","analista_impl","intentos_impl","proxima_gestion_impl"]
                              if c in df_impl_show.columns]
                st.caption(f"Mostrando {len(df_impl_show):,} aliados")
                st.dataframe(df_impl_show[cols_tbl_c], use_container_width=True, hide_index=True)
                st.download_button("📥 Descargar filtrado",
                                   df_impl_show.to_csv(index=False).encode("utf-8"),
                                   f"implementacion_{now_col().date()}.csv", "text/csv")

        with subtab_kpi:
            if df_impl_c is None:
                st.warning("Carga la base primero.")
            else:
                total_ic  = len(df_impl_c)
                conv_7_c  = len(df_impl_c[df_impl_c["total_cargues"] >= CARGUES_META_IMPL])
                pct_conv_c = round(conv_7_c / max(total_ic, 1) * 100, 1)
                abd_c     = len(df_impl_c[df_impl_c.get("estado_impl", pd.Series(dtype=str)).astype(str).str.contains("Abandona", na=False)])
                pct_abd_c = round(abd_c / max(total_ic, 1) * 100, 1)

                ci1k, ci2k, ci3k = st.columns(3)
                ci1k.metric("% Conversión a Programación (R20)", f"{pct_conv_c}%")
                ci2k.metric("% abandono", f"{pct_abd_c}%")
                ci3k.metric("Cargue promedio",
                            f"{df_impl_c['total_cargues'].mean():.1f}" if total_ic else "N/A")

                st.markdown("---")
                if "zona" in df_impl_c.columns:
                    st.markdown("#### Conversión por zona")
                    zona_gc = df_impl_c.groupby("zona").agg(
                        total=("identificacion","count"),
                        completaron=("total_cargues", lambda x: (x >= CARGUES_META_IMPL).sum())
                    ).reset_index()
                    zona_gc["% conv"] = (zona_gc["completaron"] / zona_gc["total"] * 100).round(1)
                    st.dataframe(zona_gc.sort_values("% conv", ascending=False), use_container_width=True, hide_index=True)
                    fig_zona_c = px.bar(zona_gc, x="zona", y="% conv",
                                        title="% Conversión a Programación por zona",
                                        color="% conv",
                                        color_continuous_scale=["#dc3545","#ffc107","#28a745"],
                                        range_color=[0, 100])
                    st.plotly_chart(fig_zona_c, use_container_width=True)

                if "vehiculo_norm" in df_impl_c.columns:
                    st.markdown("#### Conversión por vehículo")
                    veh_gc = df_impl_c.groupby("vehiculo_norm").agg(
                        total=("identificacion","count"),
                        completaron=("total_cargues", lambda x: (x >= CARGUES_META_IMPL).sum())
                    ).reset_index()
                    veh_gc["% conv"] = (veh_gc["completaron"] / veh_gc["total"] * 100).round(1)
                    st.dataframe(veh_gc.sort_values("% conv", ascending=False), use_container_width=True, hide_index=True)

                if not hist_impl_c.empty:
                    st.markdown("---")
                    st.markdown("#### Razones de abandono")
                    abd_df_c = hist_impl_c[hist_impl_c["estado"].astype(str).str.contains("Abandona", na=False)]
                    if not abd_df_c.empty:
                        rz_c = abd_df_c["razon"].value_counts().reset_index()
                        rz_c.columns = ["Razón","Cantidad"]
                        st.dataframe(rz_c, use_container_width=True, hide_index=True)
                        fig_rz = px.bar(rz_c, x="Cantidad", y="Razón", orientation="h",
                                        title="Razones de abandono en Implementación",
                                        color_discrete_sequence=["#A32D2D"])
                        st.plotly_chart(fig_rz, use_container_width=True)

                    st.markdown("---")
                    st.markdown("#### Tendencia diaria de gestiones")
                    tend_ic = hist_impl_c.groupby(hist_impl_c["fecha"].dt.date).size().reset_index(name="gestiones")
                    tend_ic.columns = ["fecha","gestiones"]
                    st.plotly_chart(px.line(tend_ic, x="fecha", y="gestiones",
                                            title="Gestiones diarias — Implementación", markers=True),
                                    use_container_width=True)

        with subtab_carga:
            st.subheader("📤 Cargar base de Implementación")
            st.info("""
**Columnas esperadas en el Excel:**
`identificacion`, `nombre`, `celular`, `vehiculo`, `zona`, `total_cargues` (debe ser ≥ 7), `fecha_ultimo_cargue`

Los campos CRM se agregan automáticamente. Usa **Incremental** para conservar el historial.
            """)
            modo_impl_c = st.radio(
                "Modo de carga",
                ["🔄 Incremental (recomendado) — conserva historial CRM",
                 "♻️ Reemplazar toda la base — borra historial CRM"],
                key="modo_carga_impl_coord"
            )
            archivo_impl_c = st.file_uploader("Excel (.xlsx)", type=["xlsx"], key="uploader_impl_coord_tab9")
            if archivo_impl_c:
                try:
                    df_s_c = pd.read_excel(archivo_impl_c, engine="openpyxl")
                    df_s_c = df_s_c[[c for c in df_s_c.columns if not str(c).startswith("Unnamed")]]
                    df_s_c = _df_safe_str(df_s_c).fillna("")
                    st.success(f"{len(df_s_c):,} registros leídos")
                    st.dataframe(df_s_c.head(5), use_container_width=True)
                    if "Incremental" in modo_impl_c:
                        if st.button("🚀 Ejecutar Cruce Incremental", key="btn_incr_impl_coord"):
                            with st.spinner("Procesando cruce incremental..."):
                                nn_c, na_c, nc_c = cargar_base_implementacion(df_s_c, modo="incremental")
                            st.success(f"✅ {nn_c} aliados nuevos · {na_c} actualizados · {nc_c} ya alcanzaron R20 (pasan a Programación)")
                    else:
                        st.warning("⚠️ Esto borrará TODA la base de Implementación incluyendo el historial CRM.")
                        confirmar_impl = st.checkbox("Entiendo que se borrará todo el historial de Implementación", key="confirm_impl_coord")
                        if confirmar_impl and st.button("♻️ Reemplazar base completa", key="btn_repl_impl_coord"):
                            with st.spinner("Subiendo base completa..."):
                                nn_c, na_c, nc_c = cargar_base_implementacion(df_s_c, modo="reemplazar")
                            st.success(f"✅ {len(df_s_c):,} aliados subidos desde cero.")
                except Exception as e:
                    st.error(f"Error leyendo el archivo: {e}")

# ================================================================
# ANALISTA
# ================================================================
if perfil == "Analista":
    base = _get_base(); hist = _get_hist()
    if base is None:
        st.warning("⚠️ La coordinadora aún no ha cargado la base. Espera un momento.")
        st.stop()
    tab_g, tab_h, tab_his, tab_bus, tab_impl = st.tabs(["📞 Gestión del Día","📊 Mi Resumen de Hoy","📅 Mi Histórico","🔍 Buscar Aliado","⚙️ Implementación"])

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

        # ── NUEVO: filtro por estado_aliado en el pool ───────────────────
        if "estado_aliado" in pool.columns:
            estados_pool = ["Todos"] + sorted(pool["estado_aliado"].dropna().unique().tolist())
            col_cant, col_prio, col_est = st.columns(3)
            with col_cant: cant=st.number_input("Cantidad de aliados",min_value=10,max_value=300,value=30)
            with col_prio: fp=st.selectbox("Prioridad",["Todas (ALTA + MEDIA + BAJA)","Solo 🔴 ALTA","Solo 🟡 MEDIA","Solo 🟢 BAJA"])
            with col_est:
                est_filtro = st.selectbox("Estado aliado", estados_pool, key="filtro_est_aliado")
            if est_filtro != "Todos":
                pool = pool[pool["estado_aliado"] == est_filtro]
        else:
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
            # ── NUEVO: incluir estado_aliado en la tabla de pendientes ───────
            cols_v=[c for c in ["identificacion","mensajero","celular","zona","vehiculo",
                                  "dias","intentos","estado_aliado","PRIORIDAD"]
                    if c in mis_datos.columns]
            st.markdown(f"#### 📋 Pendientes ({rest})")
            mis_datos_show = mis_datos[cols_v].copy()
            if "estado_aliado" in mis_datos_show.columns:
                mis_datos_show["estado_aliado"] = mis_datos_show["estado_aliado"].apply(_badge_estado)
            st.dataframe(mis_datos_show, use_container_width=True, hide_index=True)
            st.markdown("---"); st.markdown("#### 📞 Registrar gestión")
            with st.form("form_g",clear_on_submit=True):
                c1,c2=st.columns(2)
                with c1: ali=st.selectbox("Cédula del aliado",mis_ids); res=st.selectbox("Resultado de la llamada",RESULTADOS)
                with c2: est=st.selectbox("Estado final (si contestó)",["-"]+ESTADOS_FINALES); raz=st.selectbox("Razón (si contestó)",["-"]+RAZONES)
                fd=mis_datos[mis_datos["identificacion"].astype(str)==str(ali)]
                if not fd.empty:
                    f=fd.iloc[0]; ic=[c for c in ["mensajero","celular","intentos","estado_aliado","PRIORIDAD"] if c in f.index]
                    ci=st.columns(max(len(ic),1))
                    for i,cn in enumerate(ic): ci[i].metric(cn.replace("_"," ").capitalize(),str(f[cn]))
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
        st.subheader("🔍 Buscar Aliado por Cédula o Teléfono")
        st.caption("Consulta datos, historial y registra una gestión para cualquier aliado.")
        cedula_bus = st.text_input("Ingresa la cédula o teléfono", "", key="ana_busq_cedula")
        if cedula_bus.strip() and base is not None:
            termino = cedula_bus.strip()
            # Buscar por cédula primero, luego por celular
            res_b = base[base["identificacion"].astype(str) == termino]
            if res_b.empty and "celular" in base.columns:
                res_b = base[base["celular"].astype(str).str.replace(r"\D","",regex=True) ==
                             termino.replace(" ","")]
            if res_b.empty:
                st.warning(f"No se encontró ningún aliado con cédula **{cedula_bus}**.")
            else:
                fila_b = res_b.iloc[0]; st.success("✅ Aliado encontrado")
                cols_info = [c for c in ["identificacion","mensajero","celular","zona","municipio",
                                          "vehiculo","categoria","estado_aliado",
                                          "dias","intentos","ultimo_resultado","ultimo_estado","proxima_gestion"]
                             if c in fila_b.index]
                c1, c2 = st.columns(2); mitad = len(cols_info) // 2
                with c1:
                    for col in cols_info[:mitad]: st.metric(col.replace("_"," ").title(), str(fila_b[col]))
                with c2:
                    for col in cols_info[mitad:]: st.metric(col.replace("_"," ").title(), str(fila_b[col]))
                st.markdown("---"); st.markdown("#### 📋 Historial de gestiones")
                hist_ali = hist[hist["identificacion"].astype(str) == cedula_bus.strip()].copy()
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
                    gest_hoy_bus=hv_bus[(hv_bus["identificacion"].astype(str)==cedula_bus.strip())&(hv_bus["fecha"].dt.date==now_col().date())]
                    ya_gestionado_hoy = not gest_hoy_bus.empty
                if ya_gestionado_hoy:
                    st.info("✅ Este aliado ya fue gestionado hoy. Puedes gestionar de nuevo si es necesario.")
                form_key = f"form_busq_{cedula_bus.strip()}"
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
                        guardar_gestion({"fecha":now_col(),"analista":nombre,"identificacion":cedula_bus.strip(),"resultado":res_b2,"estado":er_b,"razon":rr_b,"obs":obs_b})
                        with st.spinner("Actualizando CRM..."): actualizar_base_crm(cedula_bus.strip(), res_b2, er_b, rr_b)
                        st.success(f"✅ Gestión guardada para {cedula_bus.strip()}. Próximo recontacto calculado."); st.rerun()
        elif cedula_bus.strip() and base is None:
            st.warning("Base no disponible.")

    # ── TAB IMPLEMENTACIÓN (dentro del módulo Analista) ─────────────────────
    with tab_impl:
        st.subheader("⚙️ Implementación — Aliados del R7 al R20 (gestión hasta Programación)")
        df_impl_a  = _get_impl()
        hist_impl_a = _get_hist_impl()

        if df_impl_a is None:
            st.info("La coordinadora aún no ha cargado la base de Implementación.")
        else:
            tab_mis_a, tab_bus_a, tab_hoy_a = st.tabs([
                "📋 Mis aliados","🔍 Buscar aliado","📊 Mi resumen de hoy"
            ])

            with tab_mis_a:
                # Filtrar por analista si existe la columna
                if "analista_impl" in df_impl_a.columns:
                    mis_a = df_impl_a[df_impl_a["analista_impl"].astype(str) == nombre].copy()
                else:
                    mis_a = df_impl_a.copy()

                # Excluir completados y abandonados
                mis_a = mis_a[~mis_a.get("estado_impl", pd.Series(dtype=str)).astype(str).str.contains("Completó|Abandona", na=False)]
                mis_a = mis_a[mis_a.get("proxima_gestion_impl", pd.Series(dtype=str)).astype(str).str.upper() != "NO_VOLVER"]

                def disp_impl_a(v):
                    v = str(v).strip()
                    if v in ("","nan","None","0","NO_VOLVER"): return True
                    f = pd.to_datetime(v, errors="coerce")
                    return pd.isna(f) or f <= now_col()
                if "proxima_gestion_impl" in mis_a.columns:
                    mis_a = mis_a[mis_a["proxima_gestion_impl"].apply(disp_impl_a)]

                # Gestionados hoy en CUALQUIER módulo
                gestionados_hoy_a = _get_gestionados_hoy_todos()
                if not hist_impl_a.empty:
                    hh_a = hist_impl_a.copy()
                    hh_a["fecha"] = pd.to_datetime(hh_a["fecha"], errors="coerce")
                    hh_a = hh_a.dropna(subset=["fecha"])
                    ya_impl_hoy_a = set(hh_a[hh_a["fecha"].dt.date == now_col().date()]["identificacion"].astype(str).tolist())
                    gestionados_hoy_a = gestionados_hoy_a | ya_impl_hoy_a

                # FIX COMPLETO: siempre operar de forma segura sobre mis_a
                _col_id_ok = "identificacion" in mis_a.columns and not mis_a.empty
                if _col_id_ok:
                    mis_a = mis_a.copy()
                    mis_a["_ya_hoy"] = mis_a["identificacion"].astype(str).isin(gestionados_hoy_a)
                    pendientes_a = mis_a[~mis_a["_ya_hoy"]].copy()
                    ya_gest_a    = mis_a[mis_a["_ya_hoy"]].copy()
                else:
                    # DataFrame vacío con columnas mínimas para que el resto no falle
                    _empty_cols = list(df_impl_a.columns) if df_impl_a is not None else ["identificacion"]
                    pendientes_a = pd.DataFrame(columns=_empty_cols)
                    ya_gest_a    = pd.DataFrame(columns=_empty_cols)

                # Limpiar columna auxiliar y ordenar
                for _df_tmp in [pendientes_a, ya_gest_a]:
                    if "_ya_hoy" in _df_tmp.columns:
                        _df_tmp.drop(columns=["_ya_hoy"], inplace=True)

                orden_a = {"🔴 ALTA":0,"🟡 MEDIA":1,"🟢 BAJA":2}
                if not pendientes_a.empty and "prioridad_impl" in pendientes_a.columns:
                    pendientes_a = pendientes_a.copy()
                    pendientes_a["_ord"] = pendientes_a["prioridad_impl"].map(orden_a).fillna(3)
                    pendientes_a = pendientes_a.sort_values("_ord").drop(columns=["_ord"]).reset_index(drop=True)

                hechas_impl_a = st.session_state.get("impl_hechas_a", 0)
                pend_n_a = len(pendientes_a)
                pct_impl_a = int(hechas_impl_a / max(hechas_impl_a + pend_n_a, 1) * 100)
                st.progress(pct_impl_a, text=f"Progreso Implementación: {hechas_impl_a} gestionados · {pend_n_a} pendientes")

                if not ya_gest_a.empty:
                    with st.expander(f"⚠️ {len(ya_gest_a)} ya gestionados hoy en otro módulo"):
                        cols_dup_a = [c for c in ["identificacion","nombre","celular","total_cargues","prioridad_impl"] if c in ya_gest_a.columns]
                        st.dataframe(ya_gest_a[cols_dup_a], use_container_width=True, hide_index=True)

                cols_v_a = [c for c in ["identificacion","nombre","celular","zona","vehiculo_norm",
                                         "total_cargues","cargues_faltantes","prioridad_impl","estado_impl"]
                            if c in pendientes_a.columns]
                st.markdown(f"#### 📋 Pendientes ({pend_n_a})")
                st.dataframe(pendientes_a[cols_v_a], use_container_width=True, hide_index=True)

                st.markdown("---")
                st.markdown("#### 📞 Registrar gestión")
                # FIX: verificar columna identificacion antes de acceder
                if "identificacion" in pendientes_a.columns:
                    todos_ids_a = pendientes_a["identificacion"].astype(str).tolist()
                else:
                    todos_ids_a = []
                if not ya_gest_a.empty and "identificacion" in ya_gest_a.columns:
                    todos_ids_a += ya_gest_a["identificacion"].astype(str).tolist()

                if not todos_ids_a:
                    st.info("✅ Sin aliados pendientes de Implementación.")
                else:
                    with st.form("form_impl_analista", clear_on_submit=True):
                        ci1, ci2 = st.columns(2)
                        with ci1:
                            ali_ia = st.selectbox("Cédula del aliado", todos_ids_a, key="ali_impl_ana")
                            res_ia = st.selectbox("Resultado de la llamada", RESULTADOS_IMPL, key="res_impl_ana")
                        with ci2:
                            est_ia = st.selectbox("Estado", ["-"] + ESTADOS_IMPL, key="est_impl_ana")
                            raz_ia = st.selectbox("Razón", ["-"] + RAZONES_IMPL, key="raz_impl_ana")

                        fd_ia = mis_a[mis_a["identificacion"].astype(str) == str(ali_ia)]
                        if fd_ia.empty:
                            fd_ia = df_impl_a[df_impl_a["identificacion"].astype(str) == str(ali_ia)]
                        if not fd_ia.empty:
                            f_ia = fd_ia.iloc[0]
                            st.markdown("**Ficha del aliado**")
                            ficha_cols_a = [c for c in ["nombre","celular","total_cargues","cargues_faltantes",
                                                         "prioridad_impl","fecha_ultimo_cargue","estado_impl"]
                                            if c in f_ia.index]
                            cols_fia = st.columns(min(len(ficha_cols_a), 4))
                            for i, cn in enumerate(ficha_cols_a):
                                cols_fia[i % 4].metric(cn.replace("_", " ").title(), str(f_ia[cn]))
                            if str(ali_ia) in gestionados_hoy_a:
                                st.warning("⚠️ Este aliado ya fue gestionado hoy en otro módulo.")

                        obs_ia = st.text_area("Observaciones", key="obs_impl_ana")
                        sub_ia = st.form_submit_button("💾 GUARDAR GESTIÓN IMPLEMENTACIÓN")

                    if sub_ia:
                        er_ia = None if est_ia == "-" else est_ia
                        rr_ia = None if raz_ia == "-" else raz_ia
                        if res_ia == "Sí contestó" and er_ia is None:
                            st.error("Selecciona el estado del aliado.")
                        else:
                            tc_ia = int(fd_ia.iloc[0].get("total_cargues", 0)) if not fd_ia.empty else 0
                            guardar_gestion_impl({
                                "analista":              nombre,
                                "identificacion":        ali_ia,
                                "resultado":             res_ia,
                                "estado":                er_ia,
                                "razon":                 rr_ia,
                                "obs":                   obs_ia,
                                "total_cargues_momento": tc_ia,
                            })
                            st.session_state["impl_hechas_a"] = st.session_state.get("impl_hechas_a", 0) + 1
                            if str(er_ia) == "Llegó al 20mo cargue" or tc_ia >= CARGUES_META_IMPL:
                                st.success(f"🏆 ¡{ali_ia} completó los 20 cargues! Pasa a Programación.")
                            else:
                                st.success(f"✅ Guardado para {ali_ia}.")
                            st.rerun()

            with tab_bus_a:
                st.subheader("🔍 Buscar aliado en Implementación")
                cedula_bia = st.text_input("Cédula", "", key="impl_buscar_ana")
                if cedula_bia.strip():
                    fila_bia = df_impl_a[df_impl_a["identificacion"].astype(str) == cedula_bia.strip()]
                    if fila_bia.empty:
                        st.warning(f"No se encontró {cedula_bia} en la base de Implementación.")
                    else:
                        f_bia = fila_bia.iloc[0]
                        st.success("✅ Aliado encontrado")
                        cols_bia = [c for c in ["identificacion","nombre","celular","zona","vehiculo",
                                                 "total_cargues","cargues_faltantes","fecha_ultimo_cargue",
                                                 "estado_impl","intentos_impl","proxima_gestion_impl","analista_impl"]
                                    if c in f_bia.index]
                        ci1b, ci2b = st.columns(2)
                        mid_bia = len(cols_bia) // 2
                        with ci1b:
                            for col in cols_bia[:mid_bia]:
                                st.metric(col.replace("_", " ").title(), str(f_bia[col]))
                        with ci2b:
                            for col in cols_bia[mid_bia:]:
                                st.metric(col.replace("_", " ").title(), str(f_bia[col]))
                        st.markdown("---")
                        if not hist_impl_a.empty:
                            h_bia = hist_impl_a[hist_impl_a["identificacion"].astype(str) == cedula_bia.strip()].copy()
                            if h_bia.empty:
                                st.info("Sin gestiones en Implementación para este aliado.")
                            else:
                                h_bia["Hora"] = h_bia["fecha"].dt.strftime("%d/%m/%Y %I:%M %p")
                                st.dataframe(
                                    h_bia[["Hora","analista","resultado","estado","razon","obs","total_cargues_momento"]].rename(
                                        columns={"analista":"Analista","resultado":"Resultado","estado":"Estado",
                                                 "razon":"Razón","obs":"Obs","total_cargues_momento":"Cargues"}
                                    ), use_container_width=True, hide_index=True
                                )

            with tab_hoy_a:
                st.subheader(f"Mi resumen de Implementación hoy — {now_col().strftime('%d/%m/%Y')}")
                if st.button("🔄 Actualizar", key="impl_ref_ana_hoy"):
                    _get_hist_impl(force=True); st.rerun()
                if hist_impl_a.empty:
                    st.info("Sin gestiones de Implementación registradas aún.")
                else:
                    mh_ia = hist_impl_a[
                        (hist_impl_a["analista"] == nombre) &
                        (hist_impl_a["fecha"].dt.date == now_col().date())
                    ].copy()
                    if mh_ia.empty:
                        st.info("Sin gestiones hoy en Implementación. ¡Empieza en Mis Aliados!")
                    else:
                        t_ia  = len(mh_ia)
                        sc_ia = len(mh_ia[mh_ia["resultado"] == "Sí contestó"])
                        nr_ia = len(mh_ia[mh_ia["resultado"].isin(NO_RESP_IMPL)])
                        c7_ia = len(mh_ia[mh_ia["estado"].astype(str).str.contains("20mo cargue", na=False)])
                        ci1h, ci2h, ci3h, ci4h = st.columns(4)
                        ci1h.metric("📞 Llamadas", t_ia)
                        ci2h.metric("✅ Contactados", sc_ia)
                        ci3h.metric("📵 No resp.", nr_ia)
                        ci4h.metric("🏆 Llegaron R20", c7_ia)
                        st.markdown("---")
                        mh_ia["Hora"] = mh_ia["fecha"].dt.strftime("%I:%M %p")
                        st.dataframe(
                            mh_ia[["Hora","identificacion","resultado","estado","razon","obs"]].rename(
                                columns={"identificacion":"Cédula","resultado":"Resultado",
                                         "estado":"Estado","razon":"Razón","obs":"Obs"}
                            ), use_container_width=True, hide_index=True
                        )
                        st.download_button(
                            "📥 Descargar hoy Implementación",
                            mh_ia.to_csv(index=False).encode("utf-8"),
                            f"impl_hoy_{now_col().date()}.csv", "text/csv"
                        )
