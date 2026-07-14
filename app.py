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

# Una única fuente de verdad para el acceso de coordinación. Configure
# `coordinator_password` en secrets.toml; ambos módulos la consumen aquí.
COORDINATOR_PASSWORD = st.secrets.get("coordinator_password", "clicoh")

def excluir_aliados_inactivos(df: pd.DataFrame) -> pd.DataFrame:
    """Devuelve únicamente aliados operables según la columna Estado de BASE."""
    if df is None or df.empty:
        return df
    if "estado" not in df.columns:
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

# ================================================================
# MÓDULO IMPLEMENTACIÓN — funciones inline
# (no requiere archivo separado)
# ================================================================

CARGUES_META_IMPL = 7
# Implementación comparte exactamente los mismos usuarios analistas que
# Programación y no tiene una contraseña independiente.
ANALISTAS_IMPL    = NOMBRES_ANALISTAS

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
    "Llegó al 7mo cargue",
]
RAZONES_IMPL = [
    "Tarifa baja","Zona no le conviene","Vehículo averiado","Trabaja fijo",
    "No disponibilidad de tiempo","Prefiere otra operación","No responde repetidamente","Cargó hoy / sigue activo",
]
NO_RESP_IMPL = ["Apagado","Fuera de servicio","No contestó","Número errado"]

def _prio_impl(cargues):
    try: cargues = int(cargues)
    except: return "🟢 BAJA"
    if cargues <= 2: return "🔴 ALTA"
    if cargues <= 4: return "🟡 MEDIA"
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
            df["vehiculo_norm"] = df["vehiculo"].apply(_norm_vh) if "vehiculo" in df.columns else "Sin vehículo"
            df["total_cargues"] = pd.to_numeric(df.get("total_cargues", 0), errors="coerce").fillna(0).astype(int)
            df["intentos_impl"] = pd.to_numeric(df.get("intentos_impl", 0), errors="coerce").fillna(0).astype(int)
            df["cargues_faltantes"] = (CARGUES_META_IMPL - df["total_cargues"]).clip(lower=0)
            df["prioridad_impl"]    = df["total_cargues"].apply(_prio_impl)
            if "zona" not in df.columns: df["zona"] = "Sin zona"
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
        estado_pipeline = "Completó 7 cargues" if (
            str(estado) == "Llegó al 7mo cargue" or int(total_cargues or 0) >= CARGUES_META_IMPL
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

def cargar_base_implementacion(df_nuevo):
    df_nuevo = df_nuevo.copy()
    df_nuevo.columns = (df_nuevo.columns.str.strip().str.lower()
                        .str.replace(r"\s+","_",regex=True))
    df_nuevo = df_nuevo[[c for c in df_nuevo.columns if c and not c.startswith("unnamed")]]
    df_nuevo = df_nuevo.loc[:, ~df_nuevo.columns.duplicated()]
    ALIAS_ID = ["identificacion","id_aliado","cedula","id","documento"]
    col_id   = next((a for a in ALIAS_ID if a in df_nuevo.columns), None)
    if not col_id:
        st.error(f"No se encontró columna de ID. Columnas: {list(df_nuevo.columns)}")
        return 0, 0, 0
    df_nuevo = df_nuevo.rename(columns={col_id: "identificacion"})
    df_nuevo["identificacion"] = df_nuevo["identificacion"].astype(str).str.strip()
    base = leer_hoja("BASE_IMPLEMENTACION")
    CRM_COLS = ["estado_impl","analista_impl","intentos_impl","proxima_gestion_impl",
                "ultimo_resultado_impl","ultimo_estado_impl","ultima_razon_impl","fecha_ingreso_impl"]
    if base.empty:
        for col in CRM_COLS:
            if col not in df_nuevo.columns:
                df_nuevo[col] = "0" if col == "intentos_impl" else ""
        df_nuevo["fecha_ingreso_impl"] = _safe_str(now_col())
        reemplazar_hoja("BASE_IMPLEMENTACION", df_nuevo)
        if "impl_df" in st.session_state: del st.session_state["impl_df"]
        return len(df_nuevo), 0, 0
    base.columns = (base.columns.str.strip().str.lower().str.replace(r"\s+","_",regex=True))
    base["identificacion"] = base["identificacion"].astype(str).str.strip()
    if "total_cargues" not in base.columns: base["total_cargues"] = 0
    base["total_cargues"] = pd.to_numeric(base["total_cargues"], errors="coerce").fillna(0).astype(int)
    ids_existentes = set(base["identificacion"].unique())
    nuevos = df_nuevo[~df_nuevo["identificacion"].isin(ids_existentes)].copy()
    for col in CRM_COLS:
        if col not in nuevos.columns:
            nuevos[col] = "0" if col == "intentos_impl" else ""
    nuevos["fecha_ingreso_impl"] = _safe_str(now_col())
    cols_op  = [c for c in df_nuevo.columns if c not in CRM_COLS and c != "identificacion"]
    exist_df = (df_nuevo[df_nuevo["identificacion"].isin(ids_existentes)][["identificacion"]+cols_op]
                .loc[:, ~pd.Index(["identificacion"]+cols_op).duplicated()]
                .set_index("identificacion"))
    base_idx = base.set_index("identificacion")
    for col in cols_op:
        if col not in exist_df.columns: continue
        col_data = exist_df[[col]]
        if col in base_idx.columns: base_idx.update(col_data)
        else: base_idx = base_idx.join(col_data, how="left")
    completados = int((pd.to_numeric(base_idx.get("total_cargues", pd.Series(dtype=float)),
                                     errors="coerce").fillna(0) >= CARGUES_META_IMPL).sum())
    base_act  = base_idx.reset_index()
    base_final = pd.concat([base_act, nuevos], ignore_index=True).fillna("")
    base_final = base_final.loc[:, ~base_final.columns.duplicated()]
    reemplazar_hoja("BASE_IMPLEMENTACION", base_final)
    if "impl_df" in st.session_state: del st.session_state["impl_df"]
    return len(nuevos), len(exist_df), completados

# ================================================================
# UI
# ================================================================
st.title("🚚 Gestión Aliados Programación")

with st.sidebar:
    st.markdown("### 👤 Acceso")
    perfil = st.selectbox("Soy:", ["— Selecciona —","Coordinador","Analista","Implementación"])

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

    elif perfil == "Implementación":
        st.markdown("#### ⚙️ Implementación")
        rol_impl = st.selectbox("Rol", ["— Selecciona —","Coordinador Impl","Analista Impl"], key="rol_impl")
        if rol_impl == "Coordinador Impl":
            pwd_i = st.text_input("Contraseña de coordinador", type="password", key="pwd_ic")
            if pwd_i != COORDINATOR_PASSWORD:
                if pwd_i: st.error("Contraseña incorrecta")
                st.stop()
            st.success("✅ Coordinador Implementación")
            nombre = "Coordinador"
        elif rol_impl == "Analista Impl":
            nombre = st.selectbox("¿Quién eres?", ANALISTAS_IMPL, key="nom_impl")
            st.success(f"✅ {nombre}")
        else:
            st.info("Selecciona tu rol.")
            st.stop()

    else:
        st.info("Selecciona tu perfil para continuar.")
        st.stop()

# ================================================================
# COORDINADOR PRINCIPAL
# ================================================================
if perfil == "Coordinador":
    base = _get_base()
    hist = _get_hist()

    tab1,tab2,tab3,tab4,tab5,tab6,tab7,tab8 = st.tabs([
        "📊 Hoy","📅 Histórico & KPIs","🔍 Buscar Aliado",
        "🔥 Estado CRM","📤 Cargar Base","🎯 Asignación","⚙️ Reglas","🗺️ Cobertura por Zona",
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
        st.subheader("🔍 Buscar Aliado por Cédula")
        cedula_buscar = st.text_input("Ingresa la cédula del aliado", "", key="busq_cedula")
        if cedula_buscar.strip() and base is not None:
            resultado_b = base[base["identificacion"].astype(str)==cedula_buscar.strip()]
            if resultado_b.empty:
                st.warning(f"No se encontró ningún aliado con cédula **{cedula_buscar}**.")
            else:
                fila_b = resultado_b.iloc[0]; st.success("✅ Aliado encontrado")
                cols_info = [c for c in ["identificacion","mensajero","celular","correo","zona","municipio","vehiculo","categoria",
                                          "dias","intentos","ultimo_resultado","ultimo_estado","proxima_gestion"] if c in fila_b.index]
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
        st.subheader("🔍 Buscar Aliado por Cédula")
        st.caption("Consulta datos, historial y registra una gestión para cualquier aliado.")
        cedula_bus = st.text_input("Ingresa la cédula", "", key="ana_busq_cedula")
        if cedula_bus.strip() and base is not None:
            res_b = base[base["identificacion"].astype(str) == cedula_bus.strip()]
            if res_b.empty:
                st.warning(f"No se encontró ningún aliado con cédula **{cedula_bus}**.")
            else:
                fila_b = res_b.iloc[0]; st.success("✅ Aliado encontrado")
                cols_info = [c for c in ["identificacion","mensajero","celular","zona","municipio","vehiculo","categoria",
                                          "dias","intentos","ultimo_resultado","ultimo_estado","proxima_gestion"] if c in fila_b.index]
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

# ================================================================
# IMPLEMENTACIÓN
# ================================================================
if perfil == "Implementación":
    st.markdown("## ⚙️ Módulo Implementación")
    st.caption("Seguimiento de aliados del 2do al 7mo cargue")

    if nombre == "Coordinador":
        # ── COORDINADOR IMPLEMENTACIÓN ──────────────────────
        df_impl = _get_impl()
        hist_impl = _get_hist_impl()

        tab_res, tab_kpi, tab_carga_impl = st.tabs([
            "📊 Resumen Implementación",
            "📈 KPIs y análisis",
            "📤 Cargar base",
        ])

        with tab_res:
            st.subheader("Resumen — Implementación")
            if st.button("🔄 Actualizar", key="impl_ref_coord"):
                _get_impl(force=True); _get_hist_impl(force=True); st.rerun()
            if df_impl is None:
                st.warning("Carga la base de Implementación primero.")
            else:
                activos     = df_impl[~df_impl.get("estado_impl", pd.Series(dtype=str)).astype(str).str.contains("Abandona|Completó", na=False)]
                completados = df_impl[df_impl.get("estado_impl", pd.Series(dtype=str)).astype(str).str.contains("Completó", na=False)]
                abandonaron = df_impl[df_impl.get("estado_impl", pd.Series(dtype=str)).astype(str).str.contains("Abandona", na=False)]
                c1,c2,c3,c4 = st.columns(4)
                c1.metric("Total base", len(df_impl))
                c2.metric("Activos en seguimiento", len(activos))
                c3.metric("Completaron 7 cargues", len(completados), delta=f"{round(len(completados)/max(len(df_impl),1)*100,1)}%")
                c4.metric("Abandonaron", len(abandonaron), delta=f"-{round(len(abandonaron)/max(len(df_impl),1)*100,1)}%")
                st.markdown("---"); st.markdown("#### Distribución por cargues actuales")
                hist_c = df_impl["total_cargues"].value_counts().sort_index().reset_index(); hist_c.columns = ["Cargues","Aliados"]
                st.plotly_chart(px.bar(hist_c,x="Cargues",y="Aliados",title="¿En qué cargue están los aliados?",color_discrete_sequence=["#534AB7"]),use_container_width=True)
                alta = df_impl[df_impl["prioridad_impl"] == "🔴 ALTA"].copy()
                if not alta.empty:
                    st.markdown(f"---\n#### 🔴 Alta prioridad — {len(alta)} aliados en 2do cargue")
                    cols_a=[c for c in ["identificacion","nombre","celular","zona","vehiculo_norm","total_cargues","cargues_faltantes","analista_impl","estado_impl"] if c in alta.columns]
                    st.dataframe(alta[cols_a],use_container_width=True,hide_index=True)
                listos = df_impl[df_impl["total_cargues"] >= CARGUES_META_IMPL].copy()
                if not listos.empty:
                    st.markdown(f"---\n#### ✅ {len(listos)} aliados lograron 7 cargues — listos para Programación")
                    cols_l=[c for c in ["identificacion","nombre","celular","zona","vehiculo_norm","total_cargues"] if c in listos.columns]
                    st.dataframe(listos[cols_l],use_container_width=True,hide_index=True)
                    st.download_button("📥 Descargar listos para Programación",listos.to_csv(index=False).encode("utf-8"),"listos_programacion.csv","text/csv")

        with tab_kpi:
            st.subheader("KPIs Implementación")
            if df_impl is None:
                st.warning("Carga la base primero.")
            else:
                total=len(df_impl); conv_7=len(df_impl[df_impl["total_cargues"]>=CARGUES_META_IMPL])
                pct_conv=round(conv_7/max(total,1)*100,1)
                abd_count=len(df_impl[df_impl.get("estado_impl",pd.Series(dtype=str)).astype(str).str.contains("Abandona",na=False)])
                pct_abnd=round(abd_count/max(total,1)*100,1)
                c1,c2,c3=st.columns(3)
                c1.metric("% conversión a 7 cargues",f"{pct_conv}%")
                c2.metric("% abandono",f"{pct_abnd}%")
                c3.metric("Cargue promedio base activa",f"{df_impl['total_cargues'].mean():.1f}" if total else "N/A")
                st.markdown("---")
                if "zona" in df_impl.columns:
                    st.markdown("#### Conversión por zona")
                    zona_g=df_impl.groupby("zona").agg(total=("identificacion","count"),completaron=("total_cargues",lambda x:(x>=CARGUES_META_IMPL).sum())).reset_index()
                    zona_g["% conv"]=(zona_g["completaron"]/zona_g["total"]*100).round(1)
                    st.dataframe(zona_g.sort_values("% conv",ascending=False),use_container_width=True,hide_index=True)
                if "vehiculo_norm" in df_impl.columns:
                    st.markdown("#### Conversión por vehículo")
                    veh_g=df_impl.groupby("vehiculo_norm").agg(total=("identificacion","count"),completaron=("total_cargues",lambda x:(x>=CARGUES_META_IMPL).sum())).reset_index()
                    veh_g["% conv"]=(veh_g["completaron"]/veh_g["total"]*100).round(1)
                    st.dataframe(veh_g.sort_values("% conv",ascending=False),use_container_width=True,hide_index=True)
                if not hist_impl.empty:
                    st.markdown("---"); st.markdown("#### Razones de abandono")
                    abd=hist_impl[hist_impl["estado"].astype(str).str.contains("Abandona",na=False)]
                    if not abd.empty:
                        rz=abd["razon"].value_counts().reset_index(); rz.columns=["Razón","Cantidad"]
                        st.dataframe(rz,use_container_width=True,hide_index=True)

        with tab_carga_impl:
            st.subheader("📤 Cargar base de Implementación")
            st.info("**Columnas esperadas:** `identificacion`, `nombre`, `celular`, `vehiculo`, `zona`, `total_cargues`, `fecha_ultimo_cargue`")
            archivo_impl = st.file_uploader("Excel (.xlsx)", type=["xlsx"], key="uploader_impl")
            if archivo_impl:
                try:
                    df_s = pd.read_excel(archivo_impl, engine="openpyxl")
                    df_s = df_s[[c for c in df_s.columns if not str(c).startswith("Unnamed")]].fillna("")
                    st.success(f"{len(df_s):,} registros leídos"); st.dataframe(df_s.head(5),use_container_width=True)
                    if st.button("🚀 Cargar a Implementación"):
                        with st.spinner("Procesando..."):
                            nn, na, nc = cargar_base_implementacion(df_s)
                        st.success(f"✅ {nn} nuevos · {na} actualizados · {nc} ya completaron 7 cargues")
                except Exception as e:
                    st.error(f"Error: {e}")

    else:
        # ── ANALISTA IMPLEMENTACIÓN ─────────────────────────
        df_impl  = _get_impl()
        hist_impl = _get_hist_impl()

        if df_impl is None:
            st.warning("La base de Implementación no está cargada todavía.")
            st.stop()

        tab_mis_impl, tab_buscar_impl, tab_hoy_impl = st.tabs([
            "📞 Mis aliados — Implementación",
            "🔍 Buscar aliado",
            "📊 Mi resumen de hoy",
        ])

        with tab_mis_impl:
            if "analista_impl" in df_impl.columns:
                mis = df_impl[df_impl["analista_impl"].astype(str) == nombre].copy()
            else:
                mis = df_impl.copy()

            mis = mis[~mis.get("estado_impl", pd.Series(dtype=str)).astype(str).str.contains("Completó|Abandona", na=False)]
            mis = mis[mis.get("proxima_gestion_impl", pd.Series(dtype=str)).astype(str).str.upper() != "NO_VOLVER"]

            def disp_impl(v):
                v = str(v).strip()
                if v in ("","nan","None","0","NO_VOLVER"): return True
                f = pd.to_datetime(v, errors="coerce")
                return pd.isna(f) or f <= now_col()
            if "proxima_gestion_impl" in mis.columns:
                mis = mis[mis["proxima_gestion_impl"].apply(disp_impl)]

            gestionados_hoy = _get_gestionados_hoy_todos()
            if not hist_impl.empty:
                hh = hist_impl.copy(); hh["fecha"] = pd.to_datetime(hh["fecha"], errors="coerce"); hh = hh.dropna(subset=["fecha"])
                ya_impl_hoy = set(hh[hh["fecha"].dt.date == now_col().date()]["identificacion"].astype(str).tolist())
                gestionados_hoy = gestionados_hoy | ya_impl_hoy

            mis["_ya_hoy"] = mis["identificacion"].astype(str).isin(gestionados_hoy)
            pendientes     = mis[~mis["_ya_hoy"]].copy()
            ya_gestionados = mis[mis["_ya_hoy"]].copy()

            orden = {"🔴 ALTA":0,"🟡 MEDIA":1,"🟢 BAJA":2}
            pendientes["_ord"] = pendientes["prioridad_impl"].map(orden).fillna(3)
            pendientes = pendientes.sort_values("_ord").drop(columns=["_ord","_ya_hoy"]).reset_index(drop=True)

            hechas_impl = st.session_state.get("impl_hechas", 0)
            pend_n = len(pendientes)
            pct_impl = int(hechas_impl / max(hechas_impl + pend_n, 1) * 100)
            st.progress(pct_impl, text=f"Progreso: {hechas_impl} gestionados · {pend_n} pendientes")

            if not ya_gestionados.empty:
                with st.expander(f"⚠️ {len(ya_gestionados)} aliados ya gestionados hoy en otro módulo — puedes gestionarlos igual"):
                    cols_dup=[c for c in ["identificacion","nombre","celular","total_cargues","prioridad_impl"] if c in ya_gestionados.columns]
                    st.dataframe(ya_gestionados[cols_dup],use_container_width=True,hide_index=True)

            cols_v=[c for c in ["identificacion","nombre","celular","zona","vehiculo_norm","total_cargues","cargues_faltantes","prioridad_impl","estado_impl"] if c in pendientes.columns]
            st.markdown(f"#### Pendientes ({pend_n})")
            st.dataframe(pendientes[cols_v],use_container_width=True,hide_index=True)

            st.markdown("---"); st.markdown("#### 📞 Registrar gestión")
            todos_ids = pendientes["identificacion"].astype(str).tolist()
            if not ya_gestionados.empty:
                todos_ids += ya_gestionados["identificacion"].astype(str).tolist()

            if not todos_ids:
                st.info("✅ Sin aliados pendientes.")
            else:
                with st.form("form_impl", clear_on_submit=True):
                    c1, c2 = st.columns(2)
                    with c1: ali_i=st.selectbox("Cédula del aliado",todos_ids); res_i=st.selectbox("Resultado de la llamada",RESULTADOS_IMPL)
                    with c2: est_i=st.selectbox("Estado",["-"]+ESTADOS_IMPL); raz_i=st.selectbox("Razón",["-"]+RAZONES_IMPL)
                    fd_i = mis[mis["identificacion"].astype(str)==str(ali_i)]
                    if fd_i.empty: fd_i = df_impl[df_impl["identificacion"].astype(str)==str(ali_i)]
                    if not fd_i.empty:
                        f_i=fd_i.iloc[0]; st.markdown("**Ficha del aliado**")
                        ficha_cols=[c for c in ["nombre","celular","total_cargues","cargues_faltantes","prioridad_impl","fecha_ultimo_cargue","estado_impl"] if c in f_i.index]
                        cols_fi=st.columns(min(len(ficha_cols),4))
                        for i,cn in enumerate(ficha_cols): cols_fi[i%4].metric(cn.replace("_"," ").title(),str(f_i[cn]))
                        if str(ali_i) in gestionados_hoy:
                            st.warning("⚠️ Este aliado ya fue gestionado hoy en otro módulo.")
                    obs_i=st.text_area("Observaciones"); sub_i=st.form_submit_button("💾 GUARDAR GESTIÓN")
                if sub_i:
                    er_i=None if est_i=="-" else est_i; rr_i=None if raz_i=="-" else raz_i
                    if res_i=="Sí contestó" and er_i is None:
                        st.error("Selecciona el estado del aliado.")
                    else:
                        tc_mi = int(fd_i.iloc[0].get("total_cargues",0)) if not fd_i.empty else 0
                        guardar_gestion_impl({"analista":nombre,"identificacion":ali_i,"resultado":res_i,
                                               "estado":er_i,"razon":rr_i,"obs":obs_i,"total_cargues_momento":tc_mi})
                        st.session_state["impl_hechas"] = st.session_state.get("impl_hechas",0)+1
                        if str(er_i)=="Llegó al 7mo cargue" or tc_mi>=CARGUES_META_IMPL:
                            st.success(f"🏆 ¡{ali_i} completó los 7 cargues! Pasa a Programación.")
                        else:
                            st.success(f"✅ Guardado para {ali_i}.")
                        st.rerun()

        with tab_buscar_impl:
            st.subheader("🔍 Buscar aliado en Implementación")
            cedula_bi = st.text_input("Cédula", "", key="impl_buscar_cc")
            if cedula_bi.strip():
                fila_bi = df_impl[df_impl["identificacion"].astype(str)==cedula_bi.strip()]
                if fila_bi.empty:
                    st.warning(f"No se encontró {cedula_bi} en Implementación.")
                else:
                    f_bi=fila_bi.iloc[0]; st.success("✅ Aliado encontrado")
                    cols_bi=[c for c in ["identificacion","nombre","celular","zona","vehiculo","total_cargues","cargues_faltantes",
                                          "fecha_ultimo_cargue","estado_impl","intentos_impl","proxima_gestion_impl","analista_impl"] if c in f_bi.index]
                    c1,c2=st.columns(2); mid=len(cols_bi)//2
                    with c1:
                        for col in cols_bi[:mid]: st.metric(col.replace("_"," ").title(),str(f_bi[col]))
                    with c2:
                        for col in cols_bi[mid:]: st.metric(col.replace("_"," ").title(),str(f_bi[col]))
                    st.markdown("---")
                    if not hist_impl.empty:
                        h_bi=hist_impl[hist_impl["identificacion"].astype(str)==cedula_bi.strip()].copy()
                        if h_bi.empty:
                            st.info("Sin gestiones en Implementación para este aliado.")
                        else:
                            h_bi["Hora"]=h_bi["fecha"].dt.strftime("%d/%m/%Y %I:%M %p")
                            st.dataframe(h_bi[["Hora","analista","resultado","estado","razon","obs","total_cargues_momento"]].rename(
                                columns={"analista":"Analista","resultado":"Resultado","estado":"Estado","razon":"Razón","obs":"Obs","total_cargues_momento":"Cargues"}),use_container_width=True,hide_index=True)

        with tab_hoy_impl:
            st.subheader(f"Mi resumen de hoy — {now_col().strftime('%d/%m/%Y')}")
            if st.button("🔄 Actualizar",key="impl_ref_analista"):
                _get_hist_impl(force=True); st.rerun()
            if hist_impl.empty:
                st.info("Sin gestiones registradas aún.")
            else:
                mh_i=hist_impl[(hist_impl["analista"]==nombre)&(hist_impl["fecha"].dt.date==now_col().date())].copy()
                if mh_i.empty:
                    st.info("Sin gestiones hoy. ¡Empieza en Mis Aliados!")
                else:
                    t_i=len(mh_i); sc_i=len(mh_i[mh_i["resultado"]=="Sí contestó"])
                    nr_i=len(mh_i[mh_i["resultado"].isin(NO_RESP_IMPL)])
                    c7_i=len(mh_i[mh_i["estado"].astype(str).str.contains("7mo cargue",na=False)])
                    c1,c2,c3,c4=st.columns(4)
                    c1.metric("📞 Llamadas",t_i); c2.metric("✅ Contactados",sc_i); c3.metric("📵 No resp.",nr_i); c4.metric("🏆 7mo cargue",c7_i)
                    st.markdown("---"); mh_i["Hora"]=mh_i["fecha"].dt.strftime("%I:%M %p")
                    st.dataframe(mh_i[["Hora","identificacion","resultado","estado","razon","obs"]].rename(
                        columns={"identificacion":"Cédula","resultado":"Resultado","estado":"Estado","razon":"Razón","obs":"Obs"}),use_container_width=True,hide_index=True)
                    st.download_button("📥 Descargar hoy",mh_i.to_csv(index=False).encode("utf-8"),f"impl_hoy_{now_col().date()}.csv","text/csv")
