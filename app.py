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
    """Hora actual en Colombia."""
    return datetime.now(TZ_COL).replace(tzinfo=None)

# ================= CONSTANTES =================
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

# ================= GOOGLE SHEETS =================
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

# ================= CACHÉ BASE =================
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
            df["vehiculo_norm"] = df["vehiculo"].apply(_norm_vh) if "vehiculo" in df.columns else "Sin vehículo"
            df["dias"] = 0
            col_f = next((c for c in ["fecha_ultimo_cargue","fecha ultimo cargue","fechaultimocargue"]
                          if c in df.columns), None)
            if col_f:
                _serie = df[col_f]
                if isinstance(_serie, pd.DataFrame): _serie = _serie.iloc[:, 0]
                df["_fc"] = pd.to_datetime(_serie.astype(str).str.strip(), dayfirst=True, errors="coerce")
                df["dias"] = (now_col()-df["_fc"]).dt.days.fillna(0).astype(int)
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

# ================= HISTORIAL (TTL 30s) =================
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

# ================= HELPERS =================
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

def calcular_proxima(resultado, estado, razon, intentos):
    hoy    = now_col()
    estado = str(estado or ""); razon = str(razon or "")
    if estado in NO_VOLVER_ESTADOS or razon in NO_VOLVER_RAZONES:
        return "NO_VOLVER"
    if resultado in ["No contestó","Apagado","Fuera de servicio","Número errado"]:
        if intentos >= 10: return hoy + timedelta(days=30)
        if resultado == "No contestó": return hoy + timedelta(days=1)
        if resultado in ["Apagado","Fuera de servicio"]: return hoy + timedelta(days=2)
        if resultado == "Número errado": return hoy + timedelta(days=3)
    if estado in ["Interesado llega a cargue","Aliado Fleet/Delivery no acepta hub"]:
        return hoy + timedelta(days=5)
    return hoy + timedelta(days=3)

def filtrar_pool(df):
    if "proxima_gestion" not in df.columns: return df
    df = df.copy()
    df = df[df["proxima_gestion"].astype(str).str.upper() != "NO_VOLVER"]
    def disponible(v):
        v = str(v).strip()
        if v in ("","nan","None","0"): return True
        f = pd.to_datetime(v, errors="coerce")
        return pd.isna(f) or f <= now_col()
    return df[df["proxima_gestion"].apply(disponible)]

# ================= GUARDADO =================
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
    except Exception as e:
        st.warning(f"CRM no actualizado en BASE: {e}")

def procesar_incremental(df_nuevo):
    base_actual = leer_hoja("BASE")
    df_nuevo = df_nuevo.copy()
    df_nuevo.columns = df_nuevo.columns.str.strip().str.lower()
    col_id = next((a for a in ["identificacion","id_aliado","id","cedula","documento"]
                   if a in df_nuevo.columns), None)
    if not col_id:
        st.error("No se encontró columna de identificación (Cédula/ID)."); return 0,0
    df_nuevo = df_nuevo.rename(columns={col_id:"identificacion"})
    df_nuevo["identificacion"] = df_nuevo["identificacion"].apply(_safe_str)
    df_nuevo = df_nuevo.fillna("")
    if base_actual.empty:
        for col in COLS_CRM: df_nuevo[col] = "0" if col=="intentos" else ""
        reemplazar_hoja("BASE", df_nuevo); _invalidar_base()
        return len(df_nuevo), 0
    base_actual["identificacion"] = base_actual["identificacion"].apply(_safe_str)
    base_actual = base_actual.fillna("")
    # Normalizar columnas de base_actual a minúsculas para que coincidan con df_nuevo
    base_actual.columns = base_actual.columns.str.strip().str.lower()
    ids_viejos = set(base_actual["identificacion"].unique())
    nuevos = df_nuevo[~df_nuevo["identificacion"].isin(ids_viejos)].copy()
    for col in COLS_CRM: nuevos[col] = "0" if col=="intentos" else ""
    cols_operativas = [c for c in df_nuevo.columns if c not in COLS_CRM]
    existentes_datos = (df_nuevo[df_nuevo["identificacion"].isin(ids_viejos)]
                        [cols_operativas].set_index("identificacion"))
    base_idx = base_actual.set_index("identificacion")
    # Actualizar TODAS las columnas operativas incluyendo Categoria, Estado, Dias, Fecha
    for col in cols_operativas:
        if col != "identificacion" and col in existentes_datos.columns:
            base_idx.update(existentes_datos[[col]])
    base_actualizada = base_idx.reset_index()
    base_final = pd.concat([base_actualizada, nuevos], ignore_index=True)
    base_final = base_final.fillna("").loc[:, ~pd.DataFrame(base_final).columns.duplicated()]
    reemplazar_hoja("BASE", base_final); _invalidar_base()
    return len(nuevos), len(existentes_datos)

def leer_config(analista):
    if "config_df" not in st.session_state:
        st.session_state["config_df"] = leer_hoja("CONFIG", ["analista","modo","zona","vehiculo"])
    df = st.session_state["config_df"]
    if df.empty or "analista" not in df.columns: return "Analista decide",None,None
    fila = df[df["analista"]==analista]
    if not fila.empty:
        r = fila.iloc[-1]; return r.get("modo","Analista decide"),r.get("zona"),r.get("vehiculo")
    fila = df[df["analista"]=="TODOS"]
    if not fila.empty:
        r = fila.iloc[-1]; return r.get("modo","Analista decide"),r.get("zona"),r.get("vehiculo")
    return "Analista decide",None,None

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

    tab1,tab2,tab3,tab4,tab5,tab6,tab7 = st.tabs([
        "📊 Hoy","📅 Histórico & KPIs","🔍 Buscar Aliado",
        "🔥 Estado CRM","📤 Cargar Base","🎯 Asignación","⚙️ Reglas",
    ])

    # ─── HOY ───
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
                valor_fecha = (now_col().date()
                               if st.session_state.pop("_reset_fecha_aud", False)
                               else now_col().date())
                fecha_aud = st.date_input("📅 Fecha a auditar", value=valor_fecha,
                                          max_value=now_col().date(), key="coord_fecha_aud")
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
                c3.metric("🚗 Interesados",it); c4.metric("❌ Rechazados",rc)
                c5.metric("📵 No resp.",nr)
                st.markdown("---")
                prod=hf.groupby("analista").size().reset_index(name="llamadas")
                ia=(hf[hf["estado"]=="Interesado llega a cargue"]
                    .groupby("analista").size().reset_index(name="interesados"))
                tp=prod.merge(ia,on="analista",how="left").fillna(0)
                tp["interesados"]=tp["interesados"].astype(int)
                tp["% efectividad"]=(tp["interesados"]/tp["llamadas"]*100).round(1)
                tp["🚦"]=tp.apply(lambda r:"🟢" if r["llamadas"]>=30 and r["interesados"]>=3
                                  else("🟡" if r["llamadas"]>=15 else "🔴"),axis=1)
                st.dataframe(tp,use_container_width=True,hide_index=True)
                st.plotly_chart(px.bar(tp,x="analista",y="llamadas",color="% efectividad",
                                       title=f"Llamadas — {label}"),use_container_width=True)
                st.markdown("---")
                fa,fr,fb=st.columns(3)
                with fa: af=st.multiselect("Analista",NOMBRES_ANALISTAS,default=NOMBRES_ANALISTAS,key="af_c")
                with fr: rf=st.multiselect("Resultado",RESULTADOS,default=RESULTADOS,key="rf_c")
                with fb: bus=st.text_input("Buscar cédula","",key="bus_c")
                df_f=hf[hf["analista"].isin(af)&hf["resultado"].isin(rf)]
                if bus: df_f=df_f[df_f["identificacion"].astype(str).str.contains(bus,na=False)]
                # Hora en Colombia
                df_show = df_f.copy()
                df_show["Hora"] = df_show["fecha"].dt.strftime("%I:%M %p")
                st.dataframe(df_show[["Hora","analista","identificacion","resultado","estado","razon","obs"]].rename(
                    columns={"analista":"Analista","identificacion":"Cédula",
                             "resultado":"Resultado","estado":"Estado","razon":"Razón","obs":"Obs"}
                ),use_container_width=True,hide_index=True)
                st.download_button("📥 Descargar día (CSV)",df_f.to_csv(index=False).encode("utf-8"),
                                   f"gestion_{fecha_aud}.csv","text/csv")

    # ─── HISTÓRICO ───
    with tab2:
        st.subheader("Histórico & KPIs")
        if st.button("🔄 Actualizar historial", key="btn_ref_hist"):
            hist = _get_hist(force_reload=True)
            st.rerun()
        if hist.empty:
            st.info("Sin historial aún.")
        else:
            hv2=hist.dropna(subset=["fecha"])
            c1,c2=st.columns(2)
            with c1: f1=st.date_input("Desde",now_col().date()-timedelta(days=7),
                                       max_value=now_col().date(),key="h_f1")
            with c2: f2=st.date_input("Hasta",now_col().date(),
                                       max_value=now_col().date(),key="h_f2")
            d=hv2[(hv2["fecha"].dt.date>=f1)&(hv2["fecha"].dt.date<=f2)]
            if d.empty:
                st.warning("Sin registros en ese rango.")
            else:
                tot=len(d); sr=d[d["resultado"]=="Sí contestó"]
                nr=d[d["resultado"].isin(NO_RESPONDEN)]
                g=len(sr); it=len(d[d["estado"]=="Interesado llega a cargue"])
                rc=len(d[d["estado"]=="Aliado Rechaza la oferta"])
                c1,c2,c3,c4,c5=st.columns(5)
                c1.metric("📞 Total",tot); c2.metric("✅ Contactados",g)
                c3.metric("% No resp",f"{round(len(nr)/tot*100,1) if tot else 0}%")
                c4.metric("% Gestión",f"{round(g/tot*100,1) if tot else 0}%")
                c5.metric("% Interesados",f"{round(it/tot*100,1) if tot else 0}%")
                c6,c7=st.columns(2)
                c6.metric("% Rechazados",f"{round(rc/tot*100,1) if tot else 0}%")
                c7.metric("% Rechazo/contacto",f"{round(rc/g*100,1) if g else 0}%")
                st.markdown("---")
                emb=pd.DataFrame({"Etapa":["Llamados","Contactados","Interesados"],
                                  "Cantidad":[tot,g,it],
                                  "%":[100,round(g/tot*100,1) if tot else 0,round(it/tot*100,1) if tot else 0]})
                st.dataframe(emb,use_container_width=True)
                st.plotly_chart(px.funnel(emb,x="Cantidad",y="Etapa",title="Embudo"),use_container_width=True)
                st.markdown("---")
                de=[[e,len(sr[sr["estado"]==e]),round(len(sr[sr["estado"]==e])/g*100,1) if g else 0]
                    for e in ESTADOS_FINALES]
                st.markdown("#### Estado final")
                st.dataframe(pd.DataFrame(de,columns=["Estado","N","%"]),use_container_width=True)
                dr=[[r,len(sr[sr["razon"]==r]),round(len(sr[sr["razon"]==r])/g*100,1) if g else 0]
                    for r in RAZONES]
                st.markdown("#### Razones")
                st.dataframe(pd.DataFrame(dr,columns=["Razón","N","%"]),use_container_width=True)
                st.markdown("---"); st.markdown("#### KPIs por analista")
                pa=d.groupby("analista").size().reset_index(name="llamadas")
                ga=d[d["resultado"]=="Sí contestó"].groupby("analista").size().reset_index(name="gest")
                ia=d[d["estado"]=="Interesado llega a cargue"].groupby("analista").size().reset_index(name="inter")
                ra=d[d["estado"]=="Aliado Rechaza la oferta"].groupby("analista").size().reset_index(name="rech")
                na=d[d["resultado"].isin(NO_RESPONDEN)].groupby("analista").size().reset_index(name="noresp")
                ta=(pa.merge(ga,on="analista",how="left").merge(ia,on="analista",how="left")
                      .merge(ra,on="analista",how="left").merge(na,on="analista",how="left").fillna(0))
                for c in ["gest","inter","rech","noresp"]: ta[c]=ta[c].astype(int)
                ta["% gest"]=(ta["gest"]/ta["llamadas"]*100).round(1)
                ta["% inter"]=(ta["inter"]/ta["llamadas"]*100).round(1)
                ta["% rech"]=(ta["rech"]/ta["llamadas"]*100).round(1)
                ta["% noresp"]=(ta["noresp"]/ta["llamadas"]*100).round(1)
                st.dataframe(ta,use_container_width=True)
                st.plotly_chart(px.bar(ta,x="analista",y=["% gest","% inter"],
                                       barmode="group",title="KPIs por Analista"),use_container_width=True)
                tend=d.groupby(d["fecha"].dt.date).size().reset_index(name="llamadas")
                tend.columns=["fecha","llamadas"]
                st.plotly_chart(px.line(tend,x="fecha",y="llamadas",title="Tendencia diaria",markers=True),
                                use_container_width=True)
                # Detalle con vehiculo y ciudad
                d_show = d.copy()
                d_show["Hora"] = d_show["fecha"].dt.strftime("%I:%M %p")
                # Enriquecer con datos de base si está disponible
                if base is not None:
                    cols_extra = [c for c in ["identificacion","vehiculo","municipio","zona"]
                                  if c in base.columns]
                    base_mini = base[cols_extra].copy()
                    base_mini["identificacion"] = base_mini["identificacion"].astype(str)
                    d_show["identificacion"] = d_show["identificacion"].astype(str)
                    d_show = d_show.merge(base_mini, on="identificacion", how="left")
                cols_hist = ["Hora","analista","identificacion","resultado","estado","razon"]
                for extra in ["vehiculo","municipio","zona"]:
                    if extra in d_show.columns: cols_hist.append(extra)
                cols_hist.append("obs")
                st.dataframe(d_show[cols_hist].rename(
                    columns={"analista":"Analista","identificacion":"Cédula",
                             "resultado":"Resultado","estado":"Estado","razon":"Razón",
                             "vehiculo":"Vehículo","municipio":"Ciudad","zona":"Zona","obs":"Obs"}
                ),use_container_width=True,hide_index=True)
                st.download_button("📥 Descargar (CSV)",d_show.to_csv(index=False).encode("utf-8"),
                                   f"historico_{f1}_{f2}.csv","text/csv")

    # ─── BUSCAR ALIADO (nuevo) ───
    with tab3:
        st.subheader("🔍 Buscar Aliado por Cédula")
        cedula_buscar = st.text_input("Ingresa la cédula del aliado", "", key="busq_cedula")
        if cedula_buscar and base is not None:
            resultado_b = base[base["identificacion"].astype(str)==cedula_buscar.strip()]
            if resultado_b.empty:
                st.warning(f"No se encontró ningún aliado con cédula **{cedula_buscar}**.")
            else:
                fila_b = resultado_b.iloc[0]
                st.success(f"✅ Aliado encontrado")
                # Datos del aliado
                cols_info = [c for c in ["identificacion","mensajero","celular","correo",
                                          "zona","municipio","vehiculo","categoria",
                                          "dias","intentos","ultimo_resultado",
                                          "ultimo_estado","proxima_gestion"] if c in fila_b.index]
                c1,c2 = st.columns(2)
                mitad = len(cols_info)//2
                with c1:
                    for col in cols_info[:mitad]:
                        st.metric(col.replace("_"," ").title(), str(fila_b[col]))
                with c2:
                    for col in cols_info[mitad:]:
                        st.metric(col.replace("_"," ").title(), str(fila_b[col]))

                # Historial de gestiones de ese aliado
                st.markdown("---")
                st.markdown("#### 📋 Historial de gestiones")
                hist_aliado = hist[hist["identificacion"].astype(str)==cedula_buscar.strip()].copy()
                if hist_aliado.empty:
                    st.info("Sin gestiones registradas para este aliado.")
                else:
                    hist_aliado["Hora"] = hist_aliado["fecha"].dt.strftime("%d/%m/%Y %I:%M %p")
                    st.dataframe(hist_aliado[["Hora","analista","resultado","estado","razon","obs"]].rename(
                        columns={"analista":"Analista","resultado":"Resultado",
                                 "estado":"Estado","razon":"Razón","obs":"Obs"}
                    ),use_container_width=True,hide_index=True)
        elif cedula_buscar and base is None:
            st.warning("Carga la base primero.")

    # ─── ESTADO CRM ───
    with tab4:
        if base is None:
            st.warning("Carga la base primero.")
        else:
            nv=base[base["proxima_gestion"].astype(str).str.upper()=="NO_VOLVER"]
            disp=filtrar_pool(base)
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
                cp=[c for c in ["identificacion","mensajero","celular","zona","vehiculo",
                                 "intentos","ultimo_resultado","ultimo_estado","proxima_gestion"]
                    if c in paus.columns]
                st.dataframe(paus[cp].sort_values("proxima_gestion"),use_container_width=True)
            if not nv.empty:
                st.markdown("---"); st.markdown("#### 🚫 Bloqueados permanentemente")
                cnv=[c for c in ["identificacion","mensajero","celular","ultimo_estado","ultima_razon"]
                     if c in nv.columns]
                st.dataframe(nv[cnv],use_container_width=True)

    # ─── CARGAR BASE ───
    with tab5:
        st.subheader("📤 Carga de Base")
        st.info("La base permanece en Google Sheets indefinidamente. Usa Incremental para conservar el historial CRM.")
        modo=st.radio("Modo de carga",[
            "🔄 Incremental (recomendado) — conserva historial CRM",
            "♻️ Reemplazar toda la base — borra historial CRM",
        ])
        archivo=st.file_uploader("Excel (.xlsx)",type=["xlsx"])
        if archivo:
            try:
                df_s=pd.read_excel(archivo,engine="openpyxl",dtype=str)
                df_s=df_s.fillna("")
                st.success(f"{len(df_s):,} registros leídos")
                st.dataframe(df_s.head(5),use_container_width=True)
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
                            reemplazar_hoja("BASE",df_s.fillna("")); _invalidar_base()
                        st.success(f"✅ {len(df_s):,} aliados subidos.")
            except Exception as e:
                st.error(f"Error leyendo el archivo: {e}")
        if base is not None:
            st.info(f"Base activa en Google Sheets: **{len(base):,} aliados**")

    # ─── ASIGNACIÓN ───
    with tab6:
        if base is None:
            st.warning("Carga la base primero.")
        else:
            zonas=sorted(base["zona"].dropna().unique())
            vhs=sorted(base["vehiculo_norm"].dropna().unique())
            modo_a=st.selectbox("Modo",[
                "Analista decide","Asignación general (todos igual)","Asignación por analista",
            ])
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
                reemplazar_hoja("CONFIG",pd.DataFrame(dc))
                st.session_state["config_df"] = pd.DataFrame(dc)
                st.success("Guardado.")
            cf = st.session_state.get("config_df", pd.DataFrame())
            if not cf.empty:
                st.markdown("---"); st.markdown("##### Configuración activa:")
                st.dataframe(cf,use_container_width=True)

    # ─── REGLAS ───
    with tab7:
        st.subheader("⚙️ Reglas de recontacto automático")
        st.markdown("""
| Resultado / Estado | Acción | Días espera |
|---|---|---|
| No contestó | Recontacto | **1 día** |
| Apagado / Fuera de servicio | Recontacto | **2 días** |
| Número errado | Recontacto | **3 días** |
| 10+ intentos sin contacto | Pausa larga | **30 días** |
| Interesado llega a cargue | Pausa | **5 días** |
| Fleet no acepta HUB | Pausa | **5 días** |
| Interesado esporádico | Recontacto | **3 días** |
| Aliado Rechaza la oferta | ❌ Bloqueo permanente | Nunca |
| Empleado / Point | ❌ Bloqueo permanente | Nunca |
| No le interesa | ❌ Bloqueo permanente | Nunca |
        """)
        st.info("Estas reglas se aplican automáticamente. Los aliados en pausa vuelven solos cuando se cumple el tiempo.")

# ================================================================
# ANALISTA
# ================================================================
if perfil == "Analista":
    base = _get_base()
    hist = _get_hist()

    if base is None:
        st.warning("⚠️ La coordinadora aún no ha cargado la base. Espera un momento.")
        st.stop()

    tab_g, tab_h, tab_his, tab_bus = st.tabs([
        "📞 Gestión del Día","📊 Mi Resumen de Hoy","📅 Mi Histórico","🔍 Buscar Aliado",
    ])

    with tab_g:
        modo_c,zona_c,vh_c=leer_config(nombre)
        if modo_c in ("Asignación general (todos igual)","Asignación por analista") and zona_c and vh_c:
            zona_sel=str(zona_c); vh_sel=str(vh_c)
            st.success(f"🎯 Hoy: **{zona_sel}** — **{vh_sel}**")
        else:
            zonas=sorted(base["zona"].dropna().unique())
            vhs=sorted(base["vehiculo_norm"].dropna().unique())
            zona_sel=st.selectbox("Zona",zonas)
            vh_sel=st.selectbox("Vehículo",vhs)

        pool=base[(base["zona"].astype(str)==zona_sel)&
                  (base["vehiculo_norm"].astype(str)==vh_sel)].copy()
        pool=filtrar_pool(pool)
        if pool.empty:
            st.info("No hay aliados disponibles. Los aliados en pausa volverán cuando se cumpla su tiempo.")
            st.stop()

        pool["PRIORIDAD"]=pool["dias"].apply(_prio)
        op={"🔴 ALTA":0,"🟡 MEDIA":1,"🟢 BAJA":2}
        pool["_o"]=pool["PRIORIDAD"].map(op).fillna(3)
        pool=pool.sort_values("_o").drop(columns=["_o"]).reset_index(drop=True)

        if not hist.empty:
            hv=hist.copy()
            hv["fecha"]=pd.to_datetime(hv["fecha"],errors="coerce")
            hv=hv.dropna(subset=["fecha"])
            gh=hv[hv["fecha"].dt.date==now_col().date()]["identificacion"].astype(str).tolist()
            pool=pool[~pool["identificacion"].astype(str).isin(gh)]

        c1,c2=st.columns(2)
        with c1: cant=st.number_input("Cantidad de aliados",min_value=10,max_value=300,value=30)
        with c2:
            fp=st.selectbox("Prioridad",[
                "Todas (ALTA + MEDIA + BAJA)","Solo 🔴 ALTA","Solo 🟡 MEDIA","Solo 🟢 BAJA",
            ])
        if fp=="Solo 🔴 ALTA":    pool=pool[pool["PRIORIDAD"]=="🔴 ALTA"]
        elif fp=="Solo 🟡 MEDIA": pool=pool[pool["PRIORIDAD"]=="🟡 MEDIA"]
        elif fp=="Solo 🟢 BAJA":  pool=pool[pool["PRIORIDAD"]=="🟢 BAJA"]
        st.caption(f"Disponibles en este filtro: **{len(pool)}**")

        if st.button("🚀 Generar mis llamadas"):
            hoy_s=now_col().date().isoformat()
            rep=cargar_reparto()
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
                nf=pd.DataFrame({"fecha":[hoy_s]*len(bloque),"analista":[nombre]*len(bloque),
                                 "identificacion":bloque["identificacion"].astype(str).tolist()})
                guardar_reparto(pd.concat([rep,nf],ignore_index=True))
                st.session_state["pool_activo"]=bloque
                st.session_state["hechas"]=0
                st.success(f"✅ {len(bloque)} aliados asignados.")
                st.rerun()

        hoy_s=now_col().date().isoformat()
        rep_act=cargar_reparto()
        mis_ids=[]
        if not rep_act.empty and "fecha" in rep_act.columns and "analista" in rep_act.columns:
            mis_ids=rep_act[(rep_act["fecha"]==hoy_s)&
                            (rep_act["analista"]==nombre)]["identificacion"].astype(str).tolist()
        if mis_ids and not hist.empty:
            hv2=hist.copy()
            hv2["fecha"]=pd.to_datetime(hv2["fecha"],errors="coerce")
            hv2=hv2.dropna(subset=["fecha"])
            gh2=hv2[hv2["fecha"].dt.date==now_col().date()]["identificacion"].astype(str).tolist()
            mis_ids=[i for i in mis_ids if i not in gh2]

        if mis_ids:
            hechas=st.session_state.get("hechas",0)
            rest=len(mis_ids)
            pct=int(hechas/(hechas+rest)*100) if (hechas+rest)>0 else 0
            st.progress(pct,text=f"Progreso: {hechas} gestionados / {rest} pendientes")
            mis_datos=base[base["identificacion"].astype(str).isin(mis_ids)].copy()
            if "PRIORIDAD" not in mis_datos.columns:
                mis_datos["PRIORIDAD"]=mis_datos["dias"].apply(_prio)
            cols_v=[c for c in ["identificacion","mensajero","celular","zona",
                                 "vehiculo","dias","intentos","PRIORIDAD"] if c in mis_datos.columns]
            st.markdown(f"#### 📋 Pendientes ({rest})")
            st.dataframe(mis_datos[cols_v],use_container_width=True,hide_index=True)
            st.markdown("---"); st.markdown("#### 📞 Registrar gestión")
            with st.form("form_g",clear_on_submit=True):
                c1,c2=st.columns(2)
                with c1:
                    ali=st.selectbox("Cédula del aliado",mis_ids)
                    res=st.selectbox("Resultado de la llamada",RESULTADOS)
                with c2:
                    est=st.selectbox("Estado final (si contestó)",["-"]+ESTADOS_FINALES)
                    raz=st.selectbox("Razón (si contestó)",["-"]+RAZONES)
                fd=mis_datos[mis_datos["identificacion"].astype(str)==str(ali)]
                if not fd.empty:
                    f=fd.iloc[0]
                    ic=[c for c in ["mensajero","celular","intentos","PRIORIDAD"] if c in f.index]
                    ci=st.columns(max(len(ic),1))
                    for i,cn in enumerate(ic): ci[i].metric(cn.capitalize(),str(f[cn]))
                obs=st.text_area("Observaciones")
                sub=st.form_submit_button("💾 GUARDAR GESTIÓN")
            if sub:
                er=None if est=="-" else est
                rr=None if raz=="-" else raz
                if res=="Sí contestó" and er is None:
                    st.error("Selecciona un Estado final.")
                else:
                    guardar_gestion({"fecha":now_col(),"analista":nombre,
                                     "identificacion":ali,"resultado":res,
                                     "estado":er,"razon":rr,"obs":obs})
                    with st.spinner("Actualizando CRM..."):
                        actualizar_base_crm(ali,res,er,rr)
                    st.session_state["hechas"]=st.session_state.get("hechas",0)+1
                    st.success("✅ Guardado. Próximo recontacto calculado automáticamente.")
                    st.rerun()
        else:
            st.info("✅ Sin aliados pendientes. Genera un nuevo bloque arriba.")

    with tab_h:
        st.subheader(f"Tus gestiones de hoy — {now_col().strftime('%d/%m/%Y')}")
        if hist.empty:
            st.info("Sin gestiones hoy.")
        else:
            hv3=hist.copy()
            hv3["fecha"]=pd.to_datetime(hv3["fecha"],errors="coerce")
            hv3=hv3.dropna(subset=["fecha"])
            mh=hv3[(hv3["analista"]==nombre)&
                   (hv3["fecha"].dt.date==now_col().date())].copy()
            if mh.empty:
                st.info("Sin gestiones hoy. ¡Empieza en Gestión del Día!")
            else:
                t=len(mh); sc=len(mh[mh["resultado"]=="Sí contestó"])
                it=len(mh[mh["estado"]=="Interesado llega a cargue"])
                nr=len(mh[mh["resultado"].isin(NO_RESPONDEN)])
                c1,c2,c3,c4=st.columns(4)
                c1.metric("📞 Llamadas",t); c2.metric("✅ Contactados",sc)
                c3.metric("🚗 Interesados",it); c4.metric("📵 No resp.",nr)
                if t>0: st.metric("% Efectividad",f"{round(it/t*100,1)}%")
                st.markdown("---")
                mh["Hora"]=mh["fecha"].dt.strftime("%I:%M %p")
                st.dataframe(mh[["Hora","identificacion","resultado","estado","razon","obs"]].rename(
                    columns={"identificacion":"Cédula","resultado":"Resultado",
                             "estado":"Estado","razon":"Razón","obs":"Obs"}
                ),use_container_width=True,hide_index=True)
                st.download_button("📥 Descargar",mh.to_csv(index=False).encode("utf-8"),
                                   f"hoy_{now_col().date()}.csv","text/csv")
                if t>=3:
                    rr=mh.groupby("resultado").size().reset_index(name="n")
                    st.plotly_chart(px.pie(rr,values="n",names="resultado",
                                          title="Distribución de resultados"),use_container_width=True)

    with tab_his:
        st.subheader("Mi Histórico de Gestiones")
        if hist.empty:
            st.info("Sin historial.")
        else:
            hv4=hist.copy()
            hv4["fecha"]=pd.to_datetime(hv4["fecha"],errors="coerce")
            hv4=hv4.dropna(subset=["fecha"])
            mhist=hv4[hv4["analista"]==nombre].copy()
            if mhist.empty:
                st.info("Sin gestiones registradas aún.")
            else:
                c1,c2=st.columns(2)
                with c1: fd=st.date_input("Desde",now_col().date()-timedelta(days=7),
                                           max_value=now_col().date(),key="mh_d")
                with c2: fh=st.date_input("Hasta",now_col().date(),
                                           max_value=now_col().date(),key="mh_h")
                mf=mhist[(mhist["fecha"].dt.date>=fd)&(mhist["fecha"].dt.date<=fh)].copy()
                if mf.empty:
                    st.warning("Sin gestiones en ese rango.")
                else:
                    t=len(mf); sc=len(mf[mf["resultado"]=="Sí contestó"])
                    it=len(mf[mf["estado"]=="Interesado llega a cargue"])
                    nr=len(mf[mf["resultado"].isin(NO_RESPONDEN)])
                    c1,c2,c3,c4=st.columns(4)
                    c1.metric("📞 Total",t); c2.metric("✅ Contactados",sc)
                    c3.metric("🚗 Interesados",it); c4.metric("📵 No resp.",nr)
                    if t>0:
                        c5,c6=st.columns(2)
                        c5.metric("% Contacto",f"{round(sc/t*100,1)}%")
                        c6.metric("% Interesados",f"{round(it/t*100,1)}%")
                    td=mf.groupby(mf["fecha"].dt.date).size().reset_index(name="llamadas")
                    td.columns=["fecha","llamadas"]
                    st.plotly_chart(px.bar(td,x="fecha",y="llamadas",title="Mis llamadas por día"),
                                    use_container_width=True)
                    st.markdown("---")
                    for dia in sorted(mf["fecha"].dt.date.unique(),reverse=True):
                        rd=mf[mf["fecha"].dt.date==dia].copy()
                        lbl="🟢 Hoy" if dia==now_col().date() else dia.strftime("%A %d/%m/%Y").capitalize()
                        with st.expander(f"{lbl} — {len(rd)} gestiones"):
                            rd["Hora"]=rd["fecha"].dt.strftime("%I:%M %p")
                            # Enriquecer con vehiculo y ciudad
                            if base is not None:
                                cols_extra=[c for c in ["identificacion","vehiculo","municipio"]
                                            if c in base.columns]
                                base_mini=base[cols_extra].copy()
                                base_mini["identificacion"]=base_mini["identificacion"].astype(str)
                                rd["identificacion"]=rd["identificacion"].astype(str)
                                rd=rd.merge(base_mini,on="identificacion",how="left")
                            cols_rd=["Hora","identificacion","resultado","estado","razon"]
                            for extra in ["vehiculo","municipio"]:
                                if extra in rd.columns: cols_rd.append(extra)
                            cols_rd.append("obs")
                            st.dataframe(rd[cols_rd].rename(
                                columns={"identificacion":"Cédula","resultado":"Resultado",
                                         "estado":"Estado","razon":"Razón",
                                         "vehiculo":"Vehículo","municipio":"Ciudad","obs":"Obs"}
                            ),use_container_width=True,hide_index=True)
                    st.download_button("📥 Descargar historial",
                                       mf.to_csv(index=False).encode("utf-8"),
                                       f"historial_{fd}_{fh}.csv","text/csv")

    with tab_bus:
        st.subheader("🔍 Buscar Aliado por Cédula")
        st.caption("Consulta los datos y el historial de cualquier aliado.")
        cedula_bus = st.text_input("Ingresa la cédula", "", key="ana_busq_cedula")
        if cedula_bus.strip() and base is not None:
            res_b = base[base["identificacion"].astype(str) == cedula_bus.strip()]
            if res_b.empty:
                st.warning(f"No se encontró ningún aliado con cédula **{cedula_bus}**.")
            else:
                fila_b = res_b.iloc[0]
                st.success("✅ Aliado encontrado")
                cols_info = [c for c in ["identificacion","mensajero","celular",
                                          "zona","municipio","vehiculo","categoria",
                                          "dias","intentos","ultimo_resultado",
                                          "ultimo_estado","proxima_gestion"]
                             if c in fila_b.index]
                c1, c2 = st.columns(2)
                mitad = len(cols_info) // 2
                with c1:
                    for col in cols_info[:mitad]:
                        st.metric(col.replace("_"," ").title(), str(fila_b[col]))
                with c2:
                    for col in cols_info[mitad:]:
                        st.metric(col.replace("_"," ").title(), str(fila_b[col]))
                st.markdown("---")
                st.markdown("#### 📋 Historial de gestiones")
                hist_ali = hist[hist["identificacion"].astype(str) == cedula_bus.strip()].copy()
                if hist_ali.empty:
                    st.info("Sin gestiones registradas para este aliado.")
                else:
                    hist_ali["Hora"] = hist_ali["fecha"].dt.strftime("%d/%m/%Y %I:%M %p")
                    st.dataframe(hist_ali[["Hora","analista","resultado","estado","razon","obs"]].rename(
                        columns={"analista":"Analista","resultado":"Resultado",
                                 "estado":"Estado","razon":"Razón","obs":"Obs"}
                    ), use_container_width=True, hide_index=True)
        elif cedula_bus.strip() and base is None:
            st.warning("Base no disponible.")
