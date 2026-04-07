import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
import plotly.express as px

# ================= 1. CONFIGURACIÓN Y CONSTANTES =================
st.set_page_config(layout="wide", page_title="Gestión Programación Aliados v4.0")

ANALISTAS = {
    "Deisy Liliana Garcia": "dgarcia@clicoh.com",
    "Erica Tatiana Garzon": "etgarzon@clicoh.com",
    "Dayan Stefany Suarez": "dsuarez@clicoh.com",
    "Carlos Andres Loaiza": "cloaiza@clicoh.com",
}

RESULTADOS = ["Apagado", "Fuera de servicio", "No contestó", "Número errado", "Sí contestó"]
ESTADOS_FINALES = ["Aliado Rechaza la oferta", "Aliado Fleet/Delivery no acepta hub", "Interesado llega a cargue", "Interesado esporádico", "Empleado", "Point"]
RAZONES = ["Interesado carga hoy", "No le interesa / cuestiones personales", "No tiene Vh / Vh dañado", "Peso / Volumen / recorrido", "Tarifa", "Tiene trabajo fijo", "Fuera de la ciudad", "Aliado no carga en HUB", "Ocasional", "Empleado", "Point"]

NO_RESPONDEN = ["Apagado", "Fuera de servicio", "No contestó", "Número errado"]

# ================= 2. CONEXIÓN Y FUNCIONES DE DATOS =================
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

@st.cache_resource
def conectar_sheets():
    creds_dict = dict(st.secrets["gcp_service_account"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds).open("GestionAliados")

def leer_hoja(nombre_hoja):
    try:
        sh = conectar_sheets()
        ws = sh.worksheet(nombre_hoja)
        data = ws.get_all_values()
        if len(data) < 1: return pd.DataFrame()
        df = pd.DataFrame(data[1:], columns=[c.strip().lower() for c in data[0]])
        return df
    except: return pd.DataFrame()

def reemplazar_hoja(nombre_hoja, df):
    sh = conectar_sheets()
    ws = sh.worksheet(nombre_hoja)
    ws.clear()
    if not df.empty:
        ws.update([df.columns.tolist()] + df.astype(str).values.tolist())

def agregar_filas(nombre_hoja, rows):
    sh = conectar_sheets()
    ws = sh.worksheet(nombre_hoja)
    ws.append_rows(rows, value_input_option="USER_ENTERED")

# ================= 3. LÓGICA CRM Y PRIORIDADES =================

def calcular_proxima_gestion(resultado, estado, razon, intentos):
    hoy = datetime.now()
    if estado in ["Aliado Rechaza la oferta", "Empleado", "Point"] or razon == "No le interesa / cuestiones personales":
        return "NO_VOLVER"
    if resultado in NO_RESPONDEN:
        if intentos >= 10: return (hoy + timedelta(days=30)).strftime("%Y-%m-%d")
        return (hoy + timedelta(days=1 if resultado == "No contestó" else 2)).strftime("%Y-%m-%d")
    if estado in ["Interesado llega a cargue", "Aliado Fleet/Delivery no acepta hub"]:
        return (hoy + timedelta(days=5)).strftime("%Y-%m-%d")
    return (hoy + timedelta(days=3)).strftime("%Y-%m-%d")

def prioridad_label(dias):
    try: dias = int(dias)
    except: return "🟢 BAJA"
    if dias > 5: return "🔴 ALTA"
    elif dias > 1: return "🟡 MEDIA"
    return "🟢 BAJA"

def normalizar_vehiculo(v):
    v = str(v).lower()
    if any(k in v for k in ["carry", "largenvan", "large van", "small van", "van"]): return "Carry / Van"
    if "moto" in v: return "Moto"
    if any(k in v for k in ["camion", "camión", "truck", "npr"]): return "Camión"
    return str(v).title()

# ================= 4. PROCESAMIENTO DE BASE =================

def cargar_base_preparada():
    df = leer_hoja("BASE")
    if df.empty: return df
    
    # Alias de columnas
    if "identificacion" not in df.columns:
        for a in ["id_aliado", "cedula", "documento"]:
            if a in df.columns: df.rename(columns={a: "identificacion"}, inplace=True)
    
    # Días sin cargar
    if "fecha_ultimo_cargue" in df.columns:
        df["_f_c"] = pd.to_datetime(df["fecha_ultimo_cargue"], errors="coerce")
        df["dias"] = (datetime.now() - df["_f_c"]).dt.days.fillna(0).astype(int)
    else: df["dias"] = 0
    
    # Vehículo
    if "vehiculo" in df.columns:
        df["vehiculo_norm"] = df["vehiculo"].apply(normalizar_vehiculo)
    else: df["vehiculo_norm"] = "Sin Vehículo"
    
    # Asegurar columnas CRM
    for c in ["intentos", "proxima_gestion", "zona"]:
        if c not in df.columns: df[c] = 0 if c == "intentos" else ""
    
    return df

# ================= 5. UI PRINCIPAL =================

st.sidebar.title("Navegación")
perfil = st.sidebar.radio("Ir a:", ["📊 Coordinador (KPIs)", "👨‍💻 Analistas"])

base = cargar_base_preparada()
hist = leer_hoja("HISTORICO")
if not hist.empty: hist["fecha"] = pd.to_datetime(hist["fecha"], errors="coerce")

# ----------------- PERFIL COORDINADOR -----------------
if perfil == "📊 Coordinador (KPIs)":
    st.title("📈 Panel de Control Estratégico")
    
    if not hist.empty:
        hoy = hist[hist["fecha"].dt.date == datetime.now().date()]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Llamadas Hoy", len(hoy))
        contactados = len(hoy[hoy["resultado"] == "Sí contestó"])
        c2.metric("% Contactabilidad", f"{(contactados/len(hoy)*100 if len(hoy)>0 else 0):.1f}%")
        c3.metric("Interesados", len(hoy[hoy["estado"] == "Interesado llega a cargue"]))
        
        # Gráfica
        prod = hoy.groupby("analista").size().reset_index(name='cuenta')
        fig = px.bar(prod, x='analista', y='cuenta', title="Gestiones por Analista (Hoy)")
        st.plotly_chart(fig, use_container_width=True)

    tab_carga, tab_config = st.tabs(["📤 Cargar Base", "🎯 Asignación"])
    
    with tab_carga:
        archivo = st.file_uploader("Subir Excel (.xlsx)", type=["xlsx"])
        if archivo and st.button("🚀 Sincronizar Base en la Nube"):
            df_nuevo = pd.read_excel(archivo).rename(columns=lambda x: str(x).lower().strip())
            reemplazar_hoja("BASE", df_nuevo)
            st.success("Base actualizada exitosamente.")

    with tab_config:
        st.subheader("Configurar asignación de hoy")
        if not base.empty:
            zonas = sorted(base["zona"].unique())
            data_conf = []
            for a in ANALISTAS.keys():
                col1, col2 = st.columns(2)
                z = col1.selectbox(f"Zona para {a}", zonas, key=f"z_{a}")
                v = col2.selectbox(f"Vehículo para {a}", ["Carry / Van", "Moto", "Camión"], key=f"v_{a}")
                data_conf.append({"analista": a, "zona": z, "vehiculo": v})
            if st.button("💾 Guardar Reparto"):
                reemplazar_hoja("CONFIG", pd.DataFrame(data_conf))
                st.success("Asignación guardada.")

# ----------------- PERFIL ANALISTA -----------------
elif perfil == "👨‍💻 Analistas":
    st.title("📞 Gestión de Aliados")
    nombre = st.selectbox("Analista", list(ANALISTAS.keys()))
    
    # Leer configuración asignada
    conf = leer_hoja("CONFIG")
    mi_conf = conf[conf["analista"] == nombre]
    
    if not mi_conf.empty:
        zona_asig = mi_conf.iloc[0]["zona"]
        vh_asig = mi_conf.iloc[0]["vehiculo"]
        st.success(f"🎯 Tu asignación: **{zona_asig}** | **{vh_asig}**")
        
        # Filtrar Pool (Inteligencia CRM + Prioridad)
        pool = base[(base["zona"] == zona_asig) & (base["vehiculo_norm"] == vh_asig)].copy()
        
        # Filtro de disponibilidad
        def esta_disponible(f):
            if f == "" or pd.isna(f) or f == "NO_VOLVER": return f != "NO_VOLVER"
            return pd.to_datetime(f).date() <= datetime.now().date()
        
        pool = pool[pool["proxima_gestion"].apply(esta_disponible)]
        pool["PRIORIDAD"] = pool["dias"].apply(prioridad_label)
        
        st.info(f"Aliados disponibles para llamar ahora: **{len(pool)}**")

        if not pool.empty:
            with st.form("f_gestion", clear_on_submit=True):
                aliado_id = st.selectbox("Seleccionar Aliado (ID)", pool["identificacion"].tolist())
                
                # Historial rápido
                if not hist.empty:
                    h_a = hist[hist["identificacion"].astype(str) == str(aliado_id)].sort_values("fecha", ascending=False).head(2)
                    if not h_a.empty: st.table(h_a[["fecha", "resultado", "obs"]])

                res = st.selectbox("Resultado", RESULTADOS)
                est = st.selectbox("Estado", ["-"] + ESTADOS_FINALES)
                raz = st.selectbox("Razón", ["-"] + RAZONES)
                obs = st.text_area("Observación")
                
                if st.form_submit_button("✅ Guardar"):
                    if len(obs) < 5: st.error("Escribe una observación válida.")
                    else:
                        # 1. Histórico
                        agregar_filas("HISTORICO", [[str(datetime.now()), nombre, aliado_id, res, est, raz, obs]])
                        
                        # 2. Actualizar Base Viva
                        idx = base[base["identificacion"].astype(str) == str(aliado_id)].index
                        intentos = int(pd.to_numeric(base.loc[idx, "intentos"], errors='coerce').fillna(0).values[0]) + 1
                        proxima = calcular_proxima_gestion(res, est, raz, intentos)
                        
                        base.loc[idx, "intentos"] = intentos
                        base.loc[idx, "proxima_gestion"] = proxima
                        base.loc[idx, "ultimo_resultado"] = res
                        base.loc[idx, "fecha_gestion"] = datetime.now().strftime("%Y-%m-%d")
                        
                        reemplazar_hoja("BASE", base)
                        st.success("Gestión exitosa.")
                        st.rerun()
