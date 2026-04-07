import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

# ================= CONFIGURACIÓN Y CONSTANTES =================
st.set_page_config(layout="wide", page_title="CRM Aliados v2.0")

ANALISTAS = {
    "Deisy Liliana Garcia": "dgarcia@clicoh.com",
    "Erica Tatiana Garzon": "etgarzon@clicoh.com",
    "Dayan Stefany Suarez": "dsuarez@clicoh.com",
    "Carlos Andres Loaiza": "cloaiza@clicoh.com",
}
NOMBRES_ANALISTAS = list(ANALISTAS.keys())

RESULTADOS = ["Apagado", "Fuera de servicio", "No contestó", "Número errado", "Sí contestó"]
ESTADOS_FINALES = ["Aliado Rechaza la oferta", "Aliado Fleet/Delivery no acepta hub", "Interesado llega a cargue", "Interesado esporádico", "Empleado", "Point"]
RAZONES = ["Interesado carga hoy", "No le interesa / cuestiones personales", "No tiene Vh / Vh dañado", "Peso / Volumen / recorrido", "Tarifa", "Tiene trabajo fijo", "Fuera de la ciudad", "Aliado no carga en HUB", "Ocasional", "Empleado", "Point"]

NO_VOLVER_ESTADOS = ["Aliado Rechaza la oferta", "Empleado", "Point"]
NO_VOLVER_RAZONES = ["No le interesa / cuestiones personales"]

# Límite de intentos configurable
MAX_INTENTOS = 6 

# ================= CONEXIÓN A GOOGLE SHEETS =================
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

@st.cache_resource
def conectar_sheets():
    creds_dict = dict(st.secrets["gcp_service_account"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds).open("GestionAliados")

def leer_hoja(nombre_hoja, esperado_cols=None):
    try:
        sh = conectar_sheets()
        ws = sh.worksheet(nombre_hoja)
        data = ws.get_all_records()
        return pd.DataFrame(data) if data else pd.DataFrame(columns=esperado_cols or [])
    except:
        return pd.DataFrame(columns=esperado_cols or [])

def agregar_filas(nombre_hoja, rows: list):
    sh = conectar_sheets()
    ws = sh.worksheet(nombre_hoja)
    ws.append_rows(rows, value_input_option="USER_ENTERED")

def reemplazar_hoja(nombre_hoja, df: pd.DataFrame):
    sh = conectar_sheets()
    ws = sh.worksheet(nombre_hoja)
    ws.clear()
    if not df.empty:
        ws.update([df.columns.tolist()] + df.astype(str).values.tolist())

# ================= LÓGICA CRM (CEREBRO) =================

def calcular_proxima_gestion(resultado, estado, razon, intentos):
    hoy = datetime.now()
    if estado in NO_VOLVER_ESTADOS or razon in NO_VOLVER_RAZONES or intentos >= MAX_INTENTOS:
        return "NO_VOLVER"
    
    if resultado in ["No contestó", "Apagado", "Fuera de servicio"]:
        return hoy + timedelta(days=1) if resultado == "No contestó" else hoy + timedelta(days=2)
    
    if estado in ["Interesado llega a cargue", "Aliado Fleet/Delivery no acepta hub"]:
        return hoy + timedelta(days=5)
    
    return hoy + timedelta(days=3)

def filtrar_pool(df_pool):
    """Filtra por fecha, bloqueos e intentos máximos."""
    if df_pool.empty: return df_pool
    df = df_pool.copy()
    
    # 1. Filtro por intentos
    df["intentos"] = pd.to_numeric(df["intentos"], errors='coerce').fillna(0)
    df = df[df["intentos"] < MAX_INTENTOS]
    
    # 2. Filtro NO_VOLVER
    df = df[df["proxima_gestion"].astype(str).str.upper() != "NO_VOLVER"]
    
    # 3. Filtro Fecha
    def es_toca_llamar(val):
        if str(val).strip() in ["", "nan", "None"]: return True
        try:
            f = pd.to_datetime(val, errors='coerce')
            return True if pd.isna(f) else f <= datetime.now()
        except: return True
        
    return df[df["proxima_gestion"].apply(es_toca_llamar)]

# ================= CARGA DE DATOS Y GESTIÓN =================

@st.cache_data(ttl=60)
def cargar_base():
    df = leer_hoja("BASE")
    if df.empty: return None
    df.columns = df.columns.str.strip().str.lower()
    # Asegurar columnas CRM
    for c in ["intentos", "proxima_gestion", "identificacion", "zona", "vehiculo"]:
        if c not in df.columns: df[c] = 0 if c == "intentos" else ""
    return df

@st.cache_data(ttl=30)
def cargar_hist():
    return leer_hoja("HISTORICO", ["fecha", "analista", "identificacion", "resultado", "estado", "razon", "obs"])

def actualizar_registro_base_optimo(identificacion, resultado, estado, razon):
    """Actualiza solo la fila del aliado en la hoja BASE."""
    sh = conectar_sheets()
    ws = sh.worksheet("BASE")
    try:
        celda = ws.find(str(identificacion))
        headers = [h.strip().lower() for h in ws.row_values(1)]
        
        # Obtener intentos actuales
        col_intentos = headers.index("intentos") + 1
        intentos_v = ws.cell(celda.row, col_intentos).value
        nuevo_intento = int(intentos_v or 0) + 1
        
        proxima = calcular_proxima_gestion(resultado, estado, razon, nuevo_intento)
        
        # Update batch de la fila
        updates = []
        campos = {"ultimo_resultado": resultado, "ultimo_estado": estado, "ultima_razon": razon, 
                  "fecha_gestion": str(datetime.now()), "intentos": nuevo_intento, "proxima_gestion": str(proxima)}
        
        for nombre, valor in campos.items():
            if nombre in headers:
                col = headers.index(nombre) + 1
                updates.append({'range': gspread.utils.rowcol_to_a1(celda.row, col), 'values': [[str(valor)]]})
        
        ws.batch_update(updates)
    except Exception as e:
        st.error(f"Error actualizando base: {e}")

# ================= INTERFAZ DE USUARIO =================

st.title("🚚 Sistema de Gestión Aliados (Logística)")
perfil = st.sidebar.selectbox("Perfil", ["Analista", "Coordinador"])

if perfil == "Coordinador":
    tab_carga, tab_kpi = st.tabs(["📤 Cargar/Sincronizar", "📊 Indicadores"])
    
    with tab_carga:
        archivo = st.file_uploader("Subir base Excel", type=["xlsx"])
        if archivo:
            df_nuevo = pd.read_excel(archivo).rename(columns=str.lower)
            if st.button("Sincronizar con Base en la Nube"):
                base_act = cargar_base()
                if base_act is not None:
                    # Anti-duplicados: Solo añadir los que no están
                    ids_existentes = set(base_act["identificacion"].astype(str))
                    nuevos = df_nuevo[~df_nuevo["identificacion"].astype(str).isin(ids_existentes)]
                    base_final = pd.concat([base_act, nuevos], ignore_index=True)
                    reemplazar_hoja("BASE", base_final)
                    st.success(f"Sincronizado. Se añadieron {len(nuevos)} nuevos registros.")
                else:
                    reemplazar_hoja("BASE", df_nuevo)
                cargar_base.clear()

if perfil == "Analista":
    base = cargar_base()
    hist = cargar_hist()
    
    if base is None:
        st.warning("No hay base cargada.")
        st.stop()
        
    nombre = st.selectbox("Analista", NOMBRES_ANALISTAS)
    
    # Filtro de Zona/Vehículo (Resumido para el ejemplo)
    zonas = sorted(base["zona"].unique())
    z_sel = st.selectbox("Zona de trabajo", zonas)
    
    pool = base[base["zona"] == z_sel]
    pool = filtrar_pool(pool)
    
    st.info(f"Aliados disponibles para llamar: {len(pool)}")
    
    if not pool.empty:
        with st.form("gestion"):
            aliado_id = st.selectbox("Seleccionar Aliado (ID)", pool["identificacion"].tolist())
            
            # --- SECCIÓN DE HISTORIAL ANTERIOR ---
            # Aquí es donde el analista ve las gestiones pasadas y sus fechas
            st.markdown("---")
            h_aliado = hist[hist["identificacion"].astype(str) == str(aliado_id)]
            if not h_aliado.empty:
                st.subheader("📜 Gestiones anteriores de este aliado")
                st.table(h_aliado.sort_values("fecha", ascending=False)[["fecha", "resultado", "estado", "obs"]].head(5))
            else:
                st.caption("Este aliado no tiene registros previos en el histórico.")
            st.markdown("---")
            
            res = st.selectbox("Resultado", RESULTADOS)
            est = st.selectbox("Estado", ["-"] + ESTADOS_FINALES)
            raz = st.selectbox("Razón", ["-"] + RAZONES)
            obs = st.text_area("Notas de la llamada")
            
            if st.form_submit_button("Guardar Gestión"):
                # 1. Guardar en HISTORICO (Fila nueva)
                nueva_fila = [str(datetime.now()), nombre, aliado_id, res, est, raz, obs]
                agregar_filas("HISTORICO", [nueva_fila])
                
                # 2. Actualizar BASE (Sobre-escribe solo la fila del aliado)
                actualizar_registro_base_optimo(aliado_id, res, est if est != "-" else "", raz if raz != "-" else "")
                
                st.success("Guardado correctamente.")
                st.rerun()
