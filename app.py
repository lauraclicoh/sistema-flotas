import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

st.set_page_config(layout="wide", page_title="Gestión Programación Aliados")

# ================= CONFIGURACIÓN Y CONSTANTES =================
ANALISTAS = {
    "Deisy Liliana Garcia":  "dgarcia@clicoh.com",
    "Erica Tatiana Garzon":  "etgarzon@clicoh.com",
    "Dayan Stefany Suarez":  "dsuarez@clicoh.com",
    "Carlos Andres Loaiza":  "cloaiza@clicoh.com",
}
NOMBRES_ANALISTAS = list(ANALISTAS.keys())

RESULTADOS = ["Apagado", "Fuera de servicio", "No contestó", "Número errado", "Sí contestó"]
ESTADOS_FINALES = ["Aliado Rechaza la oferta", "Aliado Fleet/Delivery no acepta hub", "Interesado llega a cargue", "Interesado esporádico", "Empleado", "Point"]
RAZONES = ["Interesado carga hoy", "No le interesa / cuestiones personales", "No tiene Vh / Vh dañado", "Peso / Volumen / recorrido", "Tarifa", "Tiene trabajo fijo", "Fuera de la ciudad", "Aliado no carga en HUB", "Ocasional", "Empleado", "Point"]

NO_RESPONDEN = ["Apagado", "Fuera de servicio", "No contestó", "Número errado"]
NO_VOLVER_ESTADOS = ["Aliado Rechaza la oferta", "Empleado", "Point"]
NO_VOLVER_RAZONES = ["No le interesa / cuestiones personales"]

# Límite de intentos configurable
MAX_INTENTOS = 6 

# ================= GOOGLE SHEETS =================
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

@st.cache_resource
def conectar_sheets():
    creds_dict = dict(st.secrets["gcp_service_account"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open("GestionAliados")

def leer_hoja(nombre_hoja, esperado_cols=None):
    try:
        sh = conectar_sheets()
        ws = sh.worksheet(nombre_hoja)
        data = ws.get_all_records()
        if data:
            return pd.DataFrame(data)
        return pd.DataFrame(columns=esperado_cols or [])
    except Exception as e:
        st.error(f"Error leyendo {nombre_hoja}: {e}")
        return pd.DataFrame(columns=esperado_cols or [])

def agregar_filas(nombre_hoja, rows: list):
    try:
        sh = conectar_sheets()
        ws = sh.worksheet(nombre_hoja)
        ws.append_rows(rows, value_input_option="USER_ENTERED")
    except Exception as e:
        st.error(f"Error guardando en {nombre_hoja}: {e}")

def reemplazar_hoja(nombre_hoja, df: pd.DataFrame):
    try:
        sh = conectar_sheets()
        ws = sh.worksheet(nombre_hoja)
        ws.clear()
        if not df.empty:
            ws.update([df.columns.tolist()] + df.astype(str).values.tolist())
    except Exception as e:
        st.error(f"Error reemplazando {nombre_hoja}: {e}")

# ================= LÓGICA CRM =================

def calcular_proxima_gestion(resultado, estado, razon, intentos):
    hoy = datetime.now()
    if estado in NO_VOLVER_ESTADOS or razon in NO_VOLVER_RAZONES or intentos >= MAX_INTENTOS:
        return "NO_VOLVER"

    if resultado in ["No contestó", "Apagado", "Fuera de servicio", "Número errado"]:
        if intentos >= 10: return hoy + timedelta(days=30)
        if resultado == "No contestó": return hoy + timedelta(days=1)
        return hoy + timedelta(days=2)

    if estado in ["Interesado llega a cargue", "Aliado Fleet/Delivery no acepta hub"]:
        return hoy + timedelta(days=5)

    return hoy + timedelta(days=3)

def prioridad_label(dias):
    try: dias = int(dias)
    except: return "🟢 BAJA"
    if dias > 5: return "🔴 ALTA"
    elif dias > 1: return "🟡 MEDIA"
    return "🟢 BAJA"

def normalizar_vehiculo(v):
    v = str(v).lower()
    if any(k in v for k in ["carry", "largenvan", "van"]): return "Carry / Van"
    elif "moto" in v: return "Moto"
    elif any(k in v for k in ["camion", "truck", "npr"]): return "Camión"
    return str(v).title()

# ================= CARGA Y PROCESAMIENTO =================

@st.cache_data(ttl=300)
def cargar_base():
    df = leer_hoja("BASE")
    if df.empty: return None

    df.columns = df.columns.str.strip().str.lower()

    # Arreglo para evitar el KeyError: identificacion
    if "identificacion" not in df.columns:
        for alias in ["id_aliado", "id", "cedula", "documento", "identificación"]:
            if alias in df.columns:
                df = df.rename(columns={alias: "identificacion"})
                break
    
    if "celular" not in df.columns:
        for alias in ["telefono", "tel", "phone"]:
            if alias in df.columns:
                df["celular"] = df[alias]
                break

    if "zona" not in df.columns and "municipio" in df.columns:
        df["zona"] = df["municipio"]

    if "vehiculo" in df.columns:
        df["vehiculo_norm"] = df["vehiculo"].apply(normalizar_vehiculo)
    else:
        df["vehiculo_norm"] = "Sin vehículo"

    # Días sin cargar
    col_fecha = next((p for p in ["fecha_ultimo_cargue", "fecha ultimo cargue"] if p in df.columns), None)
    if col_fecha:
        df["_fecha_cargue"] = pd.to_datetime(df[col_fecha].astype(str), errors="coerce")
        df["dias"] = (datetime.now() - df["_fecha_cargue"]).dt.days.fillna(0).astype(int)
    else:
        df["dias"] = 0

    # Inicializar columnas CRM
    for col in ["intentos", "ultimo_resultado", "ultimo_estado", "ultima_razon", "fecha_gestion", "proxima_gestion"]:
        if col not in df.columns:
            df[col] = 0 if col == "intentos" else ""

    df["intentos"] = pd.to_numeric(df["intentos"], errors="coerce").fillna(0).astype(int)
    return df

@st.cache_data(ttl=60)
def cargar_hist():
    df = leer_hoja("HISTORICO", ["fecha", "analista", "identificacion", "resultado", "estado", "razon", "obs"])
    if not df.empty and "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    return df

# ================= GUARDADO OPTIMIZADO =================

def actualizar_base_optima(identificacion, resultado, estado, razon):
    """Actualiza solo la fila específica en Google Sheets."""
    try:
        sh = conectar_sheets()
        ws = sh.worksheet("BASE")
        celda = ws.find(str(identificacion))
        headers = [h.strip().lower() for h in ws.row_values(1)]
        
        # Obtener intentos actuales
        col_int = headers.index("intentos") + 1
        intentos_act = int(ws.cell(celda.row, col_int).value or 0)
        nuevo_intento = intentos_act + 1
        
        proxima = calcular_proxima_gestion(resultado, estado, razon, nuevo_intento)
        
        # Batch update para no saturar la API
        updates = []
        mapping = {
            "ultimo_resultado": resultado, "ultimo_estado": estado, 
            "ultima_razon": razon, "fecha_gestion": str(datetime.now()),
            "intentos": nuevo_intento, "proxima_gestion": str(proxima)
        }
        for k, v in mapping.items():
            if k in headers:
                col_idx = headers.index(k) + 1
                updates.append({'range': gspread.utils.rowcol_to_a1(celda.row, col_idx), 'values': [[str(v)]]})
        
        ws.batch_update(updates)
        cargar_base.clear()
    except Exception as e:
        st.error(f"Error en actualización: {e}")

def filtrar_pool(df_pool):
    """Aplica reglas CRM: Recontacto, NO_VOLVER e Intentos Máximos."""
    if "proxima_gestion" not in df_pool.columns: return df_pool
    df = df_pool.copy()
    
    # 1. Filtro Intentos
    df = df[df["intentos"].astype(int) < MAX_INTENTOS]
    
    # 2. Filtro NO_VOLVER
    df = df[df["proxima_gestion"].astype(str).str.upper() != "NO_VOLVER"]

    # 3. Filtro Fecha
    def es_disponible(val):
        val = str(val).strip()
        if val in ("", "nan", "None"): return True
        try:
            fecha = pd.to_datetime(val, errors="coerce")
            return True if pd.isna(fecha) else fecha <= datetime.now()
        except: return True

    return df[df["proxima_gestion"].apply(es_disponible)]

# ================= UI PRINCIPAL =================
st.title("🚚 Gestión Programación de Aliados")
perfil = st.selectbox("Perfil", ["Coordinador", "Analista"])

if perfil == "Coordinador":
    clave = st.text_input("Clave coordinador", type="password")
    if clave != "clicoh":
        if clave: st.error("Clave incorrecta")
        st.stop()

    base = cargar_base()
    hist = cargar_hist()

    tab1, tab2, tab3, tab4 = st.tabs(["📊 Hoy", "📅 Histórico", "🔥 Estado CRM", "📤 Cargar Base"])

    with tab4:
        st.subheader("Sincronización de Base")
        archivo = st.file_uploader("Archivo Excel", type=["xlsx"])
        if archivo:
            df_nuevo = pd.read_excel(archivo).rename(columns=lambda x: str(x).strip().lower())
            # Evitar error de identificacion en la subida
            for alias in ["id_aliado", "id", "cedula", "documento"]:
                if alias in df_nuevo.columns:
                    df_nuevo = df_nuevo.rename(columns={alias: "identificacion"})
                    break
            
            if st.button("Sincronizar Datos"):
                base_act = cargar_base()
                if base_act is not None:
                    ids_existentes = set(base_act["identificacion"].astype(str))
                    solo_nuevos = df_nuevo[~df_nuevo["identificacion"].astype(str).isin(ids_existentes)]
                    base_final = pd.concat([base_act, solo_nuevos], ignore_index=True)
                    reemplazar_hoja("BASE", base_final)
                    st.success(f"Se añadieron {len(solo_nuevos)} aliados nuevos.")
                else:
                    reemplazar_hoja("BASE", df_nuevo)
                cargar_base.clear()

if perfil == "Analista":
    base = cargar_base()
    hist = cargar_hist()

    if base is None:
        st.warning("Base no disponible.")
        st.stop()

    nombre = st.selectbox("¿Quién eres?", NOMBRES_ANALISTAS)
    zonas = sorted(base["zona"].unique())
    z_sel = st.selectbox("Zona", zonas)
    
    # Pool inteligente con las nuevas reglas
    pool = base[base["zona"] == z_sel].copy()
    pool = filtrar_pool(pool)
    pool["PRIORIDAD"] = pool["dias"].apply(prioridad_label)

    if st.button("🚀 Generar mis llamadas"):
        st.session_state["pool_activo"] = pool.head(20)
        st.session_state["hechas"] = 0

    if "pool_activo" in st.session_state and not st.session_state["pool_activo"].empty:
        pool_act = st.session_state["pool_activo"]
        st.dataframe(pool_act[["identificacion", "celular", "intentos", "PRIORIDAD"]])

        with st.form("form_gestion", clear_on_submit=True):
            aliado_sel = st.selectbox("Cédula", pool_act["identificacion"].tolist())
            
            # --- MOSTRAR GESTIONES ANTERIORES ---
            h_aliado = hist[hist["identificacion"].astype(str) == str(aliado_sel)]
            if not h_aliado.empty:
                st.info("📅 Gestiones Previas")
                st.table(h_aliado.sort_values("fecha", ascending=False)[["fecha", "resultado", "obs"]].head(3))
            
            res = st.selectbox("Resultado", RESULTADOS)
            est = st.selectbox("Estado", ["-"] + ESTADOS_FINALES)
            raz = st.selectbox("Razón", ["-"] + RAZONES)
            obs = st.text_area("Observación")
            
            if st.form_submit_button("Guardar"):
                # 1. Histórico
                agregar_filas("HISTORICO", [[str(datetime.now()), nombre, aliado_sel, res, est, raz, obs]])
                # 2. Base (Optimizado)
                actualizar_base_optima(aliado_sel, res, est, raz)
                
                # Actualizar UI
                st.session_state["pool_activo"] = pool_act[pool_act["identificacion"] != aliado_sel]
                st.rerun()
