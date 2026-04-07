import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

# ================= 1. CONFIGURACIÓN INICIAL =================
st.set_page_config(layout="wide", page_title="CRM Aliados Clicoh")

# Listas para que el analista seleccione (Tus reglas de negocio)
ANALISTAS = ["Deisy Liliana Garcia", "Erica Tatiana Garzon", "Dayan Stefany Suarez", "Carlos Andres Loaiza"]
RESULTADOS = ["Apagado", "Fuera de servicio", "No contestó", "Número errado", "Sí contestó"]
ESTADOS_FINALES = ["Aliado Rechaza la oferta", "Aliado Fleet/Delivery no acepta hub", "Interesado llega a cargue", "Interesado esporádico", "Empleado", "Point"]
RAZONES = ["Interesado carga hoy", "No le interesa / cuestiones personales", "No tiene Vh / Vh dañado", "Peso / Volumen / recorrido", "Tarifa", "Tiene trabajo fijo", "Fuera de la ciudad", "Aliado no carga en HUB", "Ocasional", "Empleado", "Point"]

# ================= 2. CONEXIÓN CON GOOGLE SHEETS =================
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

@st.cache_resource
def conectar_sheets():
    """Se conecta a Google usando las credenciales guardadas en Secrets."""
    creds_dict = dict(st.secrets["gcp_service_account"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds).open("GestionAliados")

def leer_hoja(nombre_hoja):
    """Lee los datos de la hoja y los limpia para evitar errores de columnas vacías."""
    try:
        sh = conectar_sheets()
        ws = sh.worksheet(nombre_hoja)
        lista_datos = ws.get_all_values()
        if not lista_datos:
            return pd.DataFrame()
        
        # Usamos la primera fila como encabezados
        df = pd.DataFrame(lista_datos[1:], columns=lista_datos[0])
        # Limpiamos nombres de columnas (quitar espacios y poner en minúscula)
        df.columns = [c.strip().lower() for c in df.columns]
        return df
    except Exception as e:
        st.error(f"Error leyendo la hoja {nombre_hoja}: {e}")
        return pd.DataFrame()

def escribir_historial(fila):
    """Guarda una nueva línea en la hoja de HISTORICO."""
    sh = conectar_sheets()
    ws = sh.worksheet("HISTORICO")
    ws.append_row(fila, value_input_option="USER_ENTERED")

def guardar_base_completa(df):
    """Sobrescribe la hoja BASE con los datos actualizados."""
    sh = conectar_sheets()
    ws = sh.worksheet("BASE")
    ws.clear()
    ws.update([df.columns.tolist()] + df.astype(str).values.tolist())

# ================= 3. INTELIGENCIA DEL CRM (REGLAS AUTOMÁTICAS) =================

def calcular_proxima_gestion(resultado, estado, razon, intentos):
    """El sistema decide cuándo se debe volver a llamar al aliado."""
    hoy = datetime.now()

    # REGLA: Si no le interesa o es empleado, NO se vuelve a llamar nunca
    if estado in ["Aliado Rechaza la oferta", "Empleado", "Point"] or \
       razon == "No le interesa / cuestiones personales":
        return "NO_VOLVER"

    # REGLA: Si no contestó o está apagado
    if resultado in ["No contestó", "Apagado", "Fuera de servicio"]:
        # Si ya lo intentamos 10 veces, lo pausamos 30 días
        if intentos >= 10:
            return (hoy + timedelta(days=30)).strftime("%Y-%m-%d")
        
        if resultado == "No contestó":
            return (hoy + timedelta(days=1)).strftime("%Y-%m-%d")
        return (hoy + timedelta(days=2)).strftime("%Y-%m-%d")

    # REGLA: Interesados o temas de HUB (esperar 5 días)
    if estado in ["Interesado llega a cargue", "Aliado Fleet/Delivery no acepta hub"]:
        return (hoy + timedelta(days=5)).strftime("%Y-%m-%d")

    # Por defecto, llamar en 3 días
    return (hoy + timedelta(days=3)).strftime("%Y-%m-%d")

def filtrar_por_recontacto(df):
    """Oculta a los aliados que no deben ser llamados hoy."""
    if df.empty or "proxima_gestion" not in df.columns:
        return df
    
    # Quitar los que ya fueron descartados
    df = df[df["proxima_gestion"].astype(str).str.upper() != "NO_VOLVER"]
    
    # Solo mostrar si la fecha de recontacto ya pasó o es hoy
    df["_fecha_dt"] = pd.to_datetime(df["proxima_gestion"], errors="coerce")
    mask = (df["_fecha_dt"].isna()) | (df["_fecha_dt"] <= datetime.now())
    return df[mask].drop(columns=["_fecha_dt"])

# ================= 4. INTERFAZ DE USUARIO =================

perfil = st.sidebar.selectbox("Seleccione Perfil", ["Analista", "Coordinador"])

if perfil == "Analista":
    st.header("📲 Gestión de Llamadas (CRM)")
    
    # Cargamos datos actuales
    base = leer_hoja("BASE")
    historial = leer_hoja("HISTORICO")
    
    if not base.empty:
        nombre_analista = st.selectbox("Analista", ANALISTAS)
        zona_trabajo = st.selectbox("Zona", sorted(base["zona"].unique()))
        
        # Filtramos el "Pool" para que el analista solo vea lo que toca llamar
        pool = base[base["zona"] == zona_trabajo].copy()
        pool_disponible = filtrar_por_recontacto(pool)
        
        st.info(f"Tienes {len(pool_disponible)} aliados pendientes para hoy en {zona_trabajo}.")

        if not pool_disponible.empty:
            with st.form("formulario_llamada", clear_on_submit=True):
                # Selección del aliado a gestionar
                aliado_id = st.selectbox("Identificación del Aliado", pool_disponible["identificacion"].tolist())
                
                # --- MOSTRAR GESTIONES ANTERIORES ---
                if not historial.empty and "identificacion" in historial.columns:
                    previos = historial[historial["identificacion"].astype(str) == str(aliado_id)]
                    if not previos.empty:
                        st.subheader("📜 Historial de este aliado")
                        st.table(previos.sort_values(previos.columns[0], ascending=False).head(3))
                
                st.markdown("---")
                # Campos para llenar la nueva gestión
                res = st.selectbox("Resultado de la llamada", RESULTADOS)
                est = st.selectbox("Estado final", ["-"] + ESTADOS_FINALES)
                raz = st.selectbox("Razón", ["-"] + RAZONES)
                obs = st.text_area("Observaciones de la llamada")
                
                if st.form_submit_button("Guardar y Siguiente"):
                    # 1. Guardar en HISTORICO
                    escribir_historial([str(datetime.now()), nombre_analista, aliado_id, res, est, raz, obs])
                    
                    # 2. Actualizar BASE (Inteligencia CRM)
                    base["identificacion"] = base["identificacion"].astype(str)
                    idx = base[base["identificacion"] == str(aliado_id)].index
                    
                    # Sumar intento y calcular fecha
                    intentos_v = int(pd.to_numeric(base.loc[idx, "intentos"], errors='coerce').fillna(0).values[0]) + 1
                    fecha_proxima = calcular_proxima_gestion(res, est, raz, intentos_v)
                    
                    # Actualizar fila en la tabla
                    base.loc[idx, "intentos"] = intentos_v
                    base.loc[idx, "proxima_gestion"] = fecha_proxima
                    base.loc[idx, "ultimo_resultado"] = res
                    base.loc[idx, "ultimo_estado"] = est
                    base.loc[idx, "fecha_gestion"] = datetime.now().strftime("%Y-%m-%d %H:%M")
                    
                    # Guardar cambios en Google
                    guardar_base_completa(base)
                    
                    st.success(f"Gestión guardada. ¡Siguiente aliado!")
                    st.rerun()

elif perfil == "Coordinador":
    st.header("⚙️ Configuración y Carga de Datos")
    archivo_excel = st.file_uploader("Subir base nueva (Excel)", type=["xlsx"])
    
    if archivo_excel:
        df_nuevo = pd.read_excel(archivo_excel)
        # Limpiar columnas del Excel subido
        df_nuevo.columns = [str(c).strip().lower() for c in df_nuevo.columns]
        
        if st.button("Sincronizar Base Actual"):
            base_actual = leer_hoja("BASE")
            if not base_actual.empty:
                # Evitar duplicados: Solo añadir identificaciones que NO existen
                existentes = set(base_actual["identificacion"].astype(str))
                nuevos_registros = df_nuevo[~df_nuevo["identificacion"].astype(str).isin(existentes)]
                
                base_final = pd.concat([base_actual, nuevos_registros], ignore_index=True)
                guardar_base_completa(base_final)
                st.success(f"Sincronización lista. Se agregaron {len(nuevos_registros)} aliados.")
            else:
                guardar_base_completa(df_nuevo)
                st.success("Base creada desde cero.")
