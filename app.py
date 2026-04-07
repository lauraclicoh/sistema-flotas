import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

# ================= CONFIGURACIÓN DE PÁGINA =================
st.set_page_config(layout="wide", page_title="CRM Aliados - Base Viva")

# --- Constantes de Selección ---
ANALISTAS = ["Deisy Liliana Garcia", "Erica Tatiana Garzon", "Dayan Stefany Suarez", "Carlos Andres Loaiza"]
RESULTADOS = ["Apagado", "Fuera de servicio", "No contestó", "Número errado", "Sí contestó"]
ESTADOS_FINALES = ["Aliado Rechaza la oferta", "Aliado Fleet/Delivery no acepta hub", "Interesado llega a cargue", "Interesado esporádico", "Empleado", "Point"]
RAZONES = ["Interesado carga hoy", "No le interesa / cuestiones personales", "No tiene Vh / Vh dañado", "Peso / Volumen / recorrido", "Tarifa", "Tiene trabajo fijo", "Fuera de la ciudad", "Aliado no carga en HUB", "Ocasional", "Empleado", "Point"]

# ================= CONEXIÓN GOOGLE SHEETS =================
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
        data = ws.get_all_records()
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as e:
        st.error(f"Error al leer {nombre_hoja}: {e}")
        return pd.DataFrame()

def agregar_filas(nombre_hoja, rows):
    sh = conectar_sheets()
    ws = sh.worksheet(nombre_hoja)
    ws.append_rows(rows, value_input_option="USER_ENTERED")

def reemplazar_hoja(nombre_hoja, df):
    sh = conectar_sheets()
    ws = sh.worksheet(nombre_hoja)
    ws.clear()
    if not df.empty:
        # Aseguramos que todo sea string para evitar errores de JSON con fechas
        ws.update([df.columns.tolist()] + df.astype(str).values.tolist())

# ================= 🧠 LÓGICA DE RECONTACTO (EL CORAZÓN) =================

def calcular_proxima_gestion(resultado, estado, razon, intentos):
    """
    Determina la fecha de la siguiente llamada basándose en reglas de negocio.
    """
    hoy = datetime.now()

    # ❌ NO VOLVER: Criterios de exclusión definitiva
    if estado in ["Aliado Rechaza la oferta", "Empleado", "Point"] or \
       razon == "No le interesa / cuestiones personales":
        return "NO_VOLVER"

    # 🔁 GESTIÓN DE NO CONTACTO
    if resultado in ["No contestó", "Apagado", "Fuera de servicio"]:
        # 👉 REGLA ESPECIAL: Pausa larga de 30 días tras 10 intentos
        if intentos >= 10:
            return (hoy + timedelta(days=30)).strftime("%Y-%m-%d")
        
        # Recontactos cortos
        if resultado == "No contestó":
            return (hoy + timedelta(days=1)).strftime("%Y-%m-%d")
        return (hoy + timedelta(days=2)).strftime("%Y-%m-%d")

    # ⏸️ INTERESADOS O TEMAS DE HUB
    if estado in ["Interesado llega a cargue", "Aliado Fleet/Delivery no acepta hub"]:
        return (hoy + timedelta(days=5)).strftime("%Y-%m-%d")

    # Ciclo estándar por defecto (3 días)
    return (hoy + timedelta(days=3)).strftime("%Y-%m-%d")

def filtrar_por_recontacto(df):
    """
    Filtra el pool para que el analista solo vea lo que toca llamar HOY.
    """
    if df.empty or "proxima_gestion" not in df.columns:
        return df

    # 1. Quitar los que ya no se deben llamar
    df = df[df["proxima_gestion"].astype(str).str.upper() != "NO_VOLVER"]

    # 2. Convertir columna a fecha y filtrar por 'hoy o antes'
    df["_tmp_fecha"] = pd.to_datetime(df["proxima_gestion"], errors="coerce")
    
    # Si no tiene fecha (nuevo) o la fecha ya pasó, se muestra
    mask = (df["_tmp_fecha"].isna()) | (df["_tmp_fecha"] <= datetime.now())
    return df[mask].drop(columns=["_tmp_fecha"])

# ================= GESTIÓN DE ACTUALIZACIÓN =================

def actualizar_base(identificacion, estado, razon, resultado):
    """
    Busca al aliado en la BASE, suma un intento y programa la próxima fecha.
    """
    df = leer_hoja("BASE")
    if df.empty: return

    # Normalizar para búsqueda
    df["identificacion"] = df["identificacion"].astype(str).str.strip()
    idx = df[df["identificacion"] == str(identificacion)].index

    if not idx.empty:
        # Calcular intentos (si es nulo empieza en 0 + 1)
        intentos_actual = int(pd.to_numeric(df.loc[idx, "intentos"], errors='coerce').fillna(0).values[0]) + 1
        
        # Calcular fecha inteligente
        proxima = calcular_proxima_gestion(resultado, estado, razon, intentos_actual)

        # Actualizar valores en el DataFrame
        df.loc[idx, "ultimo_resultado"] = resultado
        df.loc[idx, "ultimo_estado"]    = estado
        df.loc[idx, "ultima_razon"]     = razon
        df.loc[idx, "fecha_gestion"]    = datetime.now().strftime("%Y-%m-%d %H:%M")
        df.loc[idx, "intentos"]         = intentos_actual
        df.loc[idx, "proxima_gestion"]  = str(proxima)

        # Guardar cambios
        reemplazar_hoja("BASE", df)
        st.cache_data.clear() # Limpia caché para reflejar cambios inmediatamente

# ================= INTERFAZ DE USUARIO (UI) =================

perfil = st.sidebar.selectbox("Perfil de Usuario", ["Analista", "Coordinador"])

if perfil == "Analista":
    st.subheader("📋 Panel de Gestión de Llamadas")
    
    # Carga de datos
    base = leer_hoja("BASE")
    hist = leer_hoja("HISTORICO")
    
    if not base.empty:
        nombre = st.selectbox("Analista responsable", ANALISTAS)
        zona = st.selectbox("Filtrar por Zona", sorted(base["zona"].unique()))
        
        # --- APLICAR INTELIGENCIA CRM ---
        pool = base[base["zona"] == zona].copy()
        pool_listo = filtrar_por_recontacto(pool)
        
        st.info(f"Tienes **{len(pool_listo)}** aliados pendientes para gestión en esta zona.")

        if not pool_listo.empty:
            with st.form("registro_llamada", clear_on_submit=True):
                # Selección de aliado
                id_aliado = st.selectbox("Seleccione el Aliado (ID)", pool_listo["identificacion"].tolist())
                
                # --- HISTORIAL VISUAL (Lo que se ha validado anteriormente) ---
                st.markdown("---")
                if not hist.empty and "identificacion" in hist.columns:
                    h_aliado = hist[hist["identificacion"].astype(str) == str(id_aliado)]
                    if not h_aliado.empty:
                        st.write("📅 **Últimas gestiones de este aliado:**")
                        st.table(h_aliado.sort_values("fecha", ascending=False)[["fecha", "resultado", "obs"]].head(3))
                    else:
                        st.caption("Este aliado no registra gestiones previas en el histórico.")
                st.markdown("---")
                
                # Datos de la nueva gestión
                res = st.selectbox("Resultado", RESULTADOS)
                est = st.selectbox("Estado", ["-"] + ESTADOS_FINALES)
                raz = st.selectbox("Razón", ["-"] + RAZONES)
                obs = st.text_area("Observaciones detalladas")
                
                if st.form_submit_button("Guardar y Siguiente"):
                    # 1. Guardar rastro en Histórico
                    nueva_gestion = [str(datetime.now()), nombre, id_aliado, res, est, raz, obs]
                    agregar_filas("HISTORICO", [nueva_gestion])
                    
                    # 2. Actualizar la "Base Viva" con recontacto automático
                    actualizar_base(id_aliado, est, raz, res)
                    
                    st.success(f"Gestión de {id_aliado} guardada. El sistema lo ha reprogramado automáticamente.")
                    st.rerun()

elif perfil == "Coordinador":
    st.subheader("⚙️ Administración de Base")
    archivo = st.file_uploader("Subir base Excel para Sincronizar", type=["xlsx"])
    
    if archivo:
        df_nuevo = pd.read_excel(archivo).rename(columns=lambda x: str(x).lower().strip())
        
        if st.button("Sincronizar Datos (Evitar Duplicados)"):
            base_act = leer_hoja("BASE")
            if not base_act.empty:
                # Anti-duplicados por Identificación
                ids_viejos = set(base_act["identificacion"].astype(str))
                solo_nuevos = df_nuevo[~df_nuevo["identificacion"].astype(str).isin(ids_viejos)]
                
                base_final = pd.concat([base_act, solo_nuevos], ignore_index=True)
                reemplazar_hoja("BASE", base_final)
                st.success(f"Sincronización completa. Se añadieron {len(solo_nuevos)} nuevos aliados.")
            else:
                reemplazar_hoja("BASE", df_nuevo)
                st.success("Base cargada por primera vez.")
