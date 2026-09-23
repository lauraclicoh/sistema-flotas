import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import date
import pytz
from datetime import datetime

# ============================================================
# CONFIGURACION
# ============================================================
SHEET_NAME = "RANKING_ALIADOS"          # Nombre del Google Sheet (debes crearlo y compartirlo con el service account)
TAB_HISTORICO = "HISTORICO_CARGAS"      # Pestana donde se acumulan todos los cortes
TZ = pytz.timezone("America/Bogota")
UMBRAL_RUTAS_DEFAULT = 10               # Rutas minimas para entrar al ranking (ajustable en la app)

# Columnas finales que se guardan en Sheets (orden fijo)
COLUMNAS_SHEET = [
    "Fecha_Carga", "Aliado", "Identificacion", "Categoria", "Proveedor",
    "Ciudad", "Vehiculo", "Total_Paq", "Cumplimiento", "Uso_Boton_VPA",
    "Efectividad", "Rendim_Global",
]

# Mapeo de columnas del archivo que subes -> columnas internas de la app
# Si tu archivo cambia de nombres de columna en el futuro, solo ajusta este diccionario.
MAPEO_COLUMNAS_ORIGEN = {
    "Aliado": "Aliado",
    "identificacion": "Identificacion",
    "Categoria": "Categoria",
    "proveedor": "Proveedor",
    "Ciudad": "Ciudad",
    "vehiculo": "Vehiculo",
    "Total Paq": "Total_Paq",
    "Cumplimiento": "Cumplimiento",
    "Uso Boton VPA": "Uso_Boton_VPA",
    "Efectividad": "Efectividad",
    "Rendim Global": "Rendim_Global",
}

CATEGORIAS_ORDEN = ["Diamante", "Platino", "Oro", "Plata", "Bronce", "Cobre"]

st.set_page_config(page_title="Ranking de Aliados", page_icon="📦", layout="wide")


# ============================================================
# CONEXION A GOOGLE SHEETS
# ============================================================
@st.cache_resource
def conectar_sheets():
    """Autentica contra Google Sheets usando el service account de st.secrets."""
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.readonly",
        ],
    )
    return gspread.authorize(creds)


def obtener_hoja():
    """Devuelve el worksheet de historico, creandolo si no existe."""
    gc = conectar_sheets()
    sh = gc.open(SHEET_NAME)
    try:
        ws = sh.worksheet(TAB_HISTORICO)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=TAB_HISTORICO, rows=3000, cols=len(COLUMNAS_SHEET) + 2)
        ws.append_row(COLUMNAS_SHEET)
    return ws


def leer_historico(forzar_recarga=False):
    """Lee todo el historico desde Sheets y lo cachea en session_state."""
    if not forzar_recarga and "df_historico" in st.session_state and not st.session_state.get("stale", True):
        return st.session_state["df_historico"]

    ws = obtener_hoja()
    valores = ws.get_all_values()  # get_all_values (no get_all_records) para evitar errores con encabezados duplicados/vacios

    if len(valores) <= 1:
        df = pd.DataFrame(columns=COLUMNAS_SHEET)
    else:
        df = pd.DataFrame(valores[1:], columns=valores[0])
        df["Total_Paq"] = pd.to_numeric(df["Total_Paq"], errors="coerce").fillna(0).astype(int)
        for c in ["Cumplimiento", "Uso_Boton_VPA", "Efectividad", "Rendim_Global"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
        df["Fecha_Carga"] = pd.to_datetime(df["Fecha_Carga"], errors="coerce").dt.date

    st.session_state["df_historico"] = df
    st.session_state["stale"] = False
    return df


def marcar_stale():
    st.session_state["stale"] = True


# ============================================================
# CARGA Y NORMALIZACION DE ARCHIVOS
# ============================================================
def leer_archivo_subido(archivo):
    """Lee CSV (detecta separador automaticamente) o Excel."""
    nombre = archivo.name.lower()
    if nombre.endswith(".csv"):
        # sep=None + engine='python' detecta ';' o ',' automaticamente
        df = pd.read_csv(archivo, sep=None, engine="python")
    elif nombre.endswith((".xlsx", ".xls")):
        df = pd.read_excel(archivo)
    else:
        raise ValueError("Formato no soportado. Sube un archivo .csv o .xlsx")
    return df


def normalizar_archivo(df_origen, fecha_corte):
    """Renombra columnas al esquema interno y valida que no falte nada."""
    faltantes = [c for c in MAPEO_COLUMNAS_ORIGEN if c not in df_origen.columns]
    if faltantes:
        raise ValueError(
            f"Al archivo le faltan estas columnas esperadas: {faltantes}. "
            f"Columnas encontradas: {list(df_origen.columns)}"
        )

    df = df_origen.rename(columns=MAPEO_COLUMNAS_ORIGEN)
    df = df[list(MAPEO_COLUMNAS_ORIGEN.values())].copy()
    df.insert(0, "Fecha_Carga", fecha_corte.isoformat())

    # Tipos numericos limpios
    df["Total_Paq"] = pd.to_numeric(df["Total_Paq"], errors="coerce").fillna(0).astype(int)
    for c in ["Cumplimiento", "Uso_Boton_VPA", "Efectividad", "Rendim_Global"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    return df


def cargar_a_sheets(df_nuevo):
    """
    Sube el corte nuevo a Sheets. Si ya existia un corte con la misma
    Fecha_Carga, lo REEMPLAZA (evita duplicados al recargar por error).
    """
    ws = obtener_hoja()
    df_actual = leer_historico(forzar_recarga=True)

    fecha_str = df_nuevo["Fecha_Carga"].iloc[0]

    if not df_actual.empty:
        df_actual_sin_fecha = df_actual[df_actual["Fecha_Carga"].astype(str) != fecha_str].copy()
    else:
        df_actual_sin_fecha = df_actual

    df_actual_sin_fecha["Fecha_Carga"] = df_actual_sin_fecha["Fecha_Carga"].astype(str)
    df_final = pd.concat([df_actual_sin_fecha, df_nuevo], ignore_index=True)

    # Reescritura completa de la hoja (simple y segura para volumenes de cientos de filas;
    # si el historico crece mucho, se puede migrar a batch_update quirurgico como en tus otras apps)
    ws.clear()
    ws.append_row(COLUMNAS_SHEET)
    ws.append_rows(df_final[COLUMNAS_SHEET].values.tolist())

    marcar_stale()
    return len(df_nuevo)


# ============================================================
# LOGICA DE RANKING
# ============================================================
def filtrar_base(df, fecha_sel, umbral_rutas, categorias_sel, ciudades_sel):
    d = df[df["Fecha_Carga"] == fecha_sel].copy()
    d = d[d["Total_Paq"] >= umbral_rutas]
    if categorias_sel:
        d = d[d["Categoria"].isin(categorias_sel)]
    if ciudades_sel:
        d = d[d["Ciudad"].isin(ciudades_sel)]
    return d


def top_bottom(df, n=10, por_ciudad=None):
    d = df if por_ciudad is None else df[df["Ciudad"] == por_ciudad]
    d = d.sort_values("Efectividad", ascending=False)
    top = d.head(n)
    bottom = d.tail(n).sort_values("Efectividad", ascending=True)
    return top, bottom


# ============================================================
# UI
# ============================================================
st.title("📦 Ranking de Efectividad de Aliados")
st.caption("clicOH — Programación de flota | Última y primera milla")

tab_cargar, tab_ranking, tab_comparar, tab_categorias = st.tabs(
    ["📤 Cargar Corte", "📊 Ranking", "📈 Comparación entre fechas", "🏷️ Categorías"]
)

# ---------------------------------------------------------------
# TAB 1: CARGAR CORTE
# ---------------------------------------------------------------
with tab_cargar:
    st.subheader("Cargar un nuevo corte de datos")
    st.markdown(
        "- Sube el archivo con las columnas: `Aliado, identificacion, Categoria, proveedor, "
        "Ciudad, vehiculo, Total Paq, Cumplimiento, Uso Boton VPA, Efectividad, Rendim Global`\n"
        "- **Selecciona la fecha del corte** (no se infiere del nombre del archivo, para evitar errores)\n"
        "- Si ya existe un corte cargado con esa misma fecha, **se reemplaza automáticamente** (no se duplica)"
    )

    col1, col2 = st.columns([2, 1])
    with col1:
        archivo = st.file_uploader("Archivo del corte (.csv o .xlsx)", type=["csv", "xlsx", "xls"])
    with col2:
        fecha_corte = st.date_input("Fecha del corte", value=date.today())

    if archivo is not None:
        try:
            df_origen = leer_archivo_subido(archivo)
            df_normalizado = normalizar_archivo(df_origen, fecha_corte)

            st.success(f"Archivo leído correctamente: {len(df_normalizado)} aliados detectados.")
            st.dataframe(df_normalizado.head(10), use_container_width=True)

            if st.button("✅ Cargar a Google Sheets", type="primary"):
                with st.spinner("Cargando..."):
                    n = cargar_a_sheets(df_normalizado)
                st.success(f"Se cargaron {n} registros para la fecha {fecha_corte.isoformat()}.")
                st.rerun()

        except ValueError as e:
            st.error(str(e))
        except Exception as e:
            st.error(f"No se pudo procesar el archivo: {e}")

    st.divider()
    st.caption("Cortes ya cargados en el histórico:")
    df_hist = leer_historico()
    if df_hist.empty:
        st.info("Todavía no hay cortes cargados.")
    else:
        resumen = df_hist.groupby("Fecha_Carga").agg(
            Aliados=("Aliado", "count"),
            Efectividad_prom=("Efectividad", "mean"),
        ).reset_index().sort_values("Fecha_Carga", ascending=False)
        resumen["Efectividad_prom"] = (resumen["Efectividad_prom"] * 100).round(1)
        st.dataframe(resumen, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------
# TAB 2: RANKING
# ---------------------------------------------------------------
with tab_ranking:
    df_hist = leer_historico()

    if df_hist.empty:
        st.info("Carga al menos un corte en la pestaña anterior para ver el ranking.")
    else:
        fechas_disponibles = sorted(df_hist["Fecha_Carga"].dropna().unique(), reverse=True)
        c1, c2, c3 = st.columns([1, 1, 1])
        with c1:
            fecha_sel = st.selectbox("Fecha del corte", fechas_disponibles)
        with c2:
            umbral_rutas = st.slider("Rutas mínimas para entrar al ranking", 0, 100, UMBRAL_RUTAS_DEFAULT)
        with c3:
            n_top = st.selectbox("Tamaño del Top/Bottom", [5, 10, 15, 20], index=1)

        categorias_sel = st.multiselect("Filtrar por categoría", CATEGORIAS_ORDEN)
        ciudades_todas = sorted(df_hist[df_hist["Fecha_Carga"] == fecha_sel]["Ciudad"].unique())
        ciudades_sel = st.multiselect("Filtrar por ciudad (vacío = todas)", ciudades_todas)

        df_filtrado = filtrar_base(df_hist, fecha_sel, umbral_rutas, categorias_sel, ciudades_sel)

        if df_filtrado.empty:
            st.warning("No hay aliados que cumplan estos filtros (revisa el umbral de rutas).")
        else:
            # --- KPIs ---
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Aliados en ranking", len(df_filtrado))
            k2.metric("Efectividad promedio", f"{df_filtrado['Efectividad'].mean()*100:.1f}%")
            k3.metric("Total rutas", int(df_filtrado["Total_Paq"].sum()))
            k4.metric(
                "Aliados activos",
                int((df_filtrado["Total_Paq"] > 0).sum()),
                help="Supuesto: activo = Total_Paq > 0 en este corte. Ajustar si existe otra fuente de estado.",
            )

            st.divider()

            # --- Top/Bottom Nacional ---
            st.markdown("### 🏆 Top / Bottom Nacional")
            top_nac, bottom_nac = top_bottom(df_filtrado, n=n_top)
            colA, colB = st.columns(2)
            with colA:
                st.markdown(f"**Top {n_top} — mejor efectividad**")
                st.dataframe(
                    top_nac[["Aliado", "Ciudad", "Categoria", "Total_Paq", "Efectividad"]]
                    .assign(Efectividad=lambda d: (d["Efectividad"] * 100).round(1)),
                    use_container_width=True, hide_index=True,
                )
            with colB:
                st.markdown(f"**Bottom {n_top} — peor efectividad**")
                st.dataframe(
                    bottom_nac[["Aliado", "Ciudad", "Categoria", "Total_Paq", "Efectividad"]]
                    .assign(Efectividad=lambda d: (d["Efectividad"] * 100).round(1)),
                    use_container_width=True, hide_index=True,
                )

            st.divider()

            # --- Top/Bottom por ciudad ---
            st.markdown("### 🏙️ Top / Bottom por ciudad")
            ciudad_focus = st.selectbox("Elige una ciudad para ver su ranking detallado", ciudades_todas)
            top_c, bottom_c = top_bottom(df_filtrado, n=n_top, por_ciudad=ciudad_focus)
            colC, colD = st.columns(2)
            with colC:
                st.markdown(f"**Top {n_top} en {ciudad_focus}**")
                st.dataframe(
                    top_c[["Aliado", "Categoria", "Total_Paq", "Efectividad", "Rendim_Global"]]
                    .assign(Efectividad=lambda d: (d["Efectividad"] * 100).round(1)),
                    use_container_width=True, hide_index=True,
                )
            with colD:
                st.markdown(f"**Bottom {n_top} en {ciudad_focus}**")
                st.dataframe(
                    bottom_c[["Aliado", "Categoria", "Total_Paq", "Efectividad", "Rendim_Global"]]
                    .assign(Efectividad=lambda d: (d["Efectividad"] * 100).round(1)),
                    use_container_width=True, hide_index=True,
                )

            st.divider()
            with st.expander("📋 Ver base completa filtrada"):
                st.dataframe(
                    df_filtrado.sort_values("Efectividad", ascending=False),
                    use_container_width=True, hide_index=True,
                )
                csv = df_filtrado.to_csv(index=False, sep=";").encode("utf-8")
                st.download_button("Descargar esta vista como CSV", csv, "ranking_filtrado.csv", "text/csv")


# ---------------------------------------------------------------
# TAB 3: COMPARACION ENTRE FECHAS
# ---------------------------------------------------------------
with tab_comparar:
    df_hist = leer_historico()
    fechas_disponibles = sorted(df_hist["Fecha_Carga"].dropna().unique(), reverse=True) if not df_hist.empty else []

    if len(fechas_disponibles) < 2:
        st.info("Necesitas al menos 2 cortes cargados para comparar evolución.")
    else:
        c1, c2 = st.columns(2)
        with c1:
            fecha_a = st.selectbox("Corte más antiguo", fechas_disponibles, index=min(1, len(fechas_disponibles)-1))
        with c2:
            fecha_b = st.selectbox("Corte más reciente", fechas_disponibles, index=0)

        df_a = df_hist[df_hist["Fecha_Carga"] == fecha_a][["Identificacion", "Aliado", "Ciudad", "Efectividad", "Total_Paq"]]
        df_b = df_hist[df_hist["Fecha_Carga"] == fecha_b][["Identificacion", "Aliado", "Ciudad", "Efectividad", "Total_Paq"]]

        comp = df_a.merge(
            df_b, on="Identificacion", how="outer", suffixes=("_ant", "_nuevo")
        )
        comp["Aliado"] = comp["Aliado_nuevo"].fillna(comp["Aliado_ant"])
        comp["Ciudad"] = comp["Ciudad_nuevo"].fillna(comp["Ciudad_ant"])
        comp["Delta_Efectividad"] = (comp["Efectividad_nuevo"] - comp["Efectividad_ant"]) * 100

        nuevos = comp[comp["Efectividad_ant"].isna()]
        salieron = comp[comp["Efectividad_nuevo"].isna()]
        continuos = comp.dropna(subset=["Efectividad_ant", "Efectividad_nuevo"])

        k1, k2, k3 = st.columns(3)
        k1.metric("Aliados nuevos", len(nuevos))
        k2.metric("Aliados que salieron", len(salieron))
        k3.metric("Aliados continuos", len(continuos))

        st.markdown("### 📈 Mayor mejora")
        st.dataframe(
            continuos.sort_values("Delta_Efectividad", ascending=False).head(10)
            [["Aliado", "Ciudad", "Delta_Efectividad"]].round(1),
            use_container_width=True, hide_index=True,
        )

        st.markdown("### 📉 Mayor caída")
        st.dataframe(
            continuos.sort_values("Delta_Efectividad", ascending=True).head(10)
            [["Aliado", "Ciudad", "Delta_Efectividad"]].round(1),
            use_container_width=True, hide_index=True,
        )


# ---------------------------------------------------------------
# TAB 4: CATEGORIAS
# ---------------------------------------------------------------
with tab_categorias:
    df_hist = leer_historico()
    if df_hist.empty:
        st.info("Carga al menos un corte para ver el análisis por categoría.")
    else:
        fechas_disponibles = sorted(df_hist["Fecha_Carga"].dropna().unique(), reverse=True)
        fecha_sel_cat = st.selectbox("Fecha del corte", fechas_disponibles, key="fecha_cat")
        d = df_hist[df_hist["Fecha_Carga"] == fecha_sel_cat]

        resumen_cat = d.groupby("Categoria").agg(
            Aliados=("Aliado", "count"),
            Efectividad_prom=("Efectividad", "mean"),
            Rendim_Global_prom=("Rendim_Global", "mean"),
            Rutas_totales=("Total_Paq", "sum"),
        ).reindex(CATEGORIAS_ORDEN).dropna(how="all").reset_index()
        resumen_cat["Efectividad_prom"] = (resumen_cat["Efectividad_prom"] * 100).round(1)
        resumen_cat["Rendim_Global_prom"] = (resumen_cat["Rendim_Global_prom"] * 100).round(1)

        st.markdown("### 🏷️ Efectividad promedio por categoría")
        st.dataframe(resumen_cat, use_container_width=True, hide_index=True)
        st.bar_chart(resumen_cat.set_index("Categoria")["Efectividad_prom"])

        st.caption(
            "Si una categoría 'alta' (Diamante/Platino) no muestra efectividad claramente mayor "
            "que las categorías bajas, es señal de que el criterio de categorización debería revisarse."
        )
