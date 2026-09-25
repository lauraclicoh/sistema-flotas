import re
from datetime import date, timedelta

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials

# ============================================================
# CONFIGURACION
# ============================================================
SHEET_NAME = "RANKING_ALIADOS"          # Google Sheet EXCLUSIVO de este proyecto (no el compartido con otras apps)
TAB_RUTAS = "HISTORICO_RUTAS"           # Eventos de ruta por dia (fuente: Diario)
TAB_CONTEXTO = "CONTEXTO_ALIADOS"       # Snapshot mas reciente (fuente: General)
UMBRAL_RUTAS_DEFAULT = 10
DIAS_RANGO_DEFAULT = 60                 # Ventana de fechas por defecto

st.set_page_config(page_title="Ranking de Aliados", page_icon="📦", layout="wide")

# ------------------------------------------------------------
# Columnas de HISTORICO_RUTAS (fuente: Diario)
# ------------------------------------------------------------
COLUMNAS_RUTAS = [
    "Fecha", "HJRT_ID", "Identificacion", "Nombre_Aliado", "Vehiculo",
    "Localidad", "Ciudad", "Estado_HR", "Tot_Paq", "Entregados",
    "Paq_Gestionados", "Pct_Gestion",
]

MAPEO_DIARIO = {
    "Creacion": "Fecha",
    "HJRT ID": "HJRT_ID",
    "Estado HR": "Estado_HR",
    "identificacion": "Identificacion",
    "Nombre Aliado": "Nombre_Aliado",
    "Vehiculo": "Vehiculo",
    "Localidad": "Localidad",
    "Ciudad": "Ciudad",
    "Tot Paq": "Tot_Paq",
    "Entregados": "Entregados",
    "Paq Gestionados por Aliado": "Paq_Gestionados",
    "% Gestion": "Pct_Gestion",
}

# ------------------------------------------------------------
# Columnas de CONTEXTO_ALIADOS (fuente: General)
# ------------------------------------------------------------
COLUMNAS_CONTEXTO = [
    "Identificacion", "Aliado", "Categoria", "Proveedor", "Ciudad_General",
    "Vehiculo_General", "Total_Paq_General", "Cumplimiento", "Uso_Boton_VPA",
    "Efectividad_General", "Rendim_Global", "Fecha_Carga_Contexto",
]

MAPEO_GENERAL = {
    "identificacion": "Identificacion",
    "Aliado": "Aliado",
    "Categoria": "Categoria",
    "proveedor": "Proveedor",
    "Ciudad": "Ciudad_General",
    "vehiculo": "Vehiculo_General",
    "Total Paq": "Total_Paq_General",
    "Cumplimiento": "Cumplimiento",
    "Uso Boton VPA": "Uso_Boton_VPA",
    "Efectividad": "Efectividad_General",
    "Rendim Global": "Rendim_Global",
}

CATEGORIAS_ORDEN = ["Diamante", "Platino", "Oro", "Plata", "Bronce", "Cobre", "Sin categoría"]

MESES_ES = {
    "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
    "jul": 7, "ago": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dic": 12,
}


def parsear_fecha_espanol(texto):
    """Convierte 'sept 23, 2026' -> date(2026, 9, 23). Devuelve None si no matchea."""
    if pd.isna(texto):
        return None
    m = re.match(r"\s*([a-zA-Zé]+)\.?\s+(\d{1,2}),?\s+(\d{4})", str(texto).strip().lower())
    if not m:
        return None
    mes_txt, dia, anio = m.groups()
    mes_txt = mes_txt.replace(".", "")
    mes = MESES_ES.get(mes_txt[:4]) or MESES_ES.get(mes_txt[:3])
    if mes is None:
        return None
    try:
        return date(int(anio), mes, int(dia))
    except ValueError:
        return None


# ============================================================
# CONEXION A GOOGLE SHEETS
# ============================================================
@st.cache_resource
def conectar_sheets():
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.readonly",
        ],
    )
    return gspread.authorize(creds)


def obtener_hoja(nombre_tab, columnas):
    gc = conectar_sheets()
    sh = gc.open(SHEET_NAME)
    try:
        ws = sh.worksheet(nombre_tab)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=nombre_tab, rows=20000, cols=len(columnas) + 2)
        ws.append_row(columnas)
    return ws


def marcar_stale():
    st.session_state["stale_rutas"] = True
    st.session_state["stale_contexto"] = True


# ---------- HISTORICO_RUTAS ----------
def leer_rutas(forzar_recarga=False):
    if not forzar_recarga and "df_rutas" in st.session_state and not st.session_state.get("stale_rutas", True):
        return st.session_state["df_rutas"]

    ws = obtener_hoja(TAB_RUTAS, COLUMNAS_RUTAS)
    valores = ws.get_all_values()
    if len(valores) <= 1:
        df = pd.DataFrame(columns=COLUMNAS_RUTAS)
    else:
        df = pd.DataFrame(valores[1:], columns=valores[0])
        df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce").dt.date
        for c in ["Tot_Paq", "Entregados", "Paq_Gestionados"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
        df["Pct_Gestion"] = pd.to_numeric(df["Pct_Gestion"], errors="coerce").fillna(0.0)
        df["Identificacion"] = df["Identificacion"].astype(str).str.strip()

    st.session_state["df_rutas"] = df
    st.session_state["stale_rutas"] = False
    return df


def limpiar_nombres_columnas(df):
    """Quita BOM invisible y espacios sobrantes de los nombres de columna."""
    df.columns = [str(c).replace("\ufeff", "").strip() for c in df.columns]
    return df


def normalizar_diario(df_origen):
    df_origen = limpiar_nombres_columnas(df_origen)
    faltantes = [c for c in MAPEO_DIARIO if c not in df_origen.columns]
    if faltantes:
        raise ValueError(f"Al archivo Diario le faltan columnas: {faltantes}")

    df = df_origen.rename(columns=MAPEO_DIARIO)
    df = df[list(MAPEO_DIARIO.values())].copy()
    df["Fecha"] = df["Fecha"].apply(parsear_fecha_espanol)
    df["Identificacion"] = df["Identificacion"].astype(str).str.strip()
    df["HJRT_ID"] = df["HJRT_ID"].astype(str).str.strip()
    for c in ["Tot_Paq", "Entregados", "Paq_Gestionados"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    df["Pct_Gestion"] = pd.to_numeric(df["Pct_Gestion"], errors="coerce").fillna(0.0)

    sin_fecha = df["Fecha"].isna().sum()
    if sin_fecha:
        st.warning(f"{sin_fecha} filas no se pudieron leer con fecha valida y se van a descartar.")
        df = df[df["Fecha"].notna()]

    return df


def cargar_rutas_a_sheets(df_nuevo):
    """Agrega filas nuevas a HISTORICO_RUTAS, deduplicando por HJRT_ID."""
    ws = obtener_hoja(TAB_RUTAS, COLUMNAS_RUTAS)
    df_actual = leer_rutas(forzar_recarga=True)

    if not df_actual.empty:
        ids_existentes = set(df_actual["HJRT_ID"].astype(str))
        df_nuevo = df_nuevo[~df_nuevo["HJRT_ID"].astype(str).isin(ids_existentes)]

    if df_nuevo.empty:
        return 0

    df_nuevo_out = df_nuevo.copy()
    df_nuevo_out["Fecha"] = df_nuevo_out["Fecha"].astype(str)
    ws.append_rows(df_nuevo_out[COLUMNAS_RUTAS].values.tolist())

    marcar_stale()
    return len(df_nuevo)


# ---------- CONTEXTO_ALIADOS ----------
def leer_contexto(forzar_recarga=False):
    if not forzar_recarga and "df_contexto" in st.session_state and not st.session_state.get("stale_contexto", True):
        return st.session_state["df_contexto"]

    ws = obtener_hoja(TAB_CONTEXTO, COLUMNAS_CONTEXTO)
    valores = ws.get_all_values()
    if len(valores) <= 1:
        df = pd.DataFrame(columns=COLUMNAS_CONTEXTO)
    else:
        df = pd.DataFrame(valores[1:], columns=valores[0])
        df["Total_Paq_General"] = pd.to_numeric(df["Total_Paq_General"], errors="coerce").fillna(0).astype(int)
        for c in ["Cumplimiento", "Uso_Boton_VPA", "Efectividad_General", "Rendim_Global"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
        df["Identificacion"] = df["Identificacion"].astype(str).str.strip()

    st.session_state["df_contexto"] = df
    st.session_state["stale_contexto"] = False
    return df


def normalizar_general(df_origen, fecha_carga):
    df_origen = limpiar_nombres_columnas(df_origen)
    faltantes = [c for c in MAPEO_GENERAL if c not in df_origen.columns]
    if faltantes:
        raise ValueError(f"Al archivo General le faltan columnas: {faltantes}")

    df = df_origen.rename(columns=MAPEO_GENERAL)
    df = df[list(MAPEO_GENERAL.values())].copy()
    df.insert(len(df.columns), "Fecha_Carga_Contexto", fecha_carga.isoformat())
    df["Identificacion"] = df["Identificacion"].astype(str).str.strip()
    df["Total_Paq_General"] = pd.to_numeric(df["Total_Paq_General"], errors="coerce").fillna(0).astype(int)
    for c in ["Cumplimiento", "Uso_Boton_VPA", "Efectividad_General", "Rendim_Global"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    return df


def cargar_contexto_a_sheets(df_nuevo):
    """Reemplaza COMPLETO el contenido de CONTEXTO_ALIADOS (es una foto, no un historico)."""
    ws = obtener_hoja(TAB_CONTEXTO, COLUMNAS_CONTEXTO)
    ws.clear()
    ws.append_row(COLUMNAS_CONTEXTO)
    ws.append_rows(df_nuevo[COLUMNAS_CONTEXTO].values.tolist())
    marcar_stale()
    return len(df_nuevo)


# ============================================================
# LOGICA DE RANKING
# ============================================================
def calcular_ranking(df_rutas, df_contexto, fecha_ini, fecha_fin, umbral_rutas, categorias_sel, ciudades_sel):
    d = df_rutas[(df_rutas["Fecha"] >= fecha_ini) & (df_rutas["Fecha"] <= fecha_fin)].copy()

    if d.empty:
        return pd.DataFrame()

    agg = d.groupby("Identificacion").agg(
        Nombre_Aliado=("Nombre_Aliado", "last"),
        Ciudad=("Ciudad", "last"),
        Vehiculo=("Vehiculo", "last"),
        Tot_Paq=("Tot_Paq", "sum"),
        Entregados=("Entregados", "sum"),
        Paq_Gestionados=("Paq_Gestionados", "sum"),
        Rutas_en_rango=("HJRT_ID", "nunique"),
        Ultima_fecha=("Fecha", "max"),
    ).reset_index()

    agg["Pct_Gestion"] = (agg["Paq_Gestionados"] / agg["Tot_Paq"].replace(0, pd.NA)).fillna(0.0)

    df = agg.merge(df_contexto, on="Identificacion", how="left")
    df["Categoria"] = df["Categoria"].replace("", pd.NA).fillna("Sin categoría")
    df["Aliado"] = df["Aliado"].fillna(df["Nombre_Aliado"])

    df = df[df["Tot_Paq"] >= umbral_rutas]
    if categorias_sel:
        df = df[df["Categoria"].isin(categorias_sel)]
    if ciudades_sel:
        df = df[df["Ciudad"].isin(ciudades_sel)]

    return df


def top_bottom(df, n=10, por_ciudad=None):
    d = df if por_ciudad is None else df[df["Ciudad"] == por_ciudad]
    d = d.sort_values("Pct_Gestion", ascending=False)
    return d.head(n), d.tail(n).sort_values("Pct_Gestion", ascending=True)


def formatear_tabla(d, cols):
    out = d[cols].copy()
    for c in ["Pct_Gestion", "Efectividad_General", "Cumplimiento", "Rendim_Global"]:
        if c in out.columns:
            out[c] = (pd.to_numeric(out[c], errors="coerce") * 100).round(1)
    return out


# ============================================================
# UI
# ============================================================
st.title("📦 Ranking de Efectividad de Aliados")
st.caption("clicOH — Programación de flota | Última y primera milla")

tab_cargar, tab_ranking, tab_comparar, tab_categorias = st.tabs(
    ["📤 Cargar Datos", "📊 Ranking", "📈 Comparación entre periodos", "🏷️ Categorías"]
)

# ---------------------------------------------------------------
# TAB 1: CARGAR DATOS
# ---------------------------------------------------------------
with tab_cargar:
    st.subheader("1️⃣ Cargar Diario (rutas por día) — base del ranking")
    st.markdown(
        "- Columnas esperadas: `Creacion, HJRT ID, Estado HR, identificacion, Nombre Aliado, Vehiculo, "
        "Localidad, Ciudad, ..., Tot Paq, Entregados, Paq Gestionados por Aliado, % Gestion`\n"
        "- La fecha se lee directamente de la columna `Creacion` (no hay que elegirla manualmente)\n"
        "- Se deduplica por `HJRT ID`: puedes resubir el mismo export sin duplicar filas"
    )
    archivo_diario = st.file_uploader("Archivo Diario (.csv)", type=["csv"], key="up_diario")

    if archivo_diario is not None:
        try:
            df_origen = pd.read_csv(archivo_diario, sep=None, engine="python", encoding="utf-8-sig")
            df_norm = normalizar_diario(df_origen)
            st.success(f"{len(df_norm)} filas válidas leídas, cubriendo {df_norm['Fecha'].nunique()} fechas distintas.")
            st.dataframe(df_norm.head(8), use_container_width=True)

            if st.button("✅ Agregar al histórico de rutas", type="primary", key="btn_diario"):
                with st.spinner("Cargando..."):
                    n = cargar_rutas_a_sheets(df_norm)
                if n == 0:
                    st.info("Todas las filas ya existían (mismo HJRT ID). No se agregó nada nuevo.")
                else:
                    st.success(f"Se agregaron {n} filas nuevas al histórico.")
                st.rerun()
        except ValueError as e:
            st.error(str(e))
        except Exception as e:
            st.error(f"No se pudo procesar el archivo: {e}")

    st.divider()

    st.subheader("2️⃣ Cargar General (contexto) — Categoría, Proveedor, Efectividad")
    st.markdown(
        "- Columnas esperadas: `Aliado, identificacion, Categoria, proveedor, Ciudad, vehiculo, "
        "Total Paq, Cumplimiento, Uso Boton VPA, Efectividad, Rendim Global`\n"
        "- **Esta carga reemplaza por completo el contexto anterior** — es una foto del momento, no un histórico"
    )
    fecha_contexto = st.date_input("Fecha de este corte del General", value=date.today(), key="fecha_gral")
    archivo_general = st.file_uploader("Archivo General (.csv o .xlsx)", type=["csv", "xlsx", "xls"], key="up_general")

    if archivo_general is not None:
        try:
            if archivo_general.name.lower().endswith(".csv"):
                df_origen_g = pd.read_csv(archivo_general, sep=None, engine="python", encoding="utf-8-sig")
            else:
                df_origen_g = pd.read_excel(archivo_general)
            df_norm_g = normalizar_general(df_origen_g, fecha_contexto)
            st.success(f"{len(df_norm_g)} aliados leídos del General.")
            st.dataframe(df_norm_g.head(8), use_container_width=True)

            if st.button("✅ Reemplazar contexto en Google Sheets", type="primary", key="btn_general"):
                with st.spinner("Cargando..."):
                    n = cargar_contexto_a_sheets(df_norm_g)
                st.success(f"Contexto actualizado con {n} aliados.")
                st.rerun()
        except ValueError as e:
            st.error(str(e))
        except Exception as e:
            st.error(f"No se pudo procesar el archivo: {e}")

    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        st.caption("Estado del histórico de rutas:")
        df_r = leer_rutas()
        if df_r.empty:
            st.info("Sin datos aún.")
        else:
            st.write(f"**{len(df_r)}** filas · **{df_r['Identificacion'].nunique()}** aliados · "
                     f"del **{df_r['Fecha'].min()}** al **{df_r['Fecha'].max()}**")
    with c2:
        st.caption("Estado del contexto:")
        df_c = leer_contexto()
        if df_c.empty:
            st.info("Sin datos aún.")
        else:
            fc = df_c["Fecha_Carga_Contexto"].iloc[0] if len(df_c) else "-"
            st.write(f"**{len(df_c)}** aliados · corte del **{fc}**")


# ---------------------------------------------------------------
# TAB 2: RANKING
# ---------------------------------------------------------------
with tab_ranking:
    df_rutas = leer_rutas()
    df_contexto = leer_contexto()

    if df_rutas.empty:
        st.info("Carga primero el archivo Diario en la pestaña anterior.")
    else:
        fecha_max = df_rutas["Fecha"].max()
        fecha_min_default = fecha_max - timedelta(days=DIAS_RANGO_DEFAULT)

        c1, c2, c3, c4 = st.columns([1, 1, 1, 1])
        with c1:
            fecha_ini = st.date_input("Desde", value=max(fecha_min_default, df_rutas["Fecha"].min()))
        with c2:
            fecha_fin = st.date_input("Hasta", value=fecha_max)
        with c3:
            umbral_rutas = st.slider("Rutas mínimas en el rango", 0, 100, UMBRAL_RUTAS_DEFAULT)
        with c4:
            n_top = st.selectbox("Tamaño Top/Bottom", [5, 10, 15, 20], index=1)

        categorias_sel = st.multiselect("Filtrar por categoría", CATEGORIAS_ORDEN)
        ciudades_todas = sorted(df_rutas["Ciudad"].dropna().unique())
        ciudades_sel = st.multiselect("Filtrar por ciudad (vacío = todas)", ciudades_todas)

        df_rank = calcular_ranking(df_rutas, df_contexto, fecha_ini, fecha_fin, umbral_rutas, categorias_sel, ciudades_sel)

        if df_rank.empty:
            st.warning("No hay aliados que cumplan estos filtros en el rango de fechas elegido.")
        else:
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Aliados en ranking", len(df_rank))
            k2.metric("% Gestión promedio", f"{df_rank['Pct_Gestion'].mean()*100:.1f}%")
            k3.metric("Total rutas en el rango", int(df_rank["Rutas_en_rango"].sum()))
            k4.metric(
                "Aliados activos (60 días)",
                int((df_rank["Ultima_fecha"] >= (fecha_max - timedelta(days=60))).sum()),
                help="Activo = tuvo al menos una ruta en los últimos 60 días respecto al dato más reciente cargado.",
            )

            cols_mostrar = ["Aliado", "Ciudad", "Categoria", "Rutas_en_rango", "Tot_Paq",
                             "Pct_Gestion", "Efectividad_General", "Cumplimiento", "Rendim_Global"]

            st.divider()
            st.markdown("### 🏆 Top / Bottom Nacional (por % Gestión)")
            top_nac, bottom_nac = top_bottom(df_rank, n=n_top)
            colA, colB = st.columns(2)
            with colA:
                st.markdown(f"**Top {n_top}**")
                st.dataframe(formatear_tabla(top_nac, cols_mostrar), use_container_width=True, hide_index=True)
            with colB:
                st.markdown(f"**Bottom {n_top}**")
                st.dataframe(formatear_tabla(bottom_nac, cols_mostrar), use_container_width=True, hide_index=True)

            st.divider()
            st.markdown("### 🏙️ Top / Bottom por ciudad")
            ciudad_focus = st.selectbox("Ciudad", sorted(df_rank["Ciudad"].dropna().unique()))
            top_c, bottom_c = top_bottom(df_rank, n=n_top, por_ciudad=ciudad_focus)
            colC, colD = st.columns(2)
            with colC:
                st.markdown(f"**Top {n_top} en {ciudad_focus}**")
                st.dataframe(formatear_tabla(top_c, cols_mostrar), use_container_width=True, hide_index=True)
            with colD:
                st.markdown(f"**Bottom {n_top} en {ciudad_focus}**")
                st.dataframe(formatear_tabla(bottom_c, cols_mostrar), use_container_width=True, hide_index=True)

            st.divider()
            with st.expander("📋 Ver base completa del rango filtrado"):
                st.dataframe(formatear_tabla(df_rank.sort_values("Pct_Gestion", ascending=False), cols_mostrar),
                             use_container_width=True, hide_index=True)
                csv = df_rank.to_csv(index=False, sep=";").encode("utf-8")
                st.download_button("Descargar esta vista como CSV", csv, "ranking_filtrado.csv", "text/csv")


# ---------------------------------------------------------------
# TAB 3: COMPARACION ENTRE PERIODOS
# ---------------------------------------------------------------
with tab_comparar:
    df_rutas = leer_rutas()
    df_contexto = leer_contexto()

    if df_rutas.empty:
        st.info("Carga primero el archivo Diario para poder comparar periodos.")
    else:
        st.markdown("Compara dos rangos de fechas (ej. este mes vs. el mes anterior) para ver quién mejoró o empeoró.")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Periodo A (más antiguo)**")
            a_ini = st.date_input("Desde", key="a_ini", value=df_rutas["Fecha"].min())
            a_fin = st.date_input("Hasta", key="a_fin", value=df_rutas["Fecha"].min() + timedelta(days=29))
        with c2:
            st.markdown("**Periodo B (más reciente)**")
            b_ini = st.date_input("Desde", key="b_ini", value=df_rutas["Fecha"].max() - timedelta(days=29))
            b_fin = st.date_input("Hasta", key="b_fin", value=df_rutas["Fecha"].max())

        umbral_comp = st.slider("Rutas mínimas por periodo para comparar", 0, 100, UMBRAL_RUTAS_DEFAULT, key="umbral_comp")

        df_a = calcular_ranking(df_rutas, df_contexto, a_ini, a_fin, umbral_comp, [], [])
        df_b = calcular_ranking(df_rutas, df_contexto, b_ini, b_fin, umbral_comp, [], [])

        if df_a.empty or df_b.empty:
            st.warning("Alguno de los dos periodos no tiene suficientes datos con este umbral de rutas.")
        else:
            comp = df_a[["Identificacion", "Aliado", "Ciudad", "Pct_Gestion"]].merge(
                df_b[["Identificacion", "Aliado", "Ciudad", "Pct_Gestion"]],
                on="Identificacion", how="outer", suffixes=("_A", "_B"),
            )
            comp["Aliado"] = comp["Aliado_B"].fillna(comp["Aliado_A"])
            comp["Ciudad"] = comp["Ciudad_B"].fillna(comp["Ciudad_A"])
            comp["Delta_Pct_Gestion"] = (comp["Pct_Gestion_B"] - comp["Pct_Gestion_A"]) * 100

            nuevos = comp[comp["Pct_Gestion_A"].isna()]
            salieron = comp[comp["Pct_Gestion_B"].isna()]
            continuos = comp.dropna(subset=["Pct_Gestion_A", "Pct_Gestion_B"])

            k1, k2, k3 = st.columns(3)
            k1.metric("Aliados nuevos en B", len(nuevos))
            k2.metric("Aliados que salieron", len(salieron))
            k3.metric("Aliados continuos", len(continuos))

            colE, colF = st.columns(2)
            with colE:
                st.markdown("### 📈 Mayor mejora")
                st.dataframe(
                    continuos.sort_values("Delta_Pct_Gestion", ascending=False).head(10)
                    [["Aliado", "Ciudad", "Delta_Pct_Gestion"]].round(1),
                    use_container_width=True, hide_index=True,
                )
            with colF:
                st.markdown("### 📉 Mayor caída")
                st.dataframe(
                    continuos.sort_values("Delta_Pct_Gestion", ascending=True).head(10)
                    [["Aliado", "Ciudad", "Delta_Pct_Gestion"]].round(1),
                    use_container_width=True, hide_index=True,
                )


# ---------------------------------------------------------------
# TAB 4: CATEGORIAS
# ---------------------------------------------------------------
with tab_categorias:
    df_rutas = leer_rutas()
    df_contexto = leer_contexto()

    if df_rutas.empty:
        st.info("Carga primero el archivo Diario.")
    else:
        fecha_max = df_rutas["Fecha"].max()
        fecha_min_default = fecha_max - timedelta(days=DIAS_RANGO_DEFAULT)
        c1, c2 = st.columns(2)
        with c1:
            f_ini_cat = st.date_input("Desde", value=max(fecha_min_default, df_rutas["Fecha"].min()), key="f_ini_cat")
        with c2:
            f_fin_cat = st.date_input("Hasta", value=fecha_max, key="f_fin_cat")

        df_rank_cat = calcular_ranking(df_rutas, df_contexto, f_ini_cat, f_fin_cat, 0, [], [])

        if df_rank_cat.empty:
            st.warning("No hay datos en este rango.")
        else:
            resumen_cat = df_rank_cat.groupby("Categoria").agg(
                Aliados=("Aliado", "count"),
                Pct_Gestion_prom=("Pct_Gestion", "mean"),
                Efectividad_General_prom=("Efectividad_General", "mean"),
                Rutas_totales=("Rutas_en_rango", "sum"),
            ).reset_index()
            resumen_cat["Pct_Gestion_prom"] = (resumen_cat["Pct_Gestion_prom"] * 100).round(1)
            resumen_cat["Efectividad_General_prom"] = (resumen_cat["Efectividad_General_prom"] * 100).round(1)

            orden = {c: i for i, c in enumerate(CATEGORIAS_ORDEN)}
            resumen_cat["_orden"] = resumen_cat["Categoria"].map(orden).fillna(99)
            resumen_cat = resumen_cat.sort_values("_orden").drop(columns="_orden")

            st.markdown("### 🏷️ % Gestión promedio por categoría (calculado en el rango elegido)")
            st.dataframe(resumen_cat, use_container_width=True, hide_index=True)
            st.bar_chart(resumen_cat.set_index("Categoria")["Pct_Gestion_prom"])

            st.caption(
                "Si una categoría 'alta' (Diamante/Platino) no muestra % Gestión claramente mayor "
                "que las categorías bajas, es señal de que el criterio de categorización debería revisarse."
            )
