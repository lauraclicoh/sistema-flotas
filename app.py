import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
import plotly.express as px

# ================= 1. CONFIGURACIÓN =================
st.set_page_config(layout="wide", page_title="CRM Aliados v3.0", page_icon="🚚")

ANALISTAS = {
    "Deisy Liliana Garcia":  "dgarcia@clicoh.com",
    "Erica Tatiana Garzon":  "etgarzon@clicoh.com",
    "Dayan Stefany Suarez":  "dsuarez@clicoh.com",
    "Carlos Andres Loaiza":  "cloaiza@clicoh.com",
}
NOMBRES_ANALISTAS = list(ANALISTAS.keys())

RESULTADOS = ["Apagado", "Fuera de servicio", "No contestó", "Número errado", "Sí contestó"]

ESTADOS_FINALES = [
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

NO_RESPONDEN      = ["Apagado", "Fuera de servicio", "No contestó", "Número errado"]
NO_VOLVER_ESTADOS = ["Aliado Rechaza la oferta", "Empleado", "Point"]
NO_VOLVER_RAZONES = ["No le interesa / cuestiones personales"]

COLS_CRM = ["intentos", "ultimo_resultado", "ultimo_estado", "ultima_razon",
            "fecha_gestion", "proxima_gestion"]

# ================= 2. GOOGLE SHEETS =================
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

@st.cache_resource
def conectar_sheets():
    try:
        creds_dict = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        client = gspread.authorize(creds)
        return client.open("GestionAliados")
    except Exception as e:
        st.error(f"Error conexión Sheets: {e}")
        return None

def leer_hoja(nombre_hoja, esperado_cols=None):
    """Lee una hoja de Google Sheets de forma robusta, tolerando columnas vacías/duplicadas."""
    try:
        sh = conectar_sheets()
        if sh is None:
            return pd.DataFrame(columns=esperado_cols or [])
        ws = sh.worksheet(nombre_hoja)

        # Leer todos los valores como lista de listas (más robusto que get_all_records)
        all_values = ws.get_all_values()

        if not all_values or len(all_values) < 1:
            return pd.DataFrame(columns=esperado_cols or [])

        # Primera fila = encabezados
        headers = all_values[0]

        # Limpiar encabezados: quitar vacíos y duplicados
        cleaned_headers = []
        seen = {}
        for h in headers:
            h = str(h).strip()
            if h == "" or h.lower() == "none":
                h = f"_col_{len(cleaned_headers)}"
            if h in seen:
                seen[h] += 1
                h = f"{h}_{seen[h]}"
            else:
                seen[h] = 0
            cleaned_headers.append(h)

        if len(all_values) < 2:
            return pd.DataFrame(columns=cleaned_headers)

        rows = all_values[1:]
        # Asegurar que cada fila tenga el mismo número de columnas
        n_cols = len(cleaned_headers)
        rows = [row + [""] * (n_cols - len(row)) if len(row) < n_cols else row[:n_cols]
                for row in rows]

        df = pd.DataFrame(rows, columns=cleaned_headers)

        # Quitar columnas auxiliares que empiezan con _col_
        df = df[[c for c in df.columns if not c.startswith("_col_")]]

        return df

    except Exception as e:
        st.warning(f"Aviso leyendo {nombre_hoja}: {e}")
        return pd.DataFrame(columns=esperado_cols or [])

def agregar_filas(nombre_hoja, rows: list):
    try:
        sh = conectar_sheets()
        if sh is None:
            return
        ws = sh.worksheet(nombre_hoja)
        ws.append_rows(rows, value_input_option="USER_ENTERED")
    except Exception as e:
        st.error(f"Error guardando en {nombre_hoja}: {e}")

def reemplazar_hoja(nombre_hoja, df: pd.DataFrame):
    try:
        sh = conectar_sheets()
        if sh is None:
            return
        ws = sh.worksheet(nombre_hoja)
        ws.clear()
        if not df.empty:
            ws.update([df.columns.tolist()] + df.astype(str).values.tolist())
    except Exception as e:
        st.error(f"Error reemplazando {nombre_hoja}: {e}")

# ================= 3. LÓGICA INCREMENTAL =================

def procesar_subida_incremental(df_nuevo):
    """Merge inteligente: nuevos se añaden, existentes actualizan datos operativos conservando CRM."""
    base_actual = leer_hoja("BASE")
    df_nuevo = df_nuevo.copy()
    df_nuevo.columns = df_nuevo.columns.str.strip().str.lower()

    col_id = next(
        (a for a in ["identificacion", "id_aliado", "id", "cedula", "documento"]
         if a in df_nuevo.columns), None
    )
    if not col_id:
        st.error("No se encontró columna de identificación en el archivo.")
        return 0, 0

    df_nuevo = df_nuevo.rename(columns={col_id: "identificacion"})
    df_nuevo["identificacion"] = df_nuevo["identificacion"].astype(str)

    if base_actual.empty:
        for col in COLS_CRM:
            df_nuevo[col] = 0 if col == "intentos" else ""
        reemplazar_hoja("BASE", df_nuevo)
        return len(df_nuevo), 0

    base_actual["identificacion"] = base_actual["identificacion"].astype(str)
    ids_viejos = set(base_actual["identificacion"].unique())

    nuevos = df_nuevo[~df_nuevo["identificacion"].isin(ids_viejos)].copy()
    for col in COLS_CRM:
        nuevos[col] = 0 if col == "intentos" else ""

    existentes_datos = df_nuevo[df_nuevo["identificacion"].isin(ids_viejos)].set_index("identificacion")
    base_idx = base_actual.set_index("identificacion")
    base_idx.update(existentes_datos)
    base_actual = base_idx.reset_index()

    base_final = pd.concat([base_actual, nuevos], ignore_index=True)
    reemplazar_hoja("BASE", base_final)
    return len(nuevos), len(existentes_datos)

# ================= 4. LÓGICA CRM =================

def calcular_proxima_gestion(resultado, estado, razon, intentos):
    hoy = datetime.now()
    estado = str(estado or "")
    razon  = str(razon or "")

    if estado in NO_VOLVER_ESTADOS or razon in NO_VOLVER_RAZONES:
        return "NO_VOLVER"

    if resultado in ["No contestó", "Apagado", "Fuera de servicio", "Número errado"]:
        if intentos >= 10:
            return hoy + timedelta(days=30)
        if resultado == "No contestó":
            return hoy + timedelta(days=1)
        return hoy + timedelta(days=2)

    if estado in ["Interesado llega a cargue", "Aliado Fleet/Delivery no acepta hub"]:
        return hoy + timedelta(days=5)

    return hoy + timedelta(days=3)

def prioridad_label(dias):
    try:
        dias = int(float(str(dias)))
    except Exception:
        return "🟢 BAJA"
    if dias > 5:
        return "🔴 ALTA"
    elif dias > 1:
        return "🟡 MEDIA"
    return "🟢 BAJA"

def normalizar_vehiculo(v):
    v = str(v).lower()
    if any(k in v for k in ["carry", "largenvan", "large van", "small van", "van"]):
        return "Carry / Van"
    elif "moto" in v:
        return "Moto"
    elif any(k in v for k in ["camion", "camión", "truck", "npr"]):
        return "Camión"
    return str(v).title()

# ================= 5. CARGA DE DATOS =================

@st.cache_data(ttl=300)
def cargar_base():
    df = leer_hoja("BASE")
    if df.empty:
        return None

    df.columns = df.columns.str.strip().str.lower()

    # Alias identificacion
    if "identificacion" not in df.columns:
        for alias in ["id_aliado", "id", "cedula", "documento"]:
            if alias in df.columns:
                df["identificacion"] = df[alias]
                break

    if "identificacion" not in df.columns:
        return None  # Sin ID no podemos operar

    # Alias celular
    if "celular" not in df.columns:
        for alias in ["telefono", "tel", "phone"]:
            if alias in df.columns:
                df["celular"] = df[alias]
                break

    # Alias zona
    if "zona" not in df.columns and "municipio" in df.columns:
        df["zona"] = df["municipio"]

    if "zona" not in df.columns:
        df["zona"] = "Sin zona"

    # Vehículo normalizado
    if "vehiculo" in df.columns:
        df["vehiculo_norm"] = df["vehiculo"].apply(normalizar_vehiculo)
    else:
        df["vehiculo_norm"] = "Sin vehículo"

    # Días sin cargar
    df["dias"] = 0
    col_fecha = next(
        (c for c in ["fecha_ultimo_cargue", "fecha ultimo cargue", "fechaultimocargue"]
         if c in df.columns), None
    )
    if col_fecha:
        df["_fecha_cargue"] = pd.to_datetime(df[col_fecha].astype(str), errors="coerce")
        df["dias"] = (datetime.now() - df["_fecha_cargue"]).dt.days.fillna(0).astype(int)
    elif "dias_desde_ult_srv." in df.columns:
        df["dias"] = pd.to_numeric(df["dias_desde_ult_srv."], errors="coerce").fillna(0).astype(int)

    # Columnas CRM
    for col in COLS_CRM:
        if col not in df.columns:
            df[col] = 0 if col == "intentos" else ""

    df["intentos"] = pd.to_numeric(df["intentos"], errors="coerce").fillna(0).astype(int)

    return df

@st.cache_data(ttl=60)
def cargar_hist():
    cols_esperadas = ["fecha", "analista", "identificacion", "resultado", "estado", "razon", "obs"]
    df = leer_hoja("HISTORICO", cols_esperadas)

    if df.empty:
        return pd.DataFrame(columns=cols_esperadas)

    # Asegurar que existen las columnas necesarias
    for col in cols_esperadas:
        if col not in df.columns:
            df[col] = ""

    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df = df.dropna(subset=["fecha"])   # eliminar filas sin fecha válida
    return df

@st.cache_data(ttl=60)
def cargar_reparto():
    return leer_hoja("REPARTO", ["fecha", "analista", "identificacion"])

def leer_config(nombre_analista):
    df = leer_hoja("CONFIG", ["analista", "modo", "zona", "vehiculo"])
    if df.empty or "analista" not in df.columns:
        return "Analista decide", None, None
    fila = df[df["analista"] == nombre_analista]
    if not fila.empty:
        r = fila.iloc[-1]
        return r.get("modo", "Analista decide"), r.get("zona"), r.get("vehiculo")
    fila_todos = df[df["analista"] == "TODOS"]
    if not fila_todos.empty:
        r = fila_todos.iloc[-1]
        return r.get("modo", "Analista decide"), r.get("zona"), r.get("vehiculo")
    return "Analista decide", None, None

# ================= 6. GUARDADO =================

def guardar_gestion(row_dict):
    fila = [
        str(row_dict.get("fecha", "")),
        str(row_dict.get("analista", "")),
        str(row_dict.get("identificacion", "")),
        str(row_dict.get("resultado", "")),
        str(row_dict.get("estado", "")),
        str(row_dict.get("razon", "")),
        str(row_dict.get("obs", "")),
    ]
    agregar_filas("HISTORICO", [fila])
    cargar_hist.clear()

def actualizar_base(identificacion, resultado, estado, razon):
    df = leer_hoja("BASE")
    if df.empty or "identificacion" not in df.columns:
        return

    df["identificacion"] = df["identificacion"].astype(str)

    for col in COLS_CRM:
        if col not in df.columns:
            df[col] = 0 if col == "intentos" else ""

    df["intentos"] = pd.to_numeric(df["intentos"], errors="coerce").fillna(0).astype(int)

    idx = df[df["identificacion"] == str(identificacion)].index
    if idx.empty:
        return

    intentos_nuevo = int(df.loc[idx[0], "intentos"]) + 1
    proxima = calcular_proxima_gestion(resultado, estado, razon, intentos_nuevo)

    df.loc[idx, "ultimo_resultado"] = str(resultado or "")
    df.loc[idx, "ultimo_estado"]    = str(estado or "")
    df.loc[idx, "ultima_razon"]     = str(razon or "")
    df.loc[idx, "fecha_gestion"]    = str(datetime.now())
    df.loc[idx, "intentos"]         = intentos_nuevo
    df.loc[idx, "proxima_gestion"]  = str(proxima)

    reemplazar_hoja("BASE", df)
    cargar_base.clear()

def filtrar_pool(df_pool):
    if "proxima_gestion" not in df_pool.columns:
        return df_pool

    df_pool = df_pool.copy()
    mask_no_volver = df_pool["proxima_gestion"].astype(str).str.upper() == "NO_VOLVER"
    df_pool = df_pool[~mask_no_volver]

    def es_disponible(val):
        val = str(val).strip()
        if val in ("", "nan", "None", "0"):
            return True
        try:
            fecha = pd.to_datetime(val, errors="coerce")
            if pd.isna(fecha):
                return True
            return fecha <= datetime.now()
        except Exception:
            return True

    return df_pool[df_pool["proxima_gestion"].apply(es_disponible)]

def guardar_reparto(df_reparto):
    reemplazar_hoja("REPARTO", df_reparto)
    cargar_reparto.clear()

# ================================================================
#  UI PRINCIPAL
# ================================================================

st.title("🚚 Gestión de Aliados")
perfil = st.sidebar.selectbox("Perfil", ["Coordinador", "Analista"])

# ================================================================
#  COORDINADOR
# ================================================================

if perfil == "Coordinador":
    password = st.sidebar.text_input("Contraseña", type="password")
    if password != "clicoh":
        if password:
            st.sidebar.error("Contraseña incorrecta")
        st.stop()

    base = cargar_base()
    hist = cargar_hist()

    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 Hoy",
        "📅 Histórico & KPIs",
        "🔥 Estado CRM Base",
        "📤 Cargar Base",
        "🎯 Asignación Analistas",
        "⚙️ Reglas Recontacto",
    ])

    # ─── HOY ───
    with tab1:
        st.subheader("Resumen operativo de hoy")
        if hist.empty:
            st.info("Aún no hay gestión registrada hoy.")
        else:
            hist_valido2 = hist.dropna(subset=["fecha"])
            hoy = hist_valido2[hist_valido2["fecha"].dt.date == datetime.now().date()]
            total   = len(hoy)
            gest    = len(hoy[hoy["resultado"] == "Sí contestó"])
            inter   = len(hoy[hoy["estado"]    == "Interesado llega a cargue"])
            rech    = len(hoy[hoy["estado"]    == "Aliado Rechaza la oferta"])
            no_resp = len(hoy[hoy["resultado"].isin(NO_RESPONDEN)])

            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("📞 Llamadas",     total)
            c2.metric("✅ Contactados",  gest)
            c3.metric("🚗 Interesados",  inter)
            c4.metric("❌ Rechazados",   rech)
            c5.metric("📵 No responden", no_resp)

            if total > 0:
                st.markdown("---")
                prod    = hoy.groupby("analista").size().reset_index(name="llamadas")
                inter_a = (hoy[hoy["estado"] == "Interesado llega a cargue"]
                           .groupby("analista").size().reset_index(name="interesados"))
                tabla   = prod.merge(inter_a, on="analista", how="left").fillna(0)
                tabla["interesados"]   = tabla["interesados"].astype(int)
                tabla["% efectividad"] = (tabla["interesados"] / tabla["llamadas"] * 100).round(1)

                def semaforo(row):
                    if row["llamadas"] >= 30 and row["interesados"] >= 3: return "🟢"
                    elif row["llamadas"] >= 15: return "🟡"
                    return "🔴"

                tabla["estado"] = tabla.apply(semaforo, axis=1)
                st.subheader("Productividad por analista")
                st.dataframe(tabla, use_container_width=True)
                fig = px.bar(tabla, x="analista", y="llamadas",
                             color="% efectividad", title="Llamadas por Analista")
                st.plotly_chart(fig, use_container_width=True)

    # ─── HISTÓRICO & KPIs ───
    with tab2:
        st.subheader("Histórico con KPIs")
        if hist.empty:
            st.info("No hay histórico aún.")
        else:
            col_f1, col_f2 = st.columns(2)
            with col_f1:
                f1 = st.date_input("Desde", datetime.now().date() - timedelta(days=7))
            with col_f2:
                f2 = st.date_input("Hasta", datetime.now().date())

            # Filtro seguro de fechas
            hist_valido = hist.dropna(subset=["fecha"])
            d = hist_valido[
                (hist_valido["fecha"].dt.date >= f1) &
                (hist_valido["fecha"].dt.date <= f2)
            ]
            total   = len(d)
            si_resp = d[d["resultado"] == "Sí contestó"]
            no_resp = d[d["resultado"].isin(NO_RESPONDEN)]
            gest    = len(si_resp)
            inter   = len(d[d["estado"] == "Interesado llega a cargue"])
            rech    = len(d[d["estado"] == "Aliado Rechaza la oferta"])

            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("📞 Total",        total)
            c2.metric("✅ Contactados",  gest)
            c3.metric("% No responden",  f"{round(len(no_resp)/total*100,1) if total else 0}%")
            c4.metric("% Gestión",       f"{round(gest/total*100,1) if total else 0}%")
            c5.metric("% Interesados",   f"{round(inter/total*100,1) if total else 0}%")

            c6, c7 = st.columns(2)
            c6.metric("% Rechazados",            f"{round(rech/total*100,1) if total else 0}%")
            c7.metric("% Rechazo (contactados)", f"{round(rech/gest*100,1) if gest else 0}%")

            st.markdown("---")
            st.markdown("#### 📉 Embudo de conversión")
            embudo = pd.DataFrame({
                "Etapa":    ["Llamados", "Contactados", "Interesados"],
                "Cantidad": [total, gest, inter],
                "%":        [100,
                             round(gest/total*100, 1) if total else 0,
                             round(inter/total*100, 1) if total else 0],
            })
            st.dataframe(embudo, use_container_width=True)
            fig_emb = px.funnel(embudo, x="Cantidad", y="Etapa", title="Embudo de Conversión")
            st.plotly_chart(fig_emb, use_container_width=True)

            st.markdown("---")
            st.markdown("#### Estado final (sobre contactados)")
            data_estado = [
                [e, len(si_resp[si_resp["estado"] == e]),
                 round(len(si_resp[si_resp["estado"] == e]) / gest * 100, 1) if gest else 0]
                for e in ESTADOS_FINALES
            ]
            st.dataframe(pd.DataFrame(data_estado, columns=["Estado", "Cantidad", "%"]),
                         use_container_width=True)

            st.markdown("---")
            st.markdown("#### Razones (sobre contactados)")
            data_razon = [
                [r, len(si_resp[si_resp["razon"] == r]),
                 round(len(si_resp[si_resp["razon"] == r]) / gest * 100, 1) if gest else 0]
                for r in RAZONES
            ]
            st.dataframe(pd.DataFrame(data_razon, columns=["Razón", "Cantidad", "%"]),
                         use_container_width=True)

            st.markdown("---")
            st.markdown("#### KPIs por analista")
            prod    = d.groupby("analista").size().reset_index(name="llamadas")
            gest_a  = d[d["resultado"] == "Sí contestó"].groupby("analista").size().reset_index(name="gestionadas")
            int_a   = d[d["estado"] == "Interesado llega a cargue"].groupby("analista").size().reset_index(name="interesados")
            rec_a   = d[d["estado"] == "Aliado Rechaza la oferta"].groupby("analista").size().reset_index(name="rechazados")
            nor_a   = d[d["resultado"].isin(NO_RESPONDEN)].groupby("analista").size().reset_index(name="no_resp")

            tabla_a = (prod.merge(gest_a, on="analista", how="left")
                           .merge(int_a,  on="analista", how="left")
                           .merge(rec_a,  on="analista", how="left")
                           .merge(nor_a,  on="analista", how="left")
                           .fillna(0))
            for col in ["gestionadas", "interesados", "rechazados", "no_resp"]:
                tabla_a[col] = tabla_a[col].astype(int)
            tabla_a["% gestión"]      = (tabla_a["gestionadas"] / tabla_a["llamadas"] * 100).round(1)
            tabla_a["% interesados"]  = (tabla_a["interesados"] / tabla_a["llamadas"] * 100).round(1)
            tabla_a["% rechazados"]   = (tabla_a["rechazados"]  / tabla_a["llamadas"] * 100).round(1)
            tabla_a["% no responden"] = (tabla_a["no_resp"]     / tabla_a["llamadas"] * 100).round(1)
            st.dataframe(tabla_a, use_container_width=True)
            fig_an = px.bar(tabla_a, x="analista",
                            y=["% gestión", "% interesados"],
                            barmode="group", title="KPIs por Analista")
            st.plotly_chart(fig_an, use_container_width=True)

            st.markdown("---")
            st.dataframe(d.sort_values("fecha", ascending=False), use_container_width=True)

    # ─── ESTADO CRM BASE ───
    with tab3:
        st.subheader("Estado actual de la base (vista CRM)")
        if base is None:
            st.warning("Carga la base primero en 📤 Cargar Base.")
        else:
            no_volver   = base[base["proxima_gestion"].astype(str).str.upper() == "NO_VOLVER"]
            disponibles = filtrar_pool(base)
            en_pausa    = base[
                base["proxima_gestion"].astype(str).apply(
                    lambda v: v not in ("", "nan", "None", "NO_VOLVER", "0") and
                              pd.to_datetime(v, errors="coerce") is not pd.NaT and
                              pd.to_datetime(v, errors="coerce") > datetime.now()
                )
            ]

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("📦 Total base",          len(base))
            c2.metric("✅ Disponibles ahora",   len(disponibles))
            c3.metric("⏸ En pausa/recontacto", len(en_pausa))
            c4.metric("🚫 No vuelven nunca",    len(no_volver))

            st.markdown("---")
            disponibles2 = disponibles.copy()
            disponibles2["PRIORIDAD"] = disponibles2["dias"].apply(prioridad_label)
            alta  = disponibles2[disponibles2["PRIORIDAD"] == "🔴 ALTA"]
            media = disponibles2[disponibles2["PRIORIDAD"] == "🟡 MEDIA"]
            baja  = disponibles2[disponibles2["PRIORIDAD"] == "🟢 BAJA"]

            c1, c2, c3 = st.columns(3)
            c1.metric("🔴 ALTA (>5 días)",   len(alta))
            c2.metric("🟡 MEDIA (2-5 días)", len(media))
            c3.metric("🟢 BAJA (0-1 días)",  len(baja))

            st.markdown("---")
            st.markdown("#### Aliados en pausa/recontacto programado")
            if not en_pausa.empty:
                cols_p = [c for c in ["identificacion", "mensajero", "celular", "zona",
                                      "vehiculo", "intentos", "ultimo_resultado",
                                      "ultimo_estado", "proxima_gestion"] if c in en_pausa.columns]
                st.dataframe(en_pausa[cols_p].sort_values("proxima_gestion"),
                             use_container_width=True)
            else:
                st.info("No hay aliados en pausa actualmente.")

            st.markdown("---")
            st.markdown("#### Aliados bloqueados permanentemente")
            if not no_volver.empty:
                cols_nv = [c for c in ["identificacion", "mensajero", "celular",
                                       "ultimo_estado", "ultima_razon"] if c in no_volver.columns]
                st.dataframe(no_volver[cols_nv], use_container_width=True)
            else:
                st.info("No hay aliados bloqueados.")

    # ─── CARGAR BASE ───
    with tab4:
        st.subheader("📤 Carga de Base")
        modo_carga = st.radio(
            "Modo de carga",
            ["🔄 Incremental (recomendado)", "♻️ Reemplazar toda la base"],
            help=(
                "**Incremental**: IDs nuevos se añaden, IDs existentes actualizan datos "
                "operativos conservando el historial CRM.\n\n"
                "**Reemplazar**: Borra toda la base y sube el archivo nuevo (usar solo la primera vez)."
            )
        )

        archivo = st.file_uploader("Selecciona archivo Excel (.xlsx)", type=["xlsx"])
        if archivo:
            try:
                df_subido = pd.read_excel(archivo, engine="openpyxl")
                st.success(f"Archivo leído: {len(df_subido):,} registros")
                st.dataframe(df_subido.head(5), use_container_width=True)

                if modo_carga == "🔄 Incremental (recomendado)":
                    st.info("IDs nuevos → se añaden. IDs existentes → actualizan datos conservando historial CRM.")
                    if st.button("🚀 Ejecutar Cruce Incremental"):
                        with st.spinner("Procesando merge..."):
                            n_nuevos, n_actualizados = procesar_subida_incremental(df_subido)
                        st.success(f"✅ {n_nuevos} nuevos añadidos · {n_actualizados} actualizados.")
                        cargar_base.clear()
                else:
                    st.warning("⚠️ Esto borrará toda la base actual.")
                    if st.button("♻️ Reemplazar base completa"):
                        with st.spinner("Subiendo base..."):
                            reemplazar_hoja("BASE", df_subido)
                            cargar_base.clear()
                        st.success(f"✅ Base reemplazada — {len(df_subido):,} aliados.")
            except Exception as e:
                st.error(f"Error leyendo el archivo: {e}")

        if base is not None:
            st.info(f"Base activa: **{len(base):,} aliados** en Google Sheets.")

    # ─── ASIGNACIÓN ───
    with tab5:
        st.subheader("Configurar modo de trabajo")
        if base is None:
            st.warning("Carga la base primero.")
        else:
            zonas = sorted(base["zona"].dropna().unique())
            vhs   = sorted(base["vehiculo_norm"].dropna().unique())

            modo = st.selectbox("Modo de asignación", [
                "Analista decide",
                "Asignación general (todos igual)",
                "Asignación por analista",
            ])

            data_conf = []
            if modo == "Asignación general (todos igual)":
                zona_g = st.selectbox("Zona para todos", zonas)
                vh_g   = st.selectbox("Vehículo para todos", vhs)
                data_conf = [{"analista": "TODOS", "modo": modo, "zona": zona_g, "vehiculo": vh_g}]

            elif modo == "Asignación por analista":
                for a in NOMBRES_ANALISTAS:
                    st.markdown(f"**{a}**")
                    col1, col2 = st.columns(2)
                    with col1:
                        z = st.selectbox("Zona", zonas, key=f"zona_{a}")
                    with col2:
                        v = st.selectbox("Vehículo", vhs, key=f"vh_{a}")
                    data_conf.append({"analista": a, "modo": modo, "zona": z, "vehiculo": v})
            else:
                data_conf = [{"analista": "TODOS", "modo": "Analista decide", "zona": "", "vehiculo": ""}]

            if st.button("💾 Guardar asignación"):
                reemplazar_hoja("CONFIG", pd.DataFrame(data_conf))
                st.success("Asignación guardada.")

            df_conf = leer_hoja("CONFIG")
            if not df_conf.empty:
                st.markdown("---")
                st.markdown("##### Configuración activa:")
                st.dataframe(df_conf, use_container_width=True)

    # ─── REGLAS ───
    with tab6:
        st.subheader("⚙️ Reglas de recontacto automático")
        st.markdown("""
| Resultado / Estado | Acción | Días espera |
|---|---|---|
| No contestó | Recontacto | 1 día |
| Apagado / Fuera de servicio | Recontacto | 2 días |
| 10+ intentos sin contacto | Pausa larga | 30 días |
| Interesado llega a cargue | Pausa | 5 días |
| Fleet no acepta HUB | Pausa | 5 días |
| Interesado esporádico | Recontacto | 3 días |
| Aliado Rechaza la oferta | ❌ Bloqueo permanente | Nunca |
| Empleado / Point | ❌ Bloqueo permanente | Nunca |
| No le interesa (razón) | ❌ Bloqueo permanente | Nunca |
        """)
        st.info("Estas reglas se aplican automáticamente al guardar cada gestión. No requieren configuración manual.")


# ================================================================
#  ANALISTA
# ================================================================

if perfil == "Analista":

    base = cargar_base()
    hist = cargar_hist()

    if base is None:
        st.warning("⚠️ La coordinadora aún no ha cargado la base. Espera un momento.")
        st.stop()

    st.markdown("---")
    nombre = st.selectbox("¿Quién eres?", NOMBRES_ANALISTAS)

    modo_conf, zona_conf, vh_conf = leer_config(nombre)

    if modo_conf in ("Asignación general (todos igual)", "Asignación por analista") \
            and zona_conf and vh_conf:
        zona_sel = str(zona_conf)
        vh_sel   = str(vh_conf)
        st.success(f"🎯 Hoy debes gestionar: **{zona_sel}** — **{vh_sel}**")
    else:
        zonas    = sorted(base["zona"].dropna().unique())
        vhs      = sorted(base["vehiculo_norm"].dropna().unique())
        zona_sel = st.selectbox("Zona", zonas)
        vh_sel   = st.selectbox("Vehículo", vhs)

    # Pool con CRM aplicado
    mask = (base["zona"].astype(str) == str(zona_sel)) & \
           (base["vehiculo_norm"].astype(str) == str(vh_sel))
    pool = base[mask].copy()
    pool = filtrar_pool(pool)

    if pool.empty:
        st.info("No hay aliados disponibles para esta zona/vehículo. Prueba otro filtro.")
        st.stop()

    pool["PRIORIDAD"] = pool["dias"].apply(prioridad_label)
    orden_prio = {"🔴 ALTA": 0, "🟡 MEDIA": 1, "🟢 BAJA": 2}
    pool["_orden"] = pool["PRIORIDAD"].map(orden_prio).fillna(3)
    pool = pool.sort_values("_orden").drop(columns=["_orden"]).reset_index(drop=True)

    # Quitar ya gestionados hoy
    if not hist.empty:
        hist_v = hist.dropna(subset=["fecha"])
        gestionados_hoy = (hist_v[hist_v["fecha"].dt.date == datetime.now().date()]
                           ["identificacion"].astype(str).tolist())
        pool = pool[~pool["identificacion"].astype(str).isin(gestionados_hoy)]

    col_cant, col_prio = st.columns(2)
    with col_cant:
        cant = st.number_input("Cantidad de aliados", min_value=10, max_value=300, value=30)
    with col_prio:
        filtro_prio = st.selectbox("Prioridad", [
            "Todas (ALTA + MEDIA + BAJA)",
            "Solo 🔴 ALTA",
            "Solo 🟡 MEDIA",
            "Solo 🟢 BAJA",
        ])

    if filtro_prio == "Solo 🔴 ALTA":
        pool = pool[pool["PRIORIDAD"] == "🔴 ALTA"]
    elif filtro_prio == "Solo 🟡 MEDIA":
        pool = pool[pool["PRIORIDAD"] == "🟡 MEDIA"]
    elif filtro_prio == "Solo 🟢 BAJA":
        pool = pool[pool["PRIORIDAD"] == "🟢 BAJA"]

    st.caption(f"Aliados disponibles en este filtro: **{len(pool)}**")

    if st.button("🚀 Generar mis llamadas"):
        hoy_str    = datetime.now().date().isoformat()
        reparto_df = cargar_reparto()

        if not reparto_df.empty and "fecha" in reparto_df.columns:
            if len(reparto_df) > 0 and str(reparto_df["fecha"].iloc[0]) != hoy_str:
                reparto_df = pd.DataFrame(columns=["fecha", "analista", "identificacion"])
        else:
            reparto_df = pd.DataFrame(columns=["fecha", "analista", "identificacion"])

        ya_asignados    = reparto_df[reparto_df["fecha"] == hoy_str]["identificacion"].astype(str).tolist()
        pool_disponible = pool[~pool["identificacion"].astype(str).isin(ya_asignados)]
        mi_bloque       = pool_disponible.head(int(cant)).reset_index(drop=True)

        if mi_bloque.empty:
            st.warning("⚠️ No hay más aliados disponibles. Prueba otra zona, prioridad o espera recontactos.")
        else:
            nuevos = pd.DataFrame({
                "fecha":          [hoy_str] * len(mi_bloque),
                "analista":       [nombre]  * len(mi_bloque),
                "identificacion": mi_bloque["identificacion"].astype(str).tolist(),
            })
            reparto_nuevo = pd.concat([reparto_df, nuevos], ignore_index=True)
            guardar_reparto(reparto_nuevo)
            st.session_state["pool_activo"] = mi_bloque
            st.session_state["hechas"]      = st.session_state.get("hechas", 0)
            st.success(f"✅ Se te asignaron {len(mi_bloque)} aliados únicos.")

    # Pool activo
    if "pool_activo" in st.session_state and not st.session_state["pool_activo"].empty:

        pool_activo = st.session_state["pool_activo"]
        hechas      = st.session_state.get("hechas", 0)
        restantes   = len(pool_activo)
        pct         = int(hechas / (hechas + restantes) * 100) if (hechas + restantes) > 0 else 0
        st.progress(pct, text=f"Progreso: {hechas} gestionados / {restantes} pendientes")

        cols_mostrar = [c for c in ["identificacion", "mensajero", "celular",
                                    "zona", "vehiculo", "dias", "intentos", "PRIORIDAD"]
                        if c in pool_activo.columns]
        st.markdown("#### 📋 Aliados pendientes")
        st.dataframe(pool_activo[cols_mostrar], use_container_width=True)

        st.markdown("---")
        st.markdown("#### 📞 Registrar gestión")

        with st.form(key="form_gestion", clear_on_submit=True):
            aliado_sel = st.selectbox("Cédula del aliado",
                                      pool_activo["identificacion"].astype(str).tolist())

            fila = pool_activo[pool_activo["identificacion"].astype(str) == aliado_sel]
            if not fila.empty:
                f         = fila.iloc[0]
                info_cols = [c for c in ["mensajero", "celular", "intentos", "PRIORIDAD"]
                             if c in f.index]
                cols_info = st.columns(max(len(info_cols), 1))
                for i, col_name in enumerate(info_cols):
                    cols_info[i].metric(col_name.capitalize(), str(f[col_name]))

            resultado = st.selectbox("Resultado de la llamada", RESULTADOS)
            estado    = st.selectbox("Estado final (solo si contestó)", ["-"] + ESTADOS_FINALES)
            razon     = st.selectbox("Razón (solo si contestó)",        ["-"] + RAZONES)
            obs       = st.text_area("Observación (opcional)")
            submitted = st.form_submit_button("💾 Guardar y siguiente")

        if submitted:
            estado_real = None if estado == "-" else estado
            razon_real  = None if razon  == "-" else razon

            if resultado == "Sí contestó" and estado_real is None:
                st.error("⚠️ Si el aliado contestó, selecciona un Estado final.")
            else:
                guardar_gestion({
                    "fecha":          datetime.now(),
                    "analista":       nombre,
                    "identificacion": aliado_sel,
                    "resultado":      resultado,
                    "estado":         estado_real,
                    "razon":          razon_real,
                    "obs":            obs,
                })

                with st.spinner("Actualizando estado CRM del aliado..."):
                    actualizar_base(aliado_sel, resultado, estado_real, razon_real)

                nuevo_pool = pool_activo[pool_activo["identificacion"].astype(str) != aliado_sel]
                st.session_state["pool_activo"] = nuevo_pool.reset_index(drop=True)
                st.session_state["hechas"]      = hechas + 1

                if nuevo_pool.empty:
                    st.success("✅ ¡Completaste todas tus llamadas! Puedes generar más.")
                    del st.session_state["pool_activo"]
                    st.session_state["hechas"] = 0

                st.rerun()

    # Resumen del analista hoy
    if not hist.empty:
        hist_v2 = hist.dropna(subset=["fecha"])
        mis = hist_v2[(hist_v2["analista"] == nombre) &
                      (hist_v2["fecha"].dt.date == datetime.now().date())]
        if not mis.empty:
            st.markdown("---")
            st.markdown(f"#### 📈 Tus gestiones de hoy ({len(mis)} registros)")
            st.dataframe(mis[["fecha", "identificacion", "resultado", "estado", "razon"]],
                         use_container_width=True)
