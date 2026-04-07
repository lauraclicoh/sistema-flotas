import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
import json

st.set_page_config(layout="wide", page_title="Gestión Programación Aliados")

# ================= ANALISTAS =================
ANALISTAS = {
    "Deisy Liliana Garcia":  "dgarcia@clicoh.com",
    "Erica Tatiana Garzon":  "etgarzon@clicoh.com",
    "Dayan Stefany Suarez":  "dsuarez@clicoh.com",
    "Carlos Andres Loaiza":  "cloaiza@clicoh.com",
}
NOMBRES_ANALISTAS = list(ANALISTAS.keys())

RESULTADOS = ["Apagado","Fuera de servicio","No contestó","Número errado","Sí contestó"]

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

NO_RESPONDEN = ["Apagado","Fuera de servicio","No contestó","Número errado"]

# ================= CONEXIÓN GOOGLE SHEETS =================

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

@st.cache_resource
def conectar_sheets():
    """Retorna el objeto spreadsheet usando credenciales de st.secrets."""
    creds_dict = dict(st.secrets["gcp_service_account"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open("GestionAliados")   # nombre del Google Sheet

def leer_hoja(nombre_hoja, esperado_cols=None):
    """Lee una hoja y devuelve DataFrame. Si está vacía devuelve DataFrame con columnas esperadas."""
    try:
        sh = conectar_sheets()
        ws = sh.worksheet(nombre_hoja)
        data = ws.get_all_records()
        if data:
            return pd.DataFrame(data)
        else:
            return pd.DataFrame(columns=esperado_cols or [])
    except Exception as e:
        st.error(f"Error leyendo hoja {nombre_hoja}: {e}")
        return pd.DataFrame(columns=esperado_cols or [])

def agregar_filas(nombre_hoja, rows: list):
    """Agrega una lista de listas al final de la hoja."""
    try:
        sh = conectar_sheets()
        ws = sh.worksheet(nombre_hoja)
        ws.append_rows(rows, value_input_option="USER_ENTERED")
    except Exception as e:
        st.error(f"Error guardando en {nombre_hoja}: {e}")

def reemplazar_hoja(nombre_hoja, df: pd.DataFrame):
    """Reemplaza toda la hoja con el DataFrame dado."""
    try:
        sh = conectar_sheets()
        ws = sh.worksheet(nombre_hoja)
        ws.clear()
        if not df.empty:
            ws.update([df.columns.tolist()] + df.astype(str).values.tolist())
    except Exception as e:
        st.error(f"Error reemplazando hoja {nombre_hoja}: {e}")

# ================= FUNCIONES DE DATOS =================

def prioridad_label(dias):
    try:
        dias = int(dias)
    except:
        return "🟢 BAJA"
    if dias > 5:
        return "🔴 ALTA"
    elif dias > 1:
        return "🟡 MEDIA"
    else:
        return "🟢 BAJA"

def normalizar_vehiculo(v):
    v = str(v).lower()
    if any(k in v for k in ["carry","largenvan","large van","small van","van"]):
        return "Carry / Van"
    elif "moto" in v:
        return "Moto"
    elif any(k in v for k in ["camion","camión","truck","npr"]):
        return "Camión"
    return str(v).title()

@st.cache_data(ttl=300)
def cargar_base():
    """Lee la base desde Google Sheets (hoja BASE). Cache 5 min."""
    df = leer_hoja("BASE")
    if df.empty:
        return None
    df.columns = df.columns.str.strip().str.lower()

    if "identificacion" not in df.columns:
        for alias in ["id_aliado","id","cedula","documento"]:
            if alias in df.columns:
                df["identificacion"] = df[alias]
                break

    if "celular" not in df.columns:
        for alias in ["telefono","tel","phone"]:
            if alias in df.columns:
                df["celular"] = df[alias]
                break

    if "zona" not in df.columns and "municipio" in df.columns:
        df["zona"] = df["municipio"]

    if "vehiculo" in df.columns:
        df["vehiculo_norm"] = df["vehiculo"].apply(normalizar_vehiculo)
    else:
        df["vehiculo_norm"] = "Sin vehículo"

    col_fecha = None
    for posible in ["fecha_ultimo_cargue","fecha ultimo cargue","fechaultimocargue"]:
        if posible in df.columns:
            col_fecha = posible
            break

    if col_fecha:
        df["_fecha_cargue"] = pd.to_datetime(df[col_fecha].astype(str), errors="coerce")
        df["dias"] = (datetime.now() - df["_fecha_cargue"]).dt.days.fillna(0).astype(int)
    elif "dias_desde_ult_srv." in df.columns:
        df["dias"] = pd.to_numeric(df["dias_desde_ult_srv."], errors="coerce").fillna(0).astype(int)
    else:
        df["dias"] = 0

    return df

@st.cache_data(ttl=60)
def cargar_hist():
    """Lee el historial desde Google Sheets (hoja HISTORICO). Cache 1 min."""
    df = leer_hoja("HISTORICO", ["fecha","analista","identificacion","resultado","estado","razon","obs"])
    if not df.empty and "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    return df

@st.cache_data(ttl=60)
def cargar_rechazados():
    df = leer_hoja("RECHAZADOS", ["identificacion"])
    if df.empty or "identificacion" not in df.columns:
        return []
    return df["identificacion"].astype(str).tolist()

@st.cache_data(ttl=60)
def cargar_pausas():
    df = leer_hoja("PAUSAS", ["identificacion","fecha_pausa"])
    if df.empty or "identificacion" not in df.columns:
        return []
    df["fecha_pausa"] = pd.to_datetime(df["fecha_pausa"], errors="coerce")
    activas = df[(datetime.now() - df["fecha_pausa"]).dt.days <= 5]
    return activas["identificacion"].astype(str).tolist()

@st.cache_data(ttl=60)
def cargar_reparto():
    df = leer_hoja("REPARTO", ["fecha","analista","identificacion"])
    return df

def guardar_gestion(row_dict):
    fila = [
        str(row_dict.get("fecha","")),
        str(row_dict.get("analista","")),
        str(row_dict.get("identificacion","")),
        str(row_dict.get("resultado","")),
        str(row_dict.get("estado","")),
        str(row_dict.get("razon","")),
        str(row_dict.get("obs","")),
    ]
    agregar_filas("HISTORICO", [fila])
    # Limpiar cache de historial
    cargar_hist.clear()

def guardar_rechazado(ident):
    agregar_filas("RECHAZADOS", [[str(ident)]])
    cargar_rechazados.clear()

def guardar_pausa(ident):
    agregar_filas("PAUSAS", [[str(ident), str(datetime.now())]])
    cargar_pausas.clear()

def guardar_reparto(df_reparto):
    reemplazar_hoja("REPARTO", df_reparto)
    cargar_reparto.clear()

def filtrar_pool(df_pool):
    rechazados = cargar_rechazados()
    pausados   = cargar_pausas()
    bloqueados = set(rechazados) | set(pausados)
    return df_pool[~df_pool["identificacion"].astype(str).isin(bloqueados)]

def leer_config(nombre_analista):
    df = leer_hoja("CONFIG", ["analista","modo","zona","vehiculo"])
    if df.empty or "analista" not in df.columns:
        return "Analista decide", None, None
    fila = df[df["analista"] == nombre_analista]
    if not fila.empty:
        r = fila.iloc[-1]
        return r.get("modo","Analista decide"), r.get("zona"), r.get("vehiculo")
    fila_todos = df[df["analista"] == "TODOS"]
    if not fila_todos.empty:
        r = fila_todos.iloc[-1]
        return r.get("modo","Analista decide"), r.get("zona"), r.get("vehiculo")
    return "Analista decide", None, None

# ================================================================
#  UI PRINCIPAL
# ================================================================

st.title("🚚 Gestión Programación de Aliados")

perfil = st.selectbox("Perfil", ["Coordinador","Analista"])

# ================================================================
#  COORDINADOR
# ================================================================

if perfil == "Coordinador":

    clave = st.text_input("Clave coordinador", type="password")
    if clave != "clicoh":
        if clave:
            st.error("Clave incorrecta")
        st.stop()

    base = cargar_base()
    hist = cargar_hist()

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Hoy",
        "📅 Histórico & KPIs",
        "🔥 Prioridades Base",
        "📤 Cargar Base",
        "🎯 Asignación Analistas",
    ])

    # ─── HOY ───
    with tab1:
        st.subheader("Resumen operativo de hoy")
        if hist.empty:
            st.info("Aún no hay gestión registrada hoy.")
        else:
            hoy = hist[hist["fecha"].dt.date == datetime.now().date()]
            total   = len(hoy)
            gest    = len(hoy[hoy["resultado"] == "Sí contestó"])
            inter   = len(hoy[hoy["estado"]    == "Interesado llega a cargue"])
            rech    = len(hoy[hoy["estado"]    == "Aliado Rechaza la oferta"])
            no_resp = len(hoy[hoy["resultado"].isin(NO_RESPONDEN)])

            c1,c2,c3,c4,c5 = st.columns(5)
            c1.metric("📞 Llamadas",    total)
            c2.metric("✅ Contactados", gest)
            c3.metric("🚗 Interesados", inter)
            c4.metric("❌ Rechazados",  rech)
            c5.metric("📵 No responden",no_resp)

            if total > 0:
                st.markdown("---")
                prod   = hoy.groupby("analista").size().reset_index(name="llamadas")
                inter_a= hoy[hoy["estado"]=="Interesado llega a cargue"].groupby("analista").size().reset_index(name="interesados")
                tabla  = prod.merge(inter_a, on="analista", how="left").fillna(0)
                tabla["interesados"]   = tabla["interesados"].astype(int)
                tabla["% efectividad"] = (tabla["interesados"]/tabla["llamadas"]*100).round(1)

                def semaforo(row):
                    if row["llamadas"] >= 30 and row["interesados"] >= 3: return "🟢"
                    elif row["llamadas"] >= 15: return "🟡"
                    return "🔴"

                tabla["estado"] = tabla.apply(semaforo, axis=1)
                st.dataframe(tabla, use_container_width=True)
                st.bar_chart(tabla.set_index("analista")["llamadas"])

    # ─── HISTÓRICO ───
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

            d       = hist[(hist["fecha"].dt.date >= f1) & (hist["fecha"].dt.date <= f2)]
            total   = len(d)
            si_resp = d[d["resultado"]=="Sí contestó"]
            no_resp = d[d["resultado"].isin(NO_RESPONDEN)]
            gest    = len(si_resp)
            inter   = len(d[d["estado"]=="Interesado llega a cargue"])
            rech    = len(d[d["estado"]=="Aliado Rechaza la oferta"])

            c1,c2,c3,c4,c5 = st.columns(5)
            c1.metric("📞 Total",       total)
            c2.metric("✅ Contactados", gest)
            c3.metric("% No responden", f"{round(len(no_resp)/total*100,1) if total else 0}%")
            c4.metric("% Gestión",      f"{round(gest/total*100,1) if total else 0}%")
            c5.metric("% Interesados",  f"{round(inter/total*100,1) if total else 0}%")

            c6,c7 = st.columns(2)
            c6.metric("% Rechazados",             f"{round(rech/total*100,1) if total else 0}%")
            c7.metric("% Rechazo (contactados)",  f"{round(rech/gest*100,1) if gest else 0}%")

            st.markdown("---")
            st.markdown("#### 📉 Embudo")
            embudo = pd.DataFrame({
                "Etapa":    ["Llamados","Contactados","Interesados"],
                "Cantidad": [total, gest, inter],
                "%":        [100, round(gest/total*100,1) if total else 0, round(inter/total*100,1) if total else 0]
            })
            st.dataframe(embudo, use_container_width=True)
            st.bar_chart(embudo.set_index("Etapa")["Cantidad"])

            st.markdown("---")
            st.markdown("#### Estado final (sobre contactados)")
            data_estado = [[e, len(si_resp[si_resp["estado"]==e]),
                           round(len(si_resp[si_resp["estado"]==e])/gest*100,1) if gest else 0]
                          for e in ESTADOS_FINALES]
            st.dataframe(pd.DataFrame(data_estado, columns=["Estado","Cantidad","%"]), use_container_width=True)

            st.markdown("---")
            st.markdown("#### Razones (sobre contactados)")
            data_razon = [[r, len(si_resp[si_resp["razon"]==r]),
                          round(len(si_resp[si_resp["razon"]==r])/gest*100,1) if gest else 0]
                         for r in RAZONES]
            st.dataframe(pd.DataFrame(data_razon, columns=["Razón","Cantidad","%"]), use_container_width=True)

            st.markdown("---")
            st.markdown("#### KPIs por analista")
            prod   = d.groupby("analista").size().reset_index(name="llamadas")
            gest_a = d[d["resultado"]=="Sí contestó"].groupby("analista").size().reset_index(name="gestionadas")
            int_a  = d[d["estado"]=="Interesado llega a cargue"].groupby("analista").size().reset_index(name="interesados")
            rec_a  = d[d["estado"]=="Aliado Rechaza la oferta"].groupby("analista").size().reset_index(name="rechazados")
            nor_a  = d[d["resultado"].isin(NO_RESPONDEN)].groupby("analista").size().reset_index(name="no_resp")

            tabla_a = (prod.merge(gest_a,on="analista",how="left")
                          .merge(int_a, on="analista",how="left")
                          .merge(rec_a, on="analista",how="left")
                          .merge(nor_a, on="analista",how="left")
                          .fillna(0))
            for col in ["gestionadas","interesados","rechazados","no_resp"]:
                tabla_a[col] = tabla_a[col].astype(int)
            tabla_a["% gestión"]      = (tabla_a["gestionadas"]/tabla_a["llamadas"]*100).round(1)
            tabla_a["% interesados"]  = (tabla_a["interesados"]/tabla_a["llamadas"]*100).round(1)
            tabla_a["% rechazados"]   = (tabla_a["rechazados"]/tabla_a["llamadas"]*100).round(1)
            tabla_a["% no responden"] = (tabla_a["no_resp"]/tabla_a["llamadas"]*100).round(1)
            st.dataframe(tabla_a, use_container_width=True)

            st.markdown("---")
            st.dataframe(d.sort_values("fecha", ascending=False), use_container_width=True)

    # ─── PRIORIDADES ───
    with tab3:
        if base is None:
            st.warning("Sube la base primero.")
        else:
            alta  = base[base["dias"] > 5]
            media = base[(base["dias"] > 1) & (base["dias"] <= 5)]
            baja  = base[base["dias"] <= 1]
            c1,c2,c3 = st.columns(3)
            c1.metric("🔴 ALTA (>5 días)",   len(alta))
            c2.metric("🟡 MEDIA (2-5 días)", len(media))
            c3.metric("🟢 BAJA (0-1 días)",  len(baja))
            st.markdown("---")
            cols_show = [c for c in ["identificacion","mensajero","celular","zona","vehiculo","dias"]
                        if c in alta.columns]
            st.dataframe(alta[cols_show].sort_values("dias",ascending=False).head(50),
                        use_container_width=True)

    # ─── CARGAR BASE ───
    with tab4:
        st.subheader("Cargar base diaria")
        st.info("La base se guarda en Google Sheets y queda disponible 24/7 para todas las analistas, sin que tengas que hacer nada más.")

        archivo = st.file_uploader("Selecciona archivo Excel (.xlsx)", type=["xlsx"])
        if archivo:
            try:
                df_nuevo = pd.read_excel(archivo, engine="openpyxl")
                st.success(f"Archivo leído: {len(df_nuevo):,} registros")

                if st.button("☁️ Subir base a Google Sheets"):
                    with st.spinner("Subiendo base... puede tardar 1-2 minutos según el tamaño."):
                        reemplazar_hoja("BASE", df_nuevo)
                        cargar_base.clear()
                    st.success(f"✅ Base subida exitosamente — {len(df_nuevo):,} aliados disponibles 24/7")
            except Exception as e:
                st.error(f"Error leyendo el archivo: {e}")

        if base is not None:
            st.info(f"Base activa: **{len(base):,} aliados** cargados en Google Sheets.")
            rechaz = cargar_rechazados()
            st.caption(f"Rechazados acumulados: {len(rechaz)}")

    # ─── ASIGNACIÓN ───
    with tab5:
        st.subheader("Configurar modo de trabajo")
        if base is None:
            st.warning("Necesitas cargar una base primero.")
        else:
            zonas = sorted(base["zona"].dropna().unique())
            vhs   = sorted(base["vehiculo_norm"].dropna().unique())

            modo = st.selectbox("Modo de asignación",[
                "Analista decide",
                "Asignación general (todos igual)",
                "Asignación por analista",
            ])

            data_conf = []

            if modo == "Asignación general (todos igual)":
                zona_g = st.selectbox("Zona para todos", zonas)
                vh_g   = st.selectbox("Vehículo para todos", vhs)
                data_conf = [{"analista":"TODOS","modo":modo,"zona":zona_g,"vehiculo":vh_g}]

            elif modo == "Asignación por analista":
                for a in NOMBRES_ANALISTAS:
                    st.markdown(f"**{a}**")
                    col1, col2 = st.columns(2)
                    with col1:
                        z = st.selectbox("Zona", zonas, key=f"zona_{a}")
                    with col2:
                        v = st.selectbox("Vehículo", vhs, key=f"vh_{a}")
                    data_conf.append({"analista":a,"modo":modo,"zona":z,"vehiculo":v})

            else:
                data_conf = [{"analista":"TODOS","modo":"Analista decide","zona":"","vehiculo":""}]

            if st.button("💾 Guardar asignación"):
                reemplazar_hoja("CONFIG", pd.DataFrame(data_conf))
                st.success("Asignación guardada en Google Sheets.")

            df_conf = leer_hoja("CONFIG")
            if not df_conf.empty:
                st.markdown("---")
                st.markdown("##### Configuración activa:")
                st.dataframe(df_conf, use_container_width=True)


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

    if modo_conf in ("Asignación general (todos igual)","Asignación por analista") and zona_conf and vh_conf:
        zona_sel = zona_conf
        vh_sel   = vh_conf
        st.success(f"🎯 Hoy debes gestionar: **{zona_sel}** — **{vh_sel}**")
    else:
        zonas    = sorted(base["zona"].dropna().unique())
        vhs      = sorted(base["vehiculo_norm"].dropna().unique())
        zona_sel = st.selectbox("Zona", zonas)
        vh_sel   = st.selectbox("Vehículo", vhs)

    # Pool con las 3 prioridades
    pool = base[(base["zona"]==zona_sel) & (base["vehiculo_norm"]==vh_sel)].copy()
    pool = filtrar_pool(pool)
    pool["PRIORIDAD"] = pool["dias"].apply(prioridad_label)

    orden_prio = {"🔴 ALTA":0,"🟡 MEDIA":1,"🟢 BAJA":2}
    pool["_orden"] = pool["PRIORIDAD"].map(orden_prio).fillna(3)
    pool = pool.sort_values("_orden").drop(columns=["_orden"]).reset_index(drop=True)

    # Quitar ya gestionados hoy
    if not hist.empty:
        gestionados_hoy = hist[hist["fecha"].dt.date == datetime.now().date()]["identificacion"].astype(str).tolist()
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
        pool = pool[pool["PRIORIDAD"]=="🔴 ALTA"]
    elif filtro_prio == "Solo 🟡 MEDIA":
        pool = pool[pool["PRIORIDAD"]=="🟡 MEDIA"]
    elif filtro_prio == "Solo 🟢 BAJA":
        pool = pool[pool["PRIORIDAD"]=="🟢 BAJA"]

    if st.button("🚀 Generar mis llamadas"):
        hoy_str = datetime.now().date().isoformat()
        reparto_df = cargar_reparto()

        if not reparto_df.empty and "fecha" in reparto_df.columns:
            if reparto_df["fecha"].iloc[0] != hoy_str:
                reparto_df = pd.DataFrame(columns=["fecha","analista","identificacion"])
        else:
            reparto_df = pd.DataFrame(columns=["fecha","analista","identificacion"])

        ya_asignados    = reparto_df[reparto_df["fecha"]==hoy_str]["identificacion"].astype(str).tolist()
        pool_disponible = pool[~pool["identificacion"].astype(str).isin(ya_asignados)]
        mi_bloque       = pool_disponible.head(int(cant)).reset_index(drop=True)

        if mi_bloque.empty:
            st.warning("⚠️ No hay más aliados disponibles en este filtro. Prueba otra zona o prioridad.")
        else:
            nuevos = pd.DataFrame({
                "fecha":          [hoy_str]*len(mi_bloque),
                "analista":       [nombre]*len(mi_bloque),
                "identificacion": mi_bloque["identificacion"].astype(str).tolist(),
            })
            reparto_nuevo = pd.concat([reparto_df, nuevos], ignore_index=True)
            guardar_reparto(reparto_nuevo)
            st.session_state["pool_activo"] = mi_bloque
            st.session_state["hechas"]      = st.session_state.get("hechas", 0)
            st.success(f"✅ Se te asignaron {len(mi_bloque)} aliados.")

    # Pool activo
    if "pool_activo" in st.session_state and not st.session_state["pool_activo"].empty:

        pool_activo = st.session_state["pool_activo"]
        hechas      = st.session_state.get("hechas", 0)
        restantes   = len(pool_activo)
        pct         = int(hechas/(hechas+restantes)*100) if (hechas+restantes) > 0 else 0
        st.progress(pct, text=f"Progreso: {hechas} gestionados / {restantes} pendientes")

        cols_mostrar = [c for c in ["identificacion","mensajero","celular","zona","vehiculo","dias","PRIORIDAD"]
                        if c in pool_activo.columns]
        st.markdown("#### 📋 Aliados pendientes")
        st.dataframe(pool_activo[cols_mostrar], use_container_width=True)

        st.markdown("---")
        st.markdown("#### 📞 Registrar gestión")

        with st.form(key="form_gestion", clear_on_submit=True):
            aliado_sel = st.selectbox("Cédula del aliado", pool_activo["identificacion"].astype(str).tolist())

            fila = pool_activo[pool_activo["identificacion"].astype(str)==aliado_sel]
            if not fila.empty:
                f = fila.iloc[0]
                info_cols = [c for c in ["mensajero","celular","PRIORIDAD"] if c in f.index]
                cols_info = st.columns(len(info_cols))
                for i, col_name in enumerate(info_cols):
                    cols_info[i].metric(col_name.capitalize(), str(f[col_name]))

            resultado = st.selectbox("Resultado de la llamada", RESULTADOS)
            estado    = st.selectbox("Estado final (solo si contestó)", ["-"] + ESTADOS_FINALES)
            razon     = st.selectbox("Razón (solo si contestó)", ["-"] + RAZONES)
            obs       = st.text_area("Observación (opcional)")
            submitted = st.form_submit_button("💾 Guardar y siguiente")

        if submitted:
            estado_real = None if estado=="-" else estado
            razon_real  = None if razon=="-"  else razon

            if resultado=="Sí contestó" and estado_real is None:
                st.error("Si el aliado contestó, selecciona un Estado final.")
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

                if estado_real == "Aliado Rechaza la oferta":
                    guardar_rechazado(aliado_sel)
                elif estado_real in ("Interesado llega a cargue","Aliado Fleet/Delivery no acepta hub"):
                    guardar_pausa(aliado_sel)

                nuevo_pool = pool_activo[pool_activo["identificacion"].astype(str)!=aliado_sel]
                st.session_state["pool_activo"] = nuevo_pool.reset_index(drop=True)
                st.session_state["hechas"]      = hechas + 1

                if nuevo_pool.empty:
                    st.success("✅ ¡Completaste todas tus llamadas! Puedes generar más.")
                    del st.session_state["pool_activo"]
                    st.session_state["hechas"] = 0

                st.rerun()

    # Resumen de hoy del analista
    if not hist.empty:
        mis = hist[(hist["analista"]==nombre) & (hist["fecha"].dt.date==datetime.now().date())]
        if not mis.empty:
            st.markdown("---")
            st.markdown(f"#### 📈 Tus gestiones de hoy ({len(mis)} registros)")
            st.dataframe(mis[["fecha","identificacion","resultado","estado","razon"]],
                        use_container_width=True)
