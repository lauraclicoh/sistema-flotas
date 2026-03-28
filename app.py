import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import os

st.set_page_config(layout="wide", page_title="Gestión Programación Aliados")

# ================= ARCHIVOS =================
BASE    = "base.xlsx"
HIST    = "historico.csv"
CONFIG  = "config.csv"
RECHAZ  = "rechazados.csv"
PAUSAS  = "pausas.csv"

# ================= ANALISTAS =================
ANALISTAS = {
    "Deisy Liliana Garcia":   "dgarcia@clicoh.com",
    "Erica Tatiana Garzon":   "etgarzon@clicoh.com",
    "Dayan Stefany Suarez":   "dsuarez@clicoh.com",
    "Carlos Andres Loaiza":   "cloaiza@clicoh.com",
}

NOMBRES_ANALISTAS = list(ANALISTAS.keys())

# ================= OPCIONES FORMULARIO =================
RESULTADOS = [
    "Apagado",
    "Fuera de servicio",
    "No contestó",
    "Número errado",
    "Sí contestó",
]

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

NO_RESPONDEN = ["Apagado", "Fuera de servicio", "No contestó", "Número errado"]

# ================= FUNCIONES AUXILIARES =================

def cargar_base():
    if not os.path.exists(BASE):
        return None
    try:
        df = pd.read_excel(BASE, engine="openpyxl")
    except Exception:
        return None

    # Normalizar nombres de columnas
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # Alias comunes para identificacion
    if "identificacion" not in df.columns:
        for alias in ["id_aliado", "id", "cedula", "documento"]:
            if alias in df.columns:
                df["identificacion"] = df[alias]
                break

    # Alias para celular
    if "celular" not in df.columns:
        for alias in ["telefono", "tel", "phone"]:
            if alias in df.columns:
                df["celular"] = df[alias]
                break

    # Alias para zona
    if "zona" not in df.columns and "municipio" in df.columns:
        df["zona"] = df["municipio"]

    # Normalizar vehiculo → Carry agrupa carry + largenvan + small van, etc.
    if "vehiculo" in df.columns:
        df["vehiculo_norm"] = df["vehiculo"].astype(str).str.lower().apply(
            lambda x: (
                "Carry / Van"   if any(k in x for k in ["carry", "largenvan", "large van", "small van", "van"]) else
                "Moto"          if "moto" in x else
                "Camión"        if any(k in x for k in ["camion", "camión", "truck", "npr"]) else
                x.title()
            )
        )
    else:
        df["vehiculo_norm"] = "Sin vehículo"

    # Fecha último cargue → dias sin cargar
    col_fecha = None
    for posible in ["fecha_ultimo_cargue", "fecha ultimo cargue", "fechaultimocargue"]:
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


def prioridad_label(dias):
    if dias <= 1:
        return "🟢 BAJA"
    elif dias <= 5:
        return "🟡 MEDIA"
    else:
        return "🔴 ALTA"


def cargar_hist():
    if not os.path.exists(HIST):
        return pd.DataFrame(columns=["fecha","analista","identificacion","resultado","estado","razon","obs"])
    df = pd.read_csv(HIST)
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    return df


def cargar_rechazados():
    if not os.path.exists(RECHAZ):
        return []
    return pd.read_csv(RECHAZ)["identificacion"].astype(str).tolist()


def cargar_pausas():
    if not os.path.exists(PAUSAS):
        return []
    p = pd.read_csv(PAUSAS)
    p["fecha_pausa"] = pd.to_datetime(p["fecha_pausa"], errors="coerce")
    activas = p[(datetime.now() - p["fecha_pausa"]).dt.days <= 5]
    return activas["identificacion"].astype(str).tolist()


def guardar_rechazado(ident):
    nuevo = pd.DataFrame({"identificacion": [str(ident)]})
    if os.path.exists(RECHAZ):
        nuevo.to_csv(RECHAZ, mode="a", header=False, index=False)
    else:
        nuevo.to_csv(RECHAZ, index=False)


def guardar_pausa(ident):
    nuevo = pd.DataFrame({"identificacion": [str(ident)], "fecha_pausa": [datetime.now()]})
    if os.path.exists(PAUSAS):
        nuevo.to_csv(PAUSAS, mode="a", header=False, index=False)
    else:
        nuevo.to_csv(PAUSAS, index=False)


def guardar_gestion(row_dict):
    reg = pd.DataFrame([row_dict])
    if os.path.exists(HIST):
        reg.to_csv(HIST, mode="a", header=False, index=False)
    else:
        reg.to_csv(HIST, index=False)


def filtrar_pool(df_zona):
    """Elimina rechazados y pausados del pool."""
    rechazados = cargar_rechazados()
    pausados   = cargar_pausas()
    bloqueados = set(rechazados) | set(pausados)
    return df_zona[~df_zona["identificacion"].astype(str).isin(bloqueados)]


# ================= CARGA DE CONFIG =================
def leer_config(nombre_analista):
    """Devuelve (modo, zona, vehiculo) para un analista."""
    if not os.path.exists(CONFIG):
        return "Analista decide", None, None
    conf = pd.read_csv(CONFIG)
    if "analista" not in conf.columns:
        return "Analista decide", None, None
    fila = conf[conf["analista"] == nombre_analista]
    if not fila.empty:
        r = fila.iloc[-1]
        return r.get("modo","Analista decide"), r.get("zona"), r.get("vehiculo")
    fila_todos = conf[conf["analista"] == "TODOS"]
    if not fila_todos.empty:
        r = fila_todos.iloc[-1]
        return r.get("modo","Analista decide"), r.get("zona"), r.get("vehiculo")
    return "Analista decide", None, None


# ================================================================
#  UI PRINCIPAL
# ================================================================

st.title("🚚 Gestión Programación de Aliados")

perfil = st.selectbox("Perfil", ["Coordinador", "Analista"])

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

    # ─────────────── TAB 1: HOY ───────────────
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

            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("📞 Llamadas", total)
            c2.metric("✅ Contactados", gest)
            c3.metric("🚗 Interesados", inter)
            c4.metric("❌ Rechazados", rech)
            c5.metric("📵 No responden", no_resp)

            if total > 0:
                st.markdown("---")
                st.subheader("Productividad por analista")
                prod  = hoy.groupby("analista").size().reset_index(name="llamadas")
                inter_a = (
                    hoy[hoy["estado"] == "Interesado llega a cargue"]
                    .groupby("analista").size().reset_index(name="interesados")
                )
                tabla = prod.merge(inter_a, on="analista", how="left").fillna(0)
                tabla["interesados"] = tabla["interesados"].astype(int)
                tabla["% efectividad"] = (tabla["interesados"] / tabla["llamadas"] * 100).round(1)

                def semaforo(row):
                    if row["llamadas"] >= 30 and row["interesados"] >= 3:
                        return "🟢"
                    elif row["llamadas"] >= 15:
                        return "🟡"
                    return "🔴"

                tabla["estado"] = tabla.apply(semaforo, axis=1)
                st.dataframe(tabla, use_container_width=True)
                st.bar_chart(tabla.set_index("analista")["llamadas"])

    # ─────────────── TAB 2: HISTÓRICO ───────────────
    with tab2:
        st.subheader("Histórico con KPIs por rango de fechas")

        if hist.empty:
            st.info("No hay histórico aún.")
        else:
            col_f1, col_f2 = st.columns(2)
            with col_f1:
                f1 = st.date_input("Desde", datetime.now().date() - timedelta(days=7))
            with col_f2:
                f2 = st.date_input("Hasta", datetime.now().date())

            d = hist[(hist["fecha"].dt.date >= f1) & (hist["fecha"].dt.date <= f2)]
            total    = len(d)
            si_resp  = d[d["resultado"] == "Sí contestó"]
            no_resp  = d[d["resultado"].isin(NO_RESPONDEN)]
            gest     = len(si_resp)
            inter    = len(d[d["estado"] == "Interesado llega a cargue"])
            rech     = len(d[d["estado"] == "Aliado Rechaza la oferta"])

            # KPIs generales
            st.markdown("#### KPIs generales")
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("📞 Total llamadas",   total)
            c2.metric("✅ Contactados",       gest)
            c3.metric("% No responden",       f"{round(len(no_resp)/total*100,1) if total else 0}%")
            c4.metric("% Gestión",            f"{round(gest/total*100,1) if total else 0}%")
            c5.metric("% Interesados",        f"{round(inter/total*100,1) if total else 0}%")

            c6, c7 = st.columns(2)
            c6.metric("% Rechazados",
                      f"{round(rech/total*100,1) if total else 0}%")
            c7.metric("% Rechazo (sobre contactados)",
                      f"{round(rech/gest*100,1) if gest else 0}%")

            st.markdown("---")

            # Embudo
            st.markdown("#### 📉 Embudo de conversión")
            embudo = pd.DataFrame({
                "Etapa":    ["Llamados", "Contactados", "Interesados"],
                "Cantidad": [total, gest, inter],
            })
            if total > 0:
                embudo["%"] = (embudo["Cantidad"] / total * 100).round(1)
            else:
                embudo["%"] = 0
            st.dataframe(embudo, use_container_width=True)
            st.bar_chart(embudo.set_index("Etapa")["Cantidad"])

            st.markdown("---")

            # Estado final
            st.markdown("#### Estado final (sobre contactados)")
            data_estado = []
            for e in ESTADOS_FINALES:
                cant = len(si_resp[si_resp["estado"] == e])
                data_estado.append([e, cant, round(cant/gest*100,1) if gest else 0])
            st.dataframe(
                pd.DataFrame(data_estado, columns=["Estado","Cantidad","%"]),
                use_container_width=True,
            )

            st.markdown("---")

            # Razones
            st.markdown("#### Razones (sobre contactados)")
            data_razon = []
            for r in RAZONES:
                cant = len(si_resp[si_resp["razon"] == r])
                data_razon.append([r, cant, round(cant/gest*100,1) if gest else 0])
            st.dataframe(
                pd.DataFrame(data_razon, columns=["Razón","Cantidad","%"]),
                use_container_width=True,
            )

            st.markdown("---")

            # KPI por analista
            st.markdown("#### KPIs por analista")
            prod = d.groupby("analista").size().reset_index(name="llamadas")
            for col_nombre, col_nuevo in [
                ("resultado", "Sí contestó",),
                ("estado",    "Interesado llega a cargue"),
                ("estado",    "Aliado Rechaza la oferta"),
            ]:
                pass  # se calculan abajo de forma limpia

            gest_a = d[d["resultado"]=="Sí contestó"].groupby("analista").size().reset_index(name="gestionadas")
            int_a  = d[d["estado"]=="Interesado llega a cargue"].groupby("analista").size().reset_index(name="interesados")
            rec_a  = d[d["estado"]=="Aliado Rechaza la oferta"].groupby("analista").size().reset_index(name="rechazados")
            nor_a  = d[d["resultado"].isin(NO_RESPONDEN)].groupby("analista").size().reset_index(name="no_resp")

            tabla_a = (prod
                       .merge(gest_a, on="analista", how="left")
                       .merge(int_a,  on="analista", how="left")
                       .merge(rec_a,  on="analista", how="left")
                       .merge(nor_a,  on="analista", how="left")
                       .fillna(0))

            for col in ["gestionadas","interesados","rechazados","no_resp"]:
                tabla_a[col] = tabla_a[col].astype(int)

            tabla_a["% gestión"]     = (tabla_a["gestionadas"]  / tabla_a["llamadas"] * 100).round(1)
            tabla_a["% interesados"] = (tabla_a["interesados"]  / tabla_a["llamadas"] * 100).round(1)
            tabla_a["% rechazados"]  = (tabla_a["rechazados"]   / tabla_a["llamadas"] * 100).round(1)
            tabla_a["% no responden"]= (tabla_a["no_resp"]      / tabla_a["llamadas"] * 100).round(1)

            st.dataframe(tabla_a, use_container_width=True)
            st.bar_chart(tabla_a.set_index("analista")[["% gestión","% interesados"]])

            st.markdown("---")
            st.markdown("#### Detalle completo")
            st.dataframe(d.sort_values("fecha", ascending=False), use_container_width=True)

    # ─────────────── TAB 3: PRIORIDADES ───────────────
    with tab3:
        st.subheader("Distribución de prioridad en la base actual")

        if base is None:
            st.warning("Sube la base primero en la pestaña 📤 Cargar Base.")
        else:
            alta  = base[base["dias"] > 5]
            media = base[(base["dias"] > 1) & (base["dias"] <= 5)]
            baja  = base[base["dias"] <= 1]

            c1, c2, c3 = st.columns(3)
            c1.metric("🔴 ALTA (>5 días)",   len(alta))
            c2.metric("🟡 MEDIA (2-5 días)", len(media))
            c3.metric("🟢 BAJA (0-1 días)",  len(baja))

            st.markdown("---")
            st.markdown("##### Top 50 aliados prioritarios (ALTA)")
            cols_show = [c for c in ["identificacion","mensajero","celular","zona","vehiculo","dias"]
                         if c in alta.columns]
            st.dataframe(alta[cols_show].sort_values("dias", ascending=False).head(50),
                         use_container_width=True)

    # ─────────────── TAB 4: BASE ───────────────
    with tab4:
        st.subheader("Cargar base diaria")
        st.info("La base que subas hoy reemplaza la anterior. Los rechazados y pausas se mantienen.")

        archivo = st.file_uploader("Selecciona archivo Excel (.xlsx)", type=["xlsx"])

        if archivo:
            try:
                df_nuevo = pd.read_excel(archivo, engine="openpyxl")
                df_nuevo.to_excel(BASE, index=False)
                base = cargar_base()
                if base is not None:
                    st.success(f"✅ Base cargada — {len(base):,} registros disponibles.")
                else:
                    st.error("El archivo se leyó pero tiene un problema en las columnas. Verifica que sea un Excel válido (.xlsx).")
            except Exception as e:
                st.error(f"❌ No se pudo leer el archivo. Asegúrate de que sea un Excel (.xlsx) válido.")
                st.caption(f"Detalle técnico: {str(e)}")

        if base is not None:
            st.info(f"Base activa: **{len(base):,} aliados** cargados.")
            rechaz = cargar_rechazados()
            st.caption(f"Rechazados acumulados (no vuelven a salir): {len(rechaz)}")

    # ─────────────── TAB 5: ASIGNACIÓN ───────────────
    with tab5:
        st.subheader("Configurar modo de trabajo para los analistas")

        if base is None:
            st.warning("Necesitas cargar una base primero.")
        else:
            zonas = sorted(base["zona"].dropna().unique())
            vhs   = sorted(base["vehiculo_norm"].dropna().unique())

            modo = st.selectbox("Modo de asignación", [
                "Analista decide",
                "Asignación general (todos igual)",
                "Asignación por analista",
            ])

            if modo == "Asignación general (todos igual)":
                zona_g = st.selectbox("Zona para todos", zonas)
                vh_g   = st.selectbox("Vehículo para todos", vhs)

                if st.button("💾 Guardar asignación general"):
                    pd.DataFrame({
                        "analista": ["TODOS"],
                        "modo":     [modo],
                        "zona":     [zona_g],
                        "vehiculo": [vh_g],
                    }).to_csv(CONFIG, index=False)
                    st.success("Asignación general guardada.")

            elif modo == "Asignación por analista":
                st.markdown("Define zona y vehículo para cada analista:")
                data_conf = []

                for a in NOMBRES_ANALISTAS:
                    st.markdown(f"**{a}**")
                    col1, col2 = st.columns(2)
                    with col1:
                        z = st.selectbox("Zona", zonas, key=f"zona_{a}")
                    with col2:
                        v = st.selectbox("Vehículo", vhs, key=f"vh_{a}")
                    data_conf.append({"analista": a, "modo": modo, "zona": z, "vehiculo": v})

                if st.button("💾 Guardar asignación por analista"):
                    pd.DataFrame(data_conf).to_csv(CONFIG, index=False)
                    st.success("Asignación por analista guardada.")

            else:  # Analista decide
                if st.button("💾 Activar modo libre"):
                    pd.DataFrame({"modo": ["Analista decide"]}).to_csv(CONFIG, index=False)
                    st.success("Modo libre activado.")

            # Vista resumen de config actual
            if os.path.exists(CONFIG):
                st.markdown("---")
                st.markdown("##### Configuración activa:")
                st.dataframe(pd.read_csv(CONFIG), use_container_width=True)


# ================================================================
#  ANALISTA
# ================================================================

if perfil == "Analista":

    base = cargar_base()
    hist = cargar_hist()

    if base is None:
        st.warning("⚠️ La coordinadora aún no ha cargado la base del día. Espera un momento.")
        st.stop()

    st.markdown("---")

    nombre = st.selectbox("¿Quién eres?", NOMBRES_ANALISTAS)

    # Leer config asignada
    modo_conf, zona_conf, vh_conf = leer_config(nombre)

    # ──── Definir zona y vehículo ────
    if modo_conf in ("Asignación general (todos igual)", "Asignación por analista") and zona_conf and vh_conf:
        zona_sel = zona_conf
        vh_sel   = vh_conf
        st.success(f"🎯 Hoy debes gestionar: **{zona_sel}** — **{vh_sel}**")
    else:
        st.info("Selecciona zona y tipo de vehículo para trabajar hoy:")
        zonas = sorted(base["zona"].dropna().unique())
        vhs   = sorted(base["vehiculo_norm"].dropna().unique())
        zona_sel = st.selectbox("Zona", zonas)
        vh_sel   = st.selectbox("Vehículo", vhs)

    # ──── Filtrar y priorizar pool ────
    pool = base[(base["zona"] == zona_sel) & (base["vehiculo_norm"] == vh_sel)].copy()
    pool = filtrar_pool(pool)
    pool["PRIORIDAD"] = pool["dias"].apply(prioridad_label)

    orden_prio = {"🔴 ALTA": 0, "🟡 MEDIA": 1, "🟢 BAJA": 2}
    pool["_orden"] = pool["PRIORIDAD"].map(orden_prio).fillna(3)
    pool = pool.sort_values("_orden").drop(columns=["_orden"]).reset_index(drop=True)

    # Quitar aliados ya gestionados hoy por cualquier analista
    if not hist.empty:
        gestionados_hoy = hist[hist["fecha"].dt.date == datetime.now().date()]["identificacion"].astype(str).tolist()
        pool = pool[~pool["identificacion"].astype(str).isin(gestionados_hoy)]

    REPARTO_FILE = "reparto.csv"
    cant = st.number_input("Cantidad de aliados a gestionar", min_value=10, max_value=150, value=30)

    if st.button("🚀 Generar mis llamadas"):
        hoy_str = datetime.now().date().isoformat()

        if os.path.exists(REPARTO_FILE):
            reparto_df = pd.read_csv(REPARTO_FILE)
            if "fecha" not in reparto_df.columns or (len(reparto_df) > 0 and reparto_df["fecha"].iloc[0] != hoy_str):
                reparto_df = pd.DataFrame(columns=["fecha","analista","identificacion"])
        else:
            reparto_df = pd.DataFrame(columns=["fecha","analista","identificacion"])

        ya_asignados = reparto_df[reparto_df["fecha"] == hoy_str]["identificacion"].astype(str).tolist()
        pool_disponible = pool[~pool["identificacion"].astype(str).isin(ya_asignados)]
        mi_bloque = pool_disponible.head(int(cant)).reset_index(drop=True)

        if mi_bloque.empty:
            st.warning("No hay mas aliados disponibles. Todos ya fueron asignados a otros analistas.")
        else:
            nuevos = pd.DataFrame({
                "fecha":          [hoy_str] * len(mi_bloque),
                "analista":       [nombre]  * len(mi_bloque),
                "identificacion": mi_bloque["identificacion"].astype(str).tolist(),
            })
            reparto_actualizado = pd.concat([reparto_df, nuevos], ignore_index=True)
            reparto_actualizado.to_csv(REPARTO_FILE, index=False)
            st.session_state["pool_activo"] = mi_bloque
            st.success(f"Se te asignaron {len(mi_bloque)} aliados unicos.")

    # ──── Mostrar pool activo ────
    if "pool_activo" in st.session_state and not st.session_state["pool_activo"].empty:

        pool_activo = st.session_state["pool_activo"]

        cols_mostrar = [c for c in ["identificacion","mensajero","celular","zona","vehiculo","dias","PRIORIDAD"]
                        if c in pool_activo.columns]

        st.markdown("#### 📋 Aliados a gestionar")
        st.dataframe(pool_activo[cols_mostrar], use_container_width=True)

        st.markdown("---")
        st.markdown("#### 📞 Registrar gestión")

        opciones_aliado = pool_activo["identificacion"].astype(str).tolist()
        aliado_sel = st.selectbox("Selecciona la cédula del aliado", opciones_aliado)

        # Mostrar info del aliado seleccionado
        fila = pool_activo[pool_activo["identificacion"].astype(str) == aliado_sel]
        if not fila.empty:
            f = fila.iloc[0]
            info_cols = [c for c in ["mensajero","celular","zona","vehiculo","PRIORIDAD"] if c in f.index]
            cols_info = st.columns(len(info_cols))
            for i, col_name in enumerate(info_cols):
                cols_info[i].metric(col_name.capitalize(), str(f[col_name]))

        resultado = st.selectbox("Resultado de la llamada", RESULTADOS)

        estado = None
        razon  = None
        obs    = ""

        # Solo pedir estado/razón si contestó
        if resultado == "Sí contestó":
            estado = st.selectbox("Estado final", ESTADOS_FINALES)
            razon  = st.selectbox("Razón", RAZONES)
            obs    = st.text_area("Observación adicional (opcional)")

        if st.button("💾 Guardar gestión"):

            row_dict = {
                "fecha":          datetime.now(),
                "analista":       nombre,
                "identificacion": aliado_sel,
                "resultado":      resultado,
                "estado":         estado,
                "razon":          razon,
                "obs":            obs,
            }

            guardar_gestion(row_dict)

            # Lógica de bloqueos
            if estado == "Aliado Rechaza la oferta":
                guardar_rechazado(aliado_sel)
                st.warning("🚫 Aliado marcado como rechazado — no volverá a aparecer.")

            elif estado in ("Interesado llega a cargue", "Aliado Fleet/Delivery no acepta hub"):
                guardar_pausa(aliado_sel)
                st.info("⏸ Aliado en pausa 5 días.")

            # Quitar del pool activo
            nuevo_pool = pool_activo[pool_activo["identificacion"].astype(str) != aliado_sel]
            st.session_state["pool_activo"] = nuevo_pool.reset_index(drop=True)

            pendientes = len(nuevo_pool)
            if pendientes > 0:
                st.success(f"✅ Guardado. Te quedan **{pendientes}** aliados pendientes.")
            else:
                st.success("✅ ¡Completaste todas tus llamadas del pool! Puedes generar más si necesitas.")
                del st.session_state["pool_activo"]

        # Contador de progreso
        if "pool_activo" in st.session_state:
            total_orig = int(cant)
            restantes  = len(st.session_state["pool_activo"])
            hechas     = total_orig - restantes
            pct        = int(hechas / total_orig * 100) if total_orig else 0
            st.progress(pct, text=f"Progreso: {hechas}/{total_orig} llamadas ({pct}%)")

    # Resumen del analista (sus gestiones de hoy)
    if not hist.empty:
        mis = hist[(hist["analista"] == nombre) &
                   (hist["fecha"].dt.date == datetime.now().date())]
        if not mis.empty:
            st.markdown("---")
            st.markdown(f"#### 📈 Tus gestiones de hoy ({len(mis)} registros)")
            st.dataframe(mis[["fecha","identificacion","resultado","estado","razon"]],
                         use_container_width=True)
