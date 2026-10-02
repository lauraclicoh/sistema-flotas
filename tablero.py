import unicodedata

import pandas as pd
import streamlit as st

NO_RESPONDEN = ["Apagado", "Fuera de servicio", "No contestó", "Número errado"]
ESTADOS_FINALES = [
    "Aliado Rechaza la oferta",
    "Aliado Fleet/Delivery no acepta hub",
    "Interesado llega a cargue/ Programado",
    "Interesado esporádico",
    "Empleado",
    "Point",
]
ALIAS_ID = ["identificacion", "cedula", "id_aliado", "documento", "id"]


# ---------- utilidades ----------
def _key(x):
    s = unicodedata.normalize("NFKD", str(x)).encode("ascii", "ignore").decode().lower().strip()
    return "bogota" if s.startswith("bogota") else s


def _label(x):
    return "Bogotá" if _key(x) == "bogota" else str(x).strip().title()


def _pct(n, d):
    return round(n / d * 100, 1) if d else 0.0


def _norm_cols(df):
    df = df.copy()
    df.columns = df.columns.astype(str).str.strip().str.lower().str.replace(r"\s+", "_", regex=True)
    return df


def _card(col, label, value, caption=""):
    with col:
        with st.container(border=True):
            st.metric(label, f"{value:,}")
            st.caption(caption or " ")


def _ultimo_cargue(cargues):
    """Devuelve {cédula: fecha del último cargue} o None si no se reconoce la hoja."""
    if cargues is None or cargues.empty:
        return {}
    c = _norm_cols(cargues)
    col_id = next((k for k in ALIAS_ID if k in c.columns), None)
    if col_id is None:
        return None
    c["_id"] = c[col_id].astype(str).str.strip()
    col_f = next((k for k in c.columns if "fecha" in k), None)
    if col_f is None:  # sin fecha: basta con que haya cargado alguna vez
        return {i: pd.Timestamp("2100-01-01") for i in c["_id"].unique()}
    c["_f"] = pd.to_datetime(c[col_f], errors="coerce")
    return c.dropna(subset=["_f"]).groupby("_id")["_f"].max().to_dict()


# ---------- tablero ----------
def render_tablero(base, hist, leer_hoja):
    st.subheader("🚚 Gestión Aliados Programación")

    if base is None or len(base) == 0:
        st.warning("Carga la base primero.")
        return

    if "cargues_df" not in st.session_state:
        st.session_state["cargues_df"] = leer_hoja("CARGUES_REALES")
    ult_cargue = _ultimo_cargue(st.session_state["cargues_df"])

    base = base.copy()
    base["identificacion"] = base["identificacion"].astype(str).str.strip()
    base = base.drop_duplicates("identificacion")
    col_ciudad = "municipio" if "municipio" in base.columns else "zona"
    base["_k"] = base[col_ciudad].map(_key)
    etiquetas = {}
    for k, v in zip(base["_k"], base[col_ciudad]):
        etiquetas.setdefault(k, _label(v))

    # --- filtros ---
    f1, f2, f3 = st.columns([2, 2, 1])
    ciudad = f1.selectbox("Ciudad", ["Todas"] + sorted(etiquetas.values()))
    periodo = f2.selectbox(
        "Periodo de gestión",
        ["Todo el histórico", "Hoy", "Últimos 7 días", "Últimos 30 días", "Este mes"],
    )
    if f3.button("🔄 Actualizar", use_container_width=True):
        st.session_state.pop("cargues_df", None)
        st.session_state["base_stale"] = True
        st.session_state["hist_last_load"] = 0
        st.rerun()

    if ciudad != "Todas":
        k_sel = next(k for k, v in etiquetas.items() if v == ciudad)
        base = base[base["_k"] == k_sel]

    # historial filtrado por ciudad y periodo
    h = hist.copy() if hist is not None else pd.DataFrame()
    if not h.empty:
        h["identificacion"] = h["identificacion"].astype(str).str.strip()
        h = h[h["identificacion"].isin(base["identificacion"])]
        hoy = pd.Timestamp.now(tz="America/Bogota").tz_localize(None).normalize()
        desde = {
            "Hoy": hoy,
            "Últimos 7 días": hoy - pd.Timedelta(days=6),
            "Últimos 30 días": hoy - pd.Timedelta(days=29),
            "Este mes": hoy.replace(day=1),
        }.get(periodo)
        if desde is not None:
            h = h[h["fecha"] >= desde]

    # --- 1. estado de la base ---
    prox = base["proxima_gestion"].astype(str).str.strip() if "proxima_gestion" in base.columns \
        else pd.Series("", index=base.index)
    bloq = prox.str.upper() == "NO_VOLVER"
    fut = pd.to_datetime(prox, errors="coerce") > pd.Timestamp.now(tz="America/Bogota").tz_localize(None)
    estado = base["estado_aliado"].astype(str) if "estado_aliado" in base.columns \
        else pd.Series("", index=base.index)
    activos = int((~estado.str.contains("Inactivo")).sum())

    st.markdown("##### Estado de la base")
    c = st.columns(3)
    _card(c[0], "✅ Aliados activos", activos)
    _card(c[1], "⏸ En pausa", int((fut & ~bloq).sum()))
    _card(c[2], "🚫 Bloqueados", int(bloq.sum()))

    if h.empty:
        st.info("Sin gestiones en este filtro.")
        return

    # --- 2. embudo (última gestión de cada aliado) ---
    ult = h.sort_values("fecha").drop_duplicates("identificacion", keep="last")
    llamados = len(ult)
    no_resp = int(ult["resultado"].isin(NO_RESPONDEN).sum())
    gestionados = int((ult["resultado"] == "Sí contestó").sum())
    est = ult["estado"].fillna("")
    interesados = int(est.str.startswith("Interesado llega").sum())
    rechazados = int((est == "Aliado Rechaza la oferta").sum())

    st.markdown("##### Embudo de gestión")
    c = st.columns(5)
    _card(c[0], "📞 Llamados", llamados, f"{_pct(llamados, len(base))}% de la base")
    _card(c[1], "📵 No responden", no_resp, f"{_pct(no_resp, llamados)}% de los llamados")
    _card(c[2], "✅ Gestionados", gestionados, f"{_pct(gestionados, llamados)}% de los llamados")
    _card(c[3], "🚗 Interesados", interesados, f"{_pct(interesados, llamados)}% de los llamados")
    _card(c[4], "❌ Rechazados", rechazados, f"{_pct(rechazados, llamados)}% de los llamados")

    # --- 3. por analista ---
    st.markdown("##### Gestión por analista")
    if ult_cargue is None:
        st.caption("⚠️ No encontré la columna de cédula en CARGUES_REALES; 'Cargaron' aparece en 0.")
        ult_cargue = {}
    filas = []
    for analista, x in h.groupby("analista"):
        inter = (
            x[x["estado"].fillna("").str.startswith("Interesado llega")]
            .sort_values("fecha")
            .drop_duplicates("identificacion")
        )
        cargaron = sum(
            1
            for i, f in zip(inter["identificacion"], inter["fecha"])
            if ult_cargue.get(i) is not None and ult_cargue[i] >= f.normalize()
        )
        contactos = int((x["resultado"] == "Sí contestó").sum())
        filas.append({
            "Analista": analista,
            "Llamados": len(x),
            "Contactos": contactos,
            "Interesados": len(inter),
            "Cargaron": cargaron,
            "% cargue": _pct(cargaron, len(inter)),
        })
    st.dataframe(
        pd.DataFrame(filas).sort_values("Llamados", ascending=False),
        hide_index=True,
        use_container_width=True,
        column_config={"% cargue": st.column_config.ProgressColumn(
            "% cargue", min_value=0, max_value=100, format="%.1f%%")},
    )

    # --- 4. estado final ---
    st.markdown("##### Estado final (sobre contactados)")
    contactados = ult[ult["resultado"] == "Sí contestó"]
    n_cont = len(contactados)
    tabla = [
        {"Estado final": e, "N°": int((contactados["estado"] == e).sum()),
         "%": _pct(int((contactados["estado"] == e).sum()), n_cont)}
        for e in ESTADOS_FINALES
    ]
    st.dataframe(
        pd.DataFrame(tabla),
        hide_index=True,
        use_container_width=True,
        column_config={"%": st.column_config.ProgressColumn("%", min_value=0, max_value=100, format="%.1f%%")},
    )
    st.caption(f"Total contactados: {n_cont:,}")
