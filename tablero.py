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
RAZONES_INTERES = ["Interesado carga hoy/ reserva"]   # igual que RAZONES_VALIDACION de app.py
ESTADOS_HR_CARGUE = {"abierto", "cerrado"}            # igual que app.py: solo HR que sí salieron
CIUDADES_CON_TILDE = {"bogota": "Bogotá", "medellin": "Medellín", "ibague": "Ibagué"}


# ---------- utilidades ----------
def _key(x):
    s = unicodedata.normalize("NFKD", str(x)).encode("ascii", "ignore").decode().lower().strip()
    if s.startswith("zona "):
        s = s[5:].strip()
    return "bogota" if s.startswith("bogota") else s


def _label(x):
    k = _key(x)
    return CIUDADES_CON_TILDE.get(k, k.title()) if k else "Sin zona"


def _pct(n, d):
    return round(n / d * 100, 1) if d else 0.0


def _nid(valor):
    """Cédula como texto sin '.0' (para cruzar BASE, HISTORICO y CARGUES_REALES)."""
    v = str(valor).strip()
    return v[:-2] if v.endswith(".0") else v


def _card(col, label, value, caption=""):
    with col:
        with st.container(border=True):
            st.metric(label, f"{value:,}")
            st.caption(caption or " ")


def _es_interesado(df):
    return df["estado"].fillna("").str.startswith("Interesado llega") | df["razon"].fillna("").isin(RAZONES_INTERES)


def _ultimo_cargue(cargues):
    """{cédula: fecha de su último cargue real} usando solo HR Abierta/Cerrada."""
    if cargues is None or len(cargues) == 0:
        return {}
    c = cargues.copy()
    c.columns = c.columns.astype(str).str.strip().str.lower()
    if "cedula" not in c.columns or "fecha_cargue" not in c.columns:
        return None
    c["_id"] = c["cedula"].map(_nid)
    c["_f"] = pd.to_datetime(c["fecha_cargue"], errors="coerce")
    if "estado_hr" in c.columns:
        est = c["estado_hr"].astype(str).str.strip().str.lower()
        c = c[est.isin(ESTADOS_HR_CARGUE) | (est == "")]
    return c.dropna(subset=["_f"]).groupby("_id")["_f"].max().to_dict()


# ---------- tablero ----------
def render_tablero(base, hist, cargues, on_refresh=None):
    st.subheader("🚚 Gestión Aliados Programación")

    if base is None or len(base) == 0:
        st.warning("Carga la base primero.")
        return

    base = base.copy()
    base["identificacion"] = base["identificacion"].map(_nid)
    base = base.drop_duplicates("identificacion")
    col_ciudad = "zona" if "zona" in base.columns else "municipio"
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
    f3.markdown("<div style='height:1.8rem'></div>", unsafe_allow_html=True)
    if on_refresh is not None and f3.button("🔄 Actualizar", use_container_width=True):
        on_refresh()
        st.rerun()

    if ciudad != "Todas":
        k_sel = next(k for k, v in etiquetas.items() if v == ciudad)
        base = base[base["_k"] == k_sel]

    ahora = pd.Timestamp.now(tz="America/Bogota").tz_localize(None)

    # historial filtrado por ciudad y periodo
    h = hist.copy() if hist is not None else pd.DataFrame()
    if not h.empty:
        h["identificacion"] = h["identificacion"].map(_nid)
        h = h[h["identificacion"].isin(base["identificacion"])]
        hoy = ahora.normalize()
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
    fut = pd.to_datetime(prox, errors="coerce") > ahora
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
    interesados = int(_es_interesado(ult).sum())
    rechazados = int((ult["estado"].fillna("") == "Aliado Rechaza la oferta").sum())

    st.markdown("##### Embudo de gestión")
    c = st.columns(5)
    _card(c[0], "📞 Llamados", llamados, f"{_pct(llamados, len(base))}% de la base")
    _card(c[1], "📵 No responden", no_resp, f"{_pct(no_resp, llamados)}% de los llamados")
    _card(c[2], "✅ Gestionados", gestionados, f"{_pct(gestionados, llamados)}% de los llamados")
    _card(c[3], "🚗 Interesados", interesados, f"{_pct(interesados, llamados)}% de los llamados")
    _card(c[4], "❌ Rechazados", rechazados, f"{_pct(rechazados, llamados)}% de los llamados")

    # --- 3. por analista ---
    st.markdown("##### Gestión por analista")
    ult_cargue = _ultimo_cargue(cargues)
    if ult_cargue is None:
        st.caption("⚠️ CARGUES_REALES no trae las columnas cedula y fecha_cargue; 'Cargaron' aparece en 0.")
        ult_cargue = {}
    filas = []
    for analista, x in h.groupby("analista"):
        inter = x[_es_interesado(x)].sort_values("fecha").drop_duplicates("identificacion")
        cargaron = sum(
            1
            for i, f in zip(inter["identificacion"], inter["fecha"])
            if i in ult_cargue and ult_cargue[i] >= f.normalize() + pd.Timedelta(days=1)
        )
        filas.append({
            "Analista": analista,
            "Llamados": len(x),
            "Contactos": int((x["resultado"] == "Sí contestó").sum()),
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
    tabla = []
    for e in ESTADOS_FINALES:
        n = int((contactados["estado"].fillna("").str.strip() == e).sum())
        tabla.append({"Estado final": e, "N°": n, "%": _pct(n, n_cont)})
    st.dataframe(
        pd.DataFrame(tabla),
        hide_index=True,
        use_container_width=True,
        column_config={"%": st.column_config.ProgressColumn("%", min_value=0, max_value=100, format="%.1f%%")},
    )
    st.caption(f"Total contactados: {n_cont:,}")
