# =============================================================
# implementacion.py
# Módulo de Implementación — Aliados del 2do al 7mo cargue
# Se importa en app.py con: from implementacion import render_implementacion
# =============================================================

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import plotly.express as px
import time
from zoneinfo import ZoneInfo

TZ_COL = ZoneInfo("America/Bogota")

def now_col():
    return datetime.now(TZ_COL).replace(tzinfo=None)

# ─────────────────────────────────────────────────────────────
# CONSTANTES
# ─────────────────────────────────────────────────────────────

CARGUES_MIN_IMPL  = 2   # Implementación recibe desde el 2do cargue
CARGUES_META_IMPL = 7   # Meta: llegar al 7mo cargue

RESULTADOS_IMPL = [
    "Apagado",
    "Fuera de servicio",
    "No contestó",
    "Número errado",
    "Sí contestó",
]

ESTADOS_IMPL = [
    "Comprometido a cargar",
    "Interesado pero sin fecha",
    "Necesita seguimiento",
    "Abandona — tarifa",
    "Abandona — zona",
    "Abandona — vehículo averiado",
    "Abandona — trabaja fijo",
    "Abandona — no le interesa",
    "Llegó al 7mo cargue",
]

RAZONES_IMPL = [
    "Tarifa baja",
    "Zona no le conviene",
    "Vehículo averiado",
    "Trabaja fijo",
    "No disponibilidad de tiempo",
    "Prefiere otra operación",
    "No responde repetidamente",
    "Cargó hoy / sigue activo",
]

ESTADOS_PIPELINE_IMPL = [
    "Activo",
    "En seguimiento",
    "En riesgo de abandono",
    "Abandonó",
    "Completó 7 cargues",
]

NO_RESPONDEN_IMPL = ["Apagado", "Fuera de servicio", "No contestó", "Número errado"]

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def _safe(val):
    if val is None: return ""
    try:
        if pd.isna(val): return ""
    except (TypeError, ValueError): pass
    if hasattr(val, "strftime"):
        try: return val.strftime("%Y-%m-%d %H:%M:%S")
        except: return ""
    try: return str(val)
    except: return ""

def _norm_vh(v):
    v = str(v).lower()
    if any(k in v for k in ["carry","largenvan","large van","small van","van"]): return "Carry / Van"
    if "moto" in v: return "Moto"
    if any(k in v for k in ["camion","camión","truck","npr"]): return "Camión"
    return str(v).title()

def _prio_impl(cargues: int) -> str:
    """Prioridad basada en qué tan cerca está del abandono vs la meta."""
    try: cargues = int(cargues)
    except: return "🟢 BAJA"
    if cargues <= 2: return "🔴 ALTA"   # Recién llegado, mayor riesgo de abandono
    if cargues <= 4: return "🟡 MEDIA"
    return "🟢 BAJA"                    # Ya cerca de la meta, menor riesgo

def calcular_proxima_impl(resultado, estado, intentos):
    hoy    = now_col()
    estado = str(estado or "")
    if "Abandona" in estado:
        return "NO_VOLVER"
    if resultado in NO_RESPONDEN_IMPL:
        if intentos >= 10: return hoy + timedelta(days=30)
        return hoy + timedelta(days=3)
    if estado == "Comprometido a cargar":
        return hoy + timedelta(days=2)
    if estado == "Interesado pero sin fecha":
        return hoy + timedelta(days=3)
    return hoy + timedelta(days=4)

# ─────────────────────────────────────────────────────────────
# LECTURA / ESCRITURA GOOGLE SHEETS
# (usa leer_hoja / agregar_filas / reemplazar_hoja de app.py)
# ─────────────────────────────────────────────────────────────

def _get_impl(force=False):
    """Carga BASE_IMPLEMENTACION con TTL 30s."""
    from app_gestion_aliados import leer_hoja
    ahora  = time.time()
    ultima = st.session_state.get("impl_last_load", 0)
    if force or "impl_df" not in st.session_state or (ahora - ultima) > 30:
        df = leer_hoja("BASE_IMPLEMENTACION")
        if df.empty:
            st.session_state["impl_df"] = None
        else:
            df.columns = df.columns.str.strip().str.lower()
            if "vehiculo" in df.columns:
                df["vehiculo_norm"] = df["vehiculo"].apply(_norm_vh)
            else:
                df["vehiculo_norm"] = "Sin vehículo"
            df["total_cargues"] = pd.to_numeric(
                df.get("total_cargues", 0), errors="coerce"
            ).fillna(0).astype(int)
            df["intentos_impl"] = pd.to_numeric(
                df.get("intentos_impl", 0), errors="coerce"
            ).fillna(0).astype(int)
            df["cargues_faltantes"] = (CARGUES_META_IMPL - df["total_cargues"]).clip(lower=0)
            df["prioridad_impl"]    = df["total_cargues"].apply(_prio_impl)
            if "zona" not in df.columns:
                df["zona"] = "Sin zona"
            st.session_state["impl_df"]        = df
            st.session_state["impl_last_load"] = ahora
    return st.session_state.get("impl_df")

def _invalidar_impl():
    st.session_state["impl_stale"] = True
    if "impl_df" in st.session_state:
        del st.session_state["impl_df"]

def _get_hist_impl(force=False):
    """Carga HIST_IMPLEMENTACION con TTL 30s."""
    from app_gestion_aliados import leer_hoja
    ahora  = time.time()
    ultima = st.session_state.get("hist_impl_last", 0)
    if force or "hist_impl_df" not in st.session_state or (ahora - ultima) > 30:
        cols = ["fecha","analista","identificacion","resultado","estado","razon","obs","total_cargues_momento"]
        df   = leer_hoja("HIST_IMPLEMENTACION", cols)
        if df.empty:
            df = pd.DataFrame(columns=cols)
        else:
            df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
            df = df.dropna(subset=["fecha"])
        st.session_state["hist_impl_df"]   = df
        st.session_state["hist_impl_last"] = ahora
    return st.session_state["hist_impl_df"]

def _agregar_hist_impl_local(row: dict):
    nuevo = pd.DataFrame([{
        "fecha":                   pd.to_datetime(row.get("fecha")),
        "analista":                _safe(row.get("analista")),
        "identificacion":          _safe(row.get("identificacion")),
        "resultado":               _safe(row.get("resultado")),
        "estado":                  _safe(row.get("estado")),
        "razon":                   _safe(row.get("razon")),
        "obs":                     _safe(row.get("obs")),
        "total_cargues_momento":   _safe(row.get("total_cargues_momento")),
    }])
    if "hist_impl_df" in st.session_state and isinstance(st.session_state["hist_impl_df"], pd.DataFrame):
        st.session_state["hist_impl_df"] = pd.concat(
            [st.session_state["hist_impl_df"], nuevo], ignore_index=True
        )
    else:
        st.session_state["hist_impl_df"] = nuevo
    st.session_state["hist_impl_last"] = time.time()

def _registrar_gestionado_hoy(identificacion: str, modulo: str):
    """Registra en GESTIONADOS_HOY para evitar duplicados entre módulos."""
    from app_gestion_aliados import agregar_filas
    try:
        fila = [_safe(now_col().date()), str(identificacion), modulo]
        agregar_filas("GESTIONADOS_HOY", [fila])
    except Exception as e:
        pass  # No interrumpir el flujo principal por esto

def _get_gestionados_hoy_todos() -> set:
    """
    Lee GESTIONADOS_HOY + HISTORICO para saber qué aliados
    ya fueron gestionados hoy en CUALQUIER módulo.
    Devuelve un set de identificaciones.
    """
    from app_gestion_aliados import leer_hoja
    gestionados = set()
    try:
        # Desde GESTIONADOS_HOY (impl + programación)
        df_g = leer_hoja("GESTIONADOS_HOY")
        if not df_g.empty and "fecha" in df_g.columns:
            df_g.columns = df_g.columns.str.lower()
            hoy = str(now_col().date())
            hoy_g = df_g[df_g["fecha"].astype(str).str.startswith(hoy)]
            gestionados.update(hoy_g["identificacion"].astype(str).tolist())
    except Exception:
        pass
    try:
        # Desde HISTORICO (base de llamadas principal)
        hist = leer_hoja("HISTORICO")
        if not hist.empty and "fecha" in hist.columns:
            hist.columns = hist.columns.str.lower()
            hist["fecha"] = pd.to_datetime(hist["fecha"], errors="coerce")
            hist = hist.dropna(subset=["fecha"])
            hoy_h = hist[hist["fecha"].dt.date == now_col().date()]
            gestionados.update(hoy_h["identificacion"].astype(str).tolist())
    except Exception:
        pass
    return gestionados

def guardar_gestion_impl(row: dict):
    """Guarda en HIST_IMPLEMENTACION y actualiza BASE_IMPLEMENTACION."""
    from app_gestion_aliados import agregar_filas
    fila = [
        _safe(now_col()),
        _safe(row.get("analista")),
        _safe(row.get("identificacion")),
        _safe(row.get("resultado")),
        _safe(row.get("estado")),
        _safe(row.get("razon")),
        _safe(row.get("obs")),
        _safe(row.get("total_cargues_momento")),
    ]
    agregar_filas("HIST_IMPLEMENTACION", [fila])
    _agregar_hist_impl_local(row)
    _registrar_gestionado_hoy(row.get("identificacion",""), "IMPLEMENTACION")
    _actualizar_crm_impl(
        row.get("identificacion"),
        row.get("resultado"),
        row.get("estado"),
        row.get("razon"),
        row.get("total_cargues_momento", 0),
    )

def _actualizar_crm_impl(identificacion, resultado, estado, razon, total_cargues):
    """Actualiza fila de BASE_IMPLEMENTACION con resultado de gestión."""
    import gspread
    try:
        from app_gestion_aliados import conectar_sheets
        sh = conectar_sheets()
        if sh is None: return
        ws      = sh.worksheet("BASE_IMPLEMENTACION")
        headers = ws.row_values(1)
        if "identificacion" not in headers: return
        col_id  = headers.index("identificacion") + 1
        ids     = ws.col_values(col_id)
        if str(identificacion) not in ids: return
        fila    = ids.index(str(identificacion)) + 1

        intentos_n = 1
        if "intentos_impl" in headers:
            col_int = headers.index("intentos_impl") + 1
            val_int = ws.cell(fila, col_int).value
            try: intentos_n = int(str(val_int or "0")) + 1
            except: intentos_n = 1

        proxima = calcular_proxima_impl(resultado, estado, intentos_n)
        proxima_str = _safe(proxima) if not isinstance(proxima, str) else proxima

        # Si llegó al 7mo cargue → marcar como completado
        estado_pipeline = "Completó 7 cargues" if (
            str(estado) == "Llegó al 7mo cargue" or
            int(total_cargues or 0) >= CARGUES_META_IMPL
        ) else str(estado or "")

        updates = {}
        for col_name, val in [
            ("ultimo_resultado_impl", _safe(resultado)),
            ("ultimo_estado_impl",    _safe(estado)),
            ("ultima_razon_impl",     _safe(razon)),
            ("proxima_gestion_impl",  proxima_str),
            ("intentos_impl",         str(intentos_n)),
            ("estado_impl",           estado_pipeline),
        ]:
            if col_name in headers:
                col_idx = headers.index(col_name) + 1
                celda   = gspread.utils.rowcol_to_a1(fila, col_idx)
                updates[celda] = val

        if updates:
            ws.batch_update([{"range": k, "values": [[v]]} for k,v in updates.items()])
        _invalidar_impl()
    except Exception as e:
        st.warning(f"CRM Implementación no actualizado: {e}")

def cargar_base_implementacion(df_nuevo: pd.DataFrame):
    """
    Carga incremental para BASE_IMPLEMENTACION.
    Agrega nuevos aliados, actualiza los existentes.
    Detecta aliados que ya llegaron a 7 cargues.
    """
    from app_gestion_aliados import leer_hoja, reemplazar_hoja

    df_nuevo = df_nuevo.copy()
    df_nuevo.columns = (
        df_nuevo.columns.str.strip().str.lower()
        .str.replace(r"\s+", "_", regex=True)
    )
    df_nuevo = df_nuevo[[c for c in df_nuevo.columns
                          if c and not c.startswith("unnamed")]]
    df_nuevo = df_nuevo.loc[:, ~df_nuevo.columns.duplicated()]

    ALIAS_ID = ["identificacion","id_aliado","cedula","id","documento"]
    col_id   = next((a for a in ALIAS_ID if a in df_nuevo.columns), None)
    if not col_id:
        st.error(f"No se encontró columna de ID. Columnas: {list(df_nuevo.columns)}")
        return 0, 0, 0

    df_nuevo = df_nuevo.rename(columns={col_id: "identificacion"})
    df_nuevo["identificacion"] = df_nuevo["identificacion"].astype(str).str.strip()

    base = leer_hoja("BASE_IMPLEMENTACION")

    # Primera carga
    if base.empty:
        for col in ["estado_impl","analista_impl","intentos_impl",
                    "proxima_gestion_impl","ultimo_resultado_impl",
                    "ultimo_estado_impl","ultima_razon_impl","fecha_ingreso_impl"]:
            if col not in df_nuevo.columns:
                df_nuevo[col] = "0" if col == "intentos_impl" else ""
        df_nuevo["fecha_ingreso_impl"] = _safe(now_col())
        reemplazar_hoja("BASE_IMPLEMENTACION", df_nuevo)
        _invalidar_impl()
        return len(df_nuevo), 0, 0

    base.columns = (
        base.columns.str.strip().str.lower()
        .str.replace(r"\s+", "_", regex=True)
    )
    base["identificacion"] = base["identificacion"].astype(str).str.strip()
    if "total_cargues" not in base.columns: base["total_cargues"] = 0
    base["total_cargues"] = pd.to_numeric(base["total_cargues"], errors="coerce").fillna(0).astype(int)

    ids_existentes = set(base["identificacion"].unique())
    CRM_COLS       = ["estado_impl","analista_impl","intentos_impl","proxima_gestion_impl",
                      "ultimo_resultado_impl","ultimo_estado_impl","ultima_razon_impl","fecha_ingreso_impl"]

    nuevos    = df_nuevo[~df_nuevo["identificacion"].isin(ids_existentes)].copy()
    for col in CRM_COLS:
        if col not in nuevos.columns:
            nuevos[col] = "0" if col == "intentos_impl" else ""
    nuevos["fecha_ingreso_impl"] = _safe(now_col())

    # Actualizar existentes
    cols_op  = [c for c in df_nuevo.columns if c not in CRM_COLS and c != "identificacion"]
    exist_df = (
        df_nuevo[df_nuevo["identificacion"].isin(ids_existentes)][["identificacion"]+cols_op]
        .loc[:, ~pd.Index(["identificacion"]+cols_op).duplicated()]
        .set_index("identificacion")
    )
    base_idx = base.set_index("identificacion")
    for col in cols_op:
        if col not in exist_df.columns: continue
        col_data = exist_df[[col]]
        if col in base_idx.columns: base_idx.update(col_data)
        else: base_idx = base_idx.join(col_data, how="left")

    # Detectar cuántos completaron los 7 cargues ahora
    completados_ahora = 0
    if "total_cargues" in base_idx.columns:
        tc_nuevo = pd.to_numeric(base_idx["total_cargues"], errors="coerce").fillna(0).astype(int)
        completados_ahora = (tc_nuevo >= CARGUES_META_IMPL).sum()

    base_act  = base_idx.reset_index()
    base_final = pd.concat([base_act, nuevos], ignore_index=True).fillna("")
    base_final = base_final.loc[:, ~base_final.columns.duplicated()]

    reemplazar_hoja("BASE_IMPLEMENTACION", base_final)
    _invalidar_impl()
    return len(nuevos), len(exist_df), int(completados_ahora)

# ─────────────────────────────────────────────────────────────
# UI — COORDINADOR: DASHBOARD IMPLEMENTACIÓN
# ─────────────────────────────────────────────────────────────

def render_dashboard_implementacion():
    """
    Pestañas de Implementación para el Coordinador.
    Llamar con: render_dashboard_implementacion()
    """
    df   = _get_impl()
    hist = _get_hist_impl()

    tab_res, tab_kpi, tab_carga = st.tabs([
        "📊 Resumen Implementación",
        "📈 KPIs y análisis",
        "📤 Cargar base",
    ])

    # ── RESUMEN ──────────────────────────────────────────────
    with tab_res:
        st.subheader("Resumen — Implementación")
        if st.button("🔄 Actualizar", key="impl_ref_coord"):
            _get_impl(force=True)
            _get_hist_impl(force=True)
            st.rerun()

        if df is None:
            st.warning("Carga la base de Implementación primero.")
            return

        activos      = df[~df.get("estado_impl","").astype(str).str.contains("Abandona|Completó", na=False)]
        completados  = df[df.get("estado_impl","").astype(str).str.contains("Completó", na=False)]
        abandonaron  = df[df.get("estado_impl","").astype(str).str.contains("Abandona", na=False)]

        c1,c2,c3,c4 = st.columns(4)
        c1.metric("Total base", len(df))
        c2.metric("Activos en seguimiento", len(activos))
        c3.metric("Completaron 7 cargues", len(completados),
                  delta=f"{round(len(completados)/max(len(df),1)*100,1)}%")
        c4.metric("Abandonaron", len(abandonaron),
                  delta=f"-{round(len(abandonaron)/max(len(df),1)*100,1)}%")

        st.markdown("---")
        st.markdown("#### Distribución por cargues actuales")
        hist_c = df["total_cargues"].value_counts().sort_index().reset_index()
        hist_c.columns = ["Cargues", "Aliados"]
        st.plotly_chart(
            px.bar(hist_c, x="Cargues", y="Aliados",
                   title="¿En qué cargue están los aliados de Implementación?",
                   color_discrete_sequence=["#534AB7"]),
            use_container_width=True
        )

        # Alta prioridad (riesgo de abandono)
        alta = df[df["prioridad_impl"] == "🔴 ALTA"].copy()
        if not alta.empty:
            st.markdown(f"---\n#### 🔴 Alta prioridad — {len(alta)} aliados en 2do cargue (mayor riesgo)")
            cols_a = [c for c in ["identificacion","nombre","celular","zona","vehiculo_norm",
                                   "total_cargues","cargues_faltantes","analista_impl","estado_impl"]
                      if c in alta.columns]
            st.dataframe(alta[cols_a], use_container_width=True, hide_index=True)

        # Listos para pasar a Programación
        listos = df[df["total_cargues"] >= CARGUES_META_IMPL].copy()
        if not listos.empty:
            st.markdown(f"---\n#### ✅ {len(listos)} aliados lograron 7 cargues — listos para Programación")
            cols_l = [c for c in ["identificacion","nombre","celular","zona","vehiculo_norm","total_cargues"]
                      if c in listos.columns]
            st.dataframe(listos[cols_l], use_container_width=True, hide_index=True)
            st.download_button("📥 Descargar listos para Programación",
                               listos.to_csv(index=False).encode("utf-8"),
                               "listos_programacion.csv", "text/csv")

    # ── KPIs ─────────────────────────────────────────────────
    with tab_kpi:
        st.subheader("KPIs Implementación")
        if df is None:
            st.warning("Carga la base primero.")
            return

        total         = len(df)
        conv_7        = len(df[df["total_cargues"] >= CARGUES_META_IMPL])
        pct_conv      = round(conv_7 / max(total,1) * 100, 1)
        pct_abnd      = round(len(df[df.get("estado_impl","").astype(str).str.contains("Abandona", na=False)])
                              / max(total,1) * 100, 1)

        c1,c2,c3 = st.columns(3)
        c1.metric("% conversión a 7 cargues", f"{pct_conv}%")
        c2.metric("% abandono", f"{pct_abnd}%")
        c3.metric("Cargue promedio base activa",
                  f"{df['total_cargues'].mean():.1f}" if total else "N/A")

        st.markdown("---")
        if "zona" in df.columns:
            st.markdown("#### Conversión por zona")
            zona_g = df.groupby("zona").agg(
                total=("identificacion","count"),
                completaron=("total_cargues", lambda x: (x >= CARGUES_META_IMPL).sum())
            ).reset_index()
            zona_g["% conv"] = (zona_g["completaron"]/zona_g["total"]*100).round(1)
            st.dataframe(zona_g.sort_values("% conv", ascending=False),
                         use_container_width=True, hide_index=True)

        if "vehiculo_norm" in df.columns:
            st.markdown("#### Conversión por tipo de vehículo")
            veh_g = df.groupby("vehiculo_norm").agg(
                total=("identificacion","count"),
                completaron=("total_cargues", lambda x: (x >= CARGUES_META_IMPL).sum())
            ).reset_index()
            veh_g["% conv"] = (veh_g["completaron"]/veh_g["total"]*100).round(1)
            st.dataframe(veh_g.sort_values("% conv", ascending=False),
                         use_container_width=True, hide_index=True)

        if not hist.empty:
            st.markdown("---")
            st.markdown("#### Razones de abandono")
            abd = hist[hist["estado"].astype(str).str.contains("Abandona", na=False)]
            if not abd.empty:
                rz = abd["razon"].value_counts().reset_index()
                rz.columns = ["Razón","Cantidad"]
                st.dataframe(rz, use_container_width=True, hide_index=True)

    # ── CARGA ─────────────────────────────────────────────────
    with tab_carga:
        st.subheader("📤 Cargar base de Implementación")
        st.info("""
**Columnas esperadas en el Excel:**
`identificacion`, `nombre`, `celular`, `vehiculo`, `zona`, `total_cargues`, `fecha_ultimo_cargue`

Los campos CRM (estado, intentos, próxima gestión) se agregan automáticamente.
Aliados que ya estén en la base se actualizan sin perder su historial CRM.
        """)
        archivo = st.file_uploader("Excel (.xlsx)", type=["xlsx"], key="uploader_impl_coord")
        if archivo:
            try:
                df_s = pd.read_excel(archivo, engine="openpyxl")
                df_s = df_s[[c for c in df_s.columns if not str(c).startswith("Unnamed")]]
                df_s = df_s.fillna("")
                st.success(f"{len(df_s):,} registros leídos")
                st.dataframe(df_s.head(5), use_container_width=True)
                if st.button("🚀 Cargar a Implementación"):
                    with st.spinner("Procesando..."):
                        nn, na, nc = cargar_base_implementacion(df_s)
                    st.success(f"✅ {nn} nuevos · {na} actualizados · {nc} ya completaron 7 cargues")
            except Exception as e:
                st.error(f"Error: {e}")

        st.markdown("---")
        st.markdown("#### Asignación de analistas")
        df_actual = _get_impl()
        if df_actual is not None and "analista_impl" in df_actual.columns:
            sin_asignar = df_actual[
                df_actual["analista_impl"].fillna("").str.strip() == ""
            ]
            if not sin_asignar.empty:
                st.warning(f"{len(sin_asignar)} aliados sin analista asignado en Implementación.")

# ─────────────────────────────────────────────────────────────
# UI — ANALISTA: CRM IMPLEMENTACIÓN
# ─────────────────────────────────────────────────────────────

def render_crm_analista_impl(nombre_analista: str):
    """
    Módulo CRM completo para el analista de Implementación.
    Llamar con: render_crm_analista_impl(nombre)
    """
    df   = _get_impl()
    hist = _get_hist_impl()

    tab_mis, tab_buscar, tab_hoy = st.tabs([
        "📞 Mis aliados — Implementación",
        "🔍 Buscar aliado",
        "📊 Mi resumen de hoy",
    ])

    # ── MIS ALIADOS ──────────────────────────────────────────
    with tab_mis:
        if df is None:
            st.warning("La base de Implementación no está cargada todavía.")
            return

        # Aliados asignados a este analista
        if "analista_impl" in df.columns:
            mis = df[df["analista_impl"].astype(str) == nombre_analista].copy()
        else:
            mis = df.copy()

        # Filtrar los que NO han completado los 7 cargues ni abandonaron
        mis = mis[~mis.get("estado_impl","").astype(str).str.contains("Completó|Abandona", na=False)]
        mis = mis[mis.get("proxima_gestion_impl","").astype(str).str.upper() != "NO_VOLVER"]

        # Filtrar por proxima_gestion_impl
        def disponible(v):
            v = str(v).strip()
            if v in ("","nan","None","0","NO_VOLVER"): return True
            f = pd.to_datetime(v, errors="coerce")
            return pd.isna(f) or f <= now_col()
        if "proxima_gestion_impl" in mis.columns:
            mis = mis[mis["proxima_gestion_impl"].apply(disponible)]

        # Excluir gestionados hoy en CUALQUIER módulo
        gestionados_hoy = _get_gestionados_hoy_todos()
        ya_hoy_impl_local = set()
        if not hist.empty:
            h_hoy = hist.copy()
            h_hoy["fecha"] = pd.to_datetime(h_hoy["fecha"], errors="coerce")
            h_hoy = h_hoy.dropna(subset=["fecha"])
            ya_hoy_impl_local = set(
                h_hoy[h_hoy["fecha"].dt.date == now_col().date()]["identificacion"].astype(str).tolist()
            )

        # Los que ya fueron gestionados hoy (cualquier módulo)
        todos_gestionados_hoy = gestionados_hoy | ya_hoy_impl_local

        # Separar: gestionados hoy (mostrar aviso) vs pendientes
        mis["_ya_hoy"] = mis["identificacion"].astype(str).isin(todos_gestionados_hoy)
        pendientes      = mis[~mis["_ya_hoy"]].copy()
        ya_gestionados  = mis[mis["_ya_hoy"]].copy()

        # Ordenar por prioridad
        orden = {"🔴 ALTA":0,"🟡 MEDIA":1,"🟢 BAJA":2}
        pendientes["_ord"] = pendientes["prioridad_impl"].map(orden).fillna(3)
        pendientes = pendientes.sort_values("_ord").drop(columns=["_ord","_ya_hoy"]).reset_index(drop=True)

        # Progreso
        hechas = st.session_state.get("impl_hechas", 0)
        pend_n = len(pendientes)
        pct    = int(hechas / max(hechas + pend_n, 1) * 100)
        st.progress(pct, text=f"Progreso: {hechas} gestionados · {pend_n} pendientes")

        # ── AVISO DE DUPLICADOS ──────────────────────────────
        if not ya_gestionados.empty:
            with st.expander(f"⚠️ {len(ya_gestionados)} aliados ya gestionados hoy en otro módulo — puedes gestionarlos igual"):
                cols_dup = [c for c in ["identificacion","nombre","celular","total_cargues","prioridad_impl"]
                            if c in ya_gestionados.columns]
                st.dataframe(ya_gestionados[cols_dup], use_container_width=True, hide_index=True)
                st.caption("Estos aliados aparecen en la base de llamadas principal o ya fueron llamados hoy en Implementación.")

        # Tabla de pendientes
        cols_v = [c for c in ["identificacion","nombre","celular","zona","vehiculo_norm",
                               "total_cargues","cargues_faltantes","prioridad_impl","estado_impl"]
                  if c in pendientes.columns]
        st.markdown(f"#### Pendientes ({pend_n})")
        st.dataframe(pendientes[cols_v], use_container_width=True, hide_index=True)

        # ── FORMULARIO DE GESTIÓN ────────────────────────────
        st.markdown("---")
        st.markdown("#### 📞 Registrar gestión")

        # Incluir TODOS (pendientes + ya gestionados) en el select por si quiere registrar
        todos_ids = pendientes["identificacion"].astype(str).tolist()
        if not ya_gestionados.empty:
            todos_ids += ya_gestionados["identificacion"].astype(str).tolist()

        if not todos_ids:
            st.info("✅ Sin aliados pendientes. Todos están al día.")
        else:
            with st.form("form_impl", clear_on_submit=True):
                c1, c2 = st.columns(2)
                with c1:
                    ali = st.selectbox("Cédula del aliado", todos_ids)
                    res = st.selectbox("Resultado de la llamada", RESULTADOS_IMPL)
                with c2:
                    est = st.selectbox("Estado", ["-"] + ESTADOS_IMPL)
                    raz = st.selectbox("Razón", ["-"] + RAZONES_IMPL)

                # Ficha del aliado
                fd = mis[mis["identificacion"].astype(str) == str(ali)]
                if fd.empty:
                    fd = df[df["identificacion"].astype(str) == str(ali)]

                if not fd.empty:
                    f = fd.iloc[0]
                    st.markdown("**Ficha del aliado**")
                    ficha_cols = [c for c in ["nombre","celular","total_cargues",
                                               "cargues_faltantes","prioridad_impl",
                                               "fecha_ultimo_cargue","estado_impl"] if c in f.index]
                    cols_f = st.columns(min(len(ficha_cols), 4))
                    for i, col_n in enumerate(ficha_cols):
                        cols_f[i % 4].metric(
                            col_n.replace("_"," ").title(),
                            str(f[col_n])
                        )

                    # Aviso si ya fue gestionado hoy
                    if str(ali) in todos_gestionados_hoy:
                        st.warning("⚠️ Este aliado ya fue gestionado hoy en otro módulo. Puedes registrar igual si lo estás contactando nuevamente.")

                obs = st.text_area("Observaciones")
                sub = st.form_submit_button("💾 GUARDAR GESTIÓN")

            if sub:
                er = None if est == "-" else est
                rr = None if raz == "-" else raz
                if res == "Sí contestó" and er is None:
                    st.error("Selecciona el estado del aliado.")
                else:
                    tc_m = int(fd.iloc[0].get("total_cargues", 0)) if not fd.empty else 0
                    guardar_gestion_impl({
                        "analista":              nombre_analista,
                        "identificacion":        ali,
                        "resultado":             res,
                        "estado":                er,
                        "razon":                 rr,
                        "obs":                   obs,
                        "total_cargues_momento": tc_m,
                    })
                    st.session_state["impl_hechas"] = st.session_state.get("impl_hechas", 0) + 1
                    if str(er) == "Llegó al 7mo cargue" or tc_m >= CARGUES_META_IMPL:
                        st.success(f"🏆 ¡{ali} completó los 7 cargues! Pasa a Programación.")
                    else:
                        st.success(f"✅ Guardado para {ali}.")
                    st.rerun()

    # ── BUSCAR ───────────────────────────────────────────────
    with tab_buscar:
        st.subheader("🔍 Buscar aliado")
        cedula_b = st.text_input("Cédula", "", key="impl_buscar_cc")
        if cedula_b.strip() and df is not None:
            fila = df[df["identificacion"].astype(str) == cedula_b.strip()]
            if fila.empty:
                st.warning(f"No se encontró {cedula_b} en la base de Implementación.")
            else:
                f = fila.iloc[0]
                st.success("✅ Aliado encontrado")
                cols_info = [c for c in ["identificacion","nombre","celular","zona","vehiculo",
                                          "total_cargues","cargues_faltantes","fecha_ultimo_cargue",
                                          "estado_impl","intentos_impl","proxima_gestion_impl",
                                          "analista_impl"] if c in f.index]
                c1, c2 = st.columns(2)
                mid = len(cols_info) // 2
                with c1:
                    for col in cols_info[:mid]:
                        st.metric(col.replace("_"," ").title(), str(f[col]))
                with c2:
                    for col in cols_info[mid:]:
                        st.metric(col.replace("_"," ").title(), str(f[col]))

                # Historial de este aliado
                st.markdown("---")
                if not hist.empty:
                    h_ali = hist[hist["identificacion"].astype(str) == cedula_b.strip()].copy()
                    if h_ali.empty:
                        st.info("Sin gestiones en Implementación para este aliado.")
                    else:
                        h_ali["Hora"] = h_ali["fecha"].dt.strftime("%d/%m/%Y %I:%M %p")
                        st.dataframe(
                            h_ali[["Hora","analista","resultado","estado","razon","obs","total_cargues_momento"]].rename(
                                columns={"analista":"Analista","resultado":"Resultado",
                                         "estado":"Estado","razon":"Razón","obs":"Obs",
                                         "total_cargues_momento":"Cargues en ese momento"}
                            ), use_container_width=True, hide_index=True
                        )

                # También verificar si aparece en base de llamadas principal
                try:
                    from app_gestion_aliados import leer_hoja
                    hist_prin = leer_hoja("HISTORICO")
                    if not hist_prin.empty:
                        hist_prin.columns = hist_prin.columns.str.lower()
                        h_prin = hist_prin[hist_prin["identificacion"].astype(str) == cedula_b.strip()]
                        if not h_prin.empty:
                            st.warning(f"⚠️ Este aliado también tiene {len(h_prin)} gestión(es) en la base de llamadas principal.")
                except Exception:
                    pass

    # ── RESUMEN HOY ──────────────────────────────────────────
    with tab_hoy:
        st.subheader(f"Mi resumen de hoy — {now_col().strftime('%d/%m/%Y')}")
        if st.button("🔄 Actualizar", key="impl_ref_analista"):
            _get_hist_impl(force=True)
            st.rerun()
        if hist.empty:
            st.info("Sin gestiones registradas aún.")
        else:
            mh = hist[
                (hist["analista"] == nombre_analista) &
                (hist["fecha"].dt.date == now_col().date())
            ].copy()
            if mh.empty:
                st.info("Sin gestiones hoy. ¡Empieza en Mis Aliados!")
            else:
                t  = len(mh)
                sc = len(mh[mh["resultado"] == "Sí contestó"])
                nr = len(mh[mh["resultado"].isin(NO_RESPONDEN_IMPL)])
                c7 = len(mh[mh["estado"].astype(str).str.contains("7mo cargue", na=False)])
                c1,c2,c3,c4 = st.columns(4)
                c1.metric("📞 Llamadas",   t)
                c2.metric("✅ Contactados", sc)
                c3.metric("📵 No resp.",   nr)
                c4.metric("🏆 7mo cargue", c7)
                st.markdown("---")
                mh["Hora"] = mh["fecha"].dt.strftime("%I:%M %p")
                st.dataframe(
                    mh[["Hora","identificacion","resultado","estado","razon","obs"]].rename(
                        columns={"identificacion":"Cédula","resultado":"Resultado",
                                 "estado":"Estado","razon":"Razón","obs":"Obs"}
                    ), use_container_width=True, hide_index=True
                )
                st.download_button(
                    "📥 Descargar hoy",
                    mh.to_csv(index=False).encode("utf-8"),
                    f"impl_hoy_{now_col().date()}.csv", "text/csv"
                )


# ─────────────────────────────────────────────────────────────
# PUNTO DE ENTRADA PRINCIPAL
# (llamado desde app.py según el perfil)
# ─────────────────────────────────────────────────────────────

PASS_IMPL_COORD    = "impl_coord"   # Contraseña coordinador en módulo Impl
PASS_IMPL_ANALISTA = "impl2024"     # Contraseña analistas de Implementación

ANALISTAS_IMPL = [
    "Analista Impl 1",
    "Analista Impl 2",
    "Analista Impl 3",
    "Analista Impl 4",
]

def render_implementacion():
    """
    Función principal. Llamar al final de app.py:

        from implementacion import render_implementacion
        if perfil == "Implementación":
            render_implementacion()
    """
    st.markdown("## ⚙️ Módulo Implementación")
    st.caption("Seguimiento de aliados del 2do al 7mo cargue")

    with st.sidebar:
        st.markdown("### ⚙️ Implementación")
        rol_impl = st.selectbox(
            "Rol en Implementación",
            ["— Selecciona —", "Coordinador Impl", "Analista Impl"],
            key="rol_impl_select"
        )

        if rol_impl == "Coordinador Impl":
            pwd_i = st.text_input("Contraseña coordinador", type="password", key="pwd_impl_coord")
            if pwd_i != PASS_IMPL_COORD:
                if pwd_i: st.error("Contraseña incorrecta")
                st.stop()
            st.success("✅ Coordinador Implementación")
            nombre_impl = "Coordinador"

        elif rol_impl == "Analista Impl":
            nombre_impl = st.selectbox("¿Quién eres?", ANALISTAS_IMPL, key="nom_impl_sel")
            pwd_a = st.text_input("Contraseña", type="password", key="pwd_impl_ana")
            if pwd_a != PASS_IMPL_ANALISTA:
                if pwd_a: st.error("Contraseña incorrecta")
                st.stop()
            st.success(f"✅ {nombre_impl.split()[0]}")
        else:
            st.info("Selecciona tu rol para continuar.")
            st.stop()

    if nombre_impl == "Coordinador":
        render_dashboard_implementacion()
    else:
        render_crm_analista_impl(nombre_impl)
