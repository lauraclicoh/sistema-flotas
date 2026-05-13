"""
MÓDULO PIPELINE — Supplay → Implementación → Programación/Fidelización
=======================================================================
Este archivo se AGREGA a app_gestion_aliados.py como bloque nuevo.
Instrucciones de integración al final del archivo.
=======================================================================
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
import uuid
from zoneinfo import ZoneInfo

# ─────────────────────────────────────────────
# CONSTANTES DEL PIPELINE
# ─────────────────────────────────────────────

TZ_COL = ZoneInfo("America/Bogota")

def now_col():
    return datetime.now(TZ_COL).replace(tzinfo=None)

ETAPAS_PIPELINE = [
    "SUPPLAY",
    "IMPLEMENTACION",
    "LISTO_PROGRAMACION",
    "PROGRAMACION",
    "FIDELIZADO",
    "RIESGO",
    "PERDIDO",
]

ESTADOS_PROGRAMACION = [
    "Activo",
    "En riesgo",
    "Reactivado",
    "Perdido",
    "No interesado",
]

RESULTADOS_PROG = [
    "Apagado",
    "Fuera de servicio",
    "No contestó",
    "Número errado",
    "Sí contestó",
]

RAZONES_PROG = [
    "Tarifa",
    "Zona",
    "Horarios",
    "No disponibilidad",
    "Vehículo averiado",
    "No responde",
    "Trabaja fijo",
    "Prefiere otra operación",
]

# Umbral de cargues para pasar de etapa
CARGUES_IMPLEMENTACION = 1   # Supplay entrega desde el 1er cargue
CARGUES_PROGRAMACION   = 7   # Implementación debe llegar a 7
CARGUES_FIDELIZADO     = 15  # Programación define fidelizado en 15+

# Semáforo por días sin cargar
def calcular_semaforo(dias: int) -> str:
    try: dias = int(dias)
    except: return "🟢 Activo"
    if dias <= 2:  return "🟢 Activo"
    if dias <= 5:  return "🟡 Riesgo"
    return "🔴 Perdido"

def calcular_prioridad_prog(dias: int) -> str:
    try: dias = int(dias)
    except: return "🟢 BAJA"
    if dias >= 6:  return "🔴 ALTA"
    if dias >= 3:  return "🟡 MEDIA"
    return "🟢 BAJA"

# ─────────────────────────────────────────────
# GOOGLE SHEETS — HOJAS DEL PIPELINE
# ─────────────────────────────────────────────
# Estas funciones usan conectar_sheets() de la app principal.
# Se asume que conectar_sheets() ya está definida en el archivo base.

def _safe_str_p(val):
    """Conversión segura a string (versión local del módulo)."""
    if val is None: return ""
    try:
        if pd.isna(val): return ""
    except (TypeError, ValueError): pass
    if hasattr(val, "strftime"):
        try: return val.strftime("%Y-%m-%d %H:%M:%S")
        except: return ""
    try: return str(val)
    except: return ""

def leer_pipeline():
    """Lee BASE_PIPELINE desde Google Sheets y la procesa."""
    from app_gestion_aliados import conectar_sheets, leer_hoja  # ajustar import según tu estructura
    if "pipeline_df" not in st.session_state or st.session_state.get("pipeline_stale", True):
        df = leer_hoja("BASE_PIPELINE")
        if df.empty:
            st.session_state["pipeline_df"] = None
        else:
            df.columns = df.columns.str.strip().str.lower()
            # Calcular campos automáticos
            if "fecha_ultimo_cargue" in df.columns:
                df["_fuc"] = pd.to_datetime(df["fecha_ultimo_cargue"], dayfirst=True, errors="coerce")
                df["dias_sin_cargar"] = (now_col() - df["_fuc"]).dt.days.fillna(999).astype(int)
            else:
                df["dias_sin_cargar"] = 999
            if "total_cargues" not in df.columns:
                df["total_cargues"] = 0
            df["total_cargues"] = pd.to_numeric(df["total_cargues"], errors="coerce").fillna(0).astype(int)
            # Semáforo y prioridad automáticos
            df["estado_semaforo"] = df["dias_sin_cargar"].apply(calcular_semaforo)
            df["prioridad"]       = df["dias_sin_cargar"].apply(calcular_prioridad_prog)
            # Etapa pipeline automática según cargues (si no está seteada manualmente)
            if "etapa_pipeline" not in df.columns:
                df["etapa_pipeline"] = df["total_cargues"].apply(_inferir_etapa)
            st.session_state["pipeline_df"] = df
        st.session_state["pipeline_stale"] = False
    return st.session_state.get("pipeline_df")

def _inferir_etapa(cargues: int) -> str:
    try: cargues = int(cargues)
    except: return "SUPPLAY"
    if cargues >= CARGUES_FIDELIZADO: return "FIDELIZADO"
    if cargues > CARGUES_PROGRAMACION: return "PROGRAMACION"
    if cargues == CARGUES_PROGRAMACION: return "LISTO_PROGRAMACION"
    if cargues >= CARGUES_IMPLEMENTACION: return "IMPLEMENTACION"
    return "SUPPLAY"

def invalidar_pipeline():
    st.session_state["pipeline_stale"] = True

def guardar_gestion_prog(row: dict):
    """
    Guarda una gestión en GESTIONES_PROGRAMACION.
    row debe tener: identificacion, analista, resultado_llamada,
                    estado_aliado, razon, observacion, proxima_gestion,
                    total_cargues_momento, dias_sin_cargar_momento
    """
    from app_gestion_aliados import agregar_filas
    gid  = str(uuid.uuid4())[:8]
    fila = [
        gid,
        _safe_str_p(row.get("identificacion")),
        _safe_str_p(row.get("analista")),
        _safe_str_p(now_col()),
        _safe_str_p(row.get("resultado_llamada")),
        _safe_str_p(row.get("estado_aliado")),
        _safe_str_p(row.get("razon")),
        _safe_str_p(row.get("observacion")),
        _safe_str_p(row.get("proxima_gestion")),
        _safe_str_p(row.get("total_cargues_momento")),
        _safe_str_p(row.get("dias_sin_cargar_momento")),
    ]
    agregar_filas("GESTIONES_PROGRAMACION", [fila])
    # Actualizar estado en BASE_PIPELINE si contestó
    if row.get("resultado_llamada") == "Sí contestó" and row.get("estado_aliado"):
        _actualizar_estado_pipeline(
            row["identificacion"],
            row["estado_aliado"],
            row.get("proxima_gestion",""),
        )

def _actualizar_estado_pipeline(identificacion, estado_aliado, proxima_gestion):
    """Actualiza estado_programacion y proxima_gestion en BASE_PIPELINE."""
    from app_gestion_aliados import conectar_sheets
    import gspread
    try:
        sh = conectar_sheets()
        if sh is None: return
        ws = sh.worksheet("BASE_PIPELINE")
        headers = ws.row_values(1)
        if "identificacion" not in headers: return
        col_id  = headers.index("identificacion") + 1
        ids     = ws.col_values(col_id)
        if str(identificacion) not in ids: return
        fila    = ids.index(str(identificacion)) + 1
        updates = []
        for col_name, val in [
            ("estado_programacion", estado_aliado),
            ("proxima_gestion",     _safe_str_p(proxima_gestion)),
            ("fecha_ultima_gestion",_safe_str_p(now_col())),
        ]:
            if col_name in headers:
                c = headers.index(col_name) + 1
                updates.append({"range": gspread.utils.rowcol_to_a1(fila, c), "values": [[val]]})
        if updates:
            ws.batch_update(updates)
        invalidar_pipeline()
    except Exception as e:
        st.warning(f"No se pudo actualizar BASE_PIPELINE: {e}")

def procesar_carga_pipeline(df_nuevo: pd.DataFrame):
    """
    Carga incremental para BASE_PIPELINE.
    - Si el aliado es nuevo: lo agrega con etapa inferida.
    - Si ya existe: actualiza total_cargues, fecha_ultimo_cargue, dias_sin_cargar.
    - Detecta si alcanzó 7 cargues y actualiza etapa a LISTO_PROGRAMACION / PROGRAMACION.
    - Registra en HISTORIAL_CARGUES si cambió fecha_ultimo_cargue.
    """
    from app_gestion_aliados import leer_hoja, reemplazar_hoja, agregar_filas

    df_nuevo = df_nuevo.copy()
    df_nuevo.columns = (df_nuevo.columns.str.strip().str.lower()
                        .str.replace(r"\s+","_",regex=True))

    # Buscar columna de identificación
    ALIAS_ID = ["identificacion","id_aliado","cedula","id","documento"]
    col_id = next((a for a in ALIAS_ID if a in df_nuevo.columns), None)
    if not col_id:
        st.error(f"No se encontró columna de identificación. Columnas: {list(df_nuevo.columns)}")
        return 0, 0, 0

    df_nuevo = df_nuevo.rename(columns={col_id: "identificacion"})
    df_nuevo["identificacion"] = df_nuevo["identificacion"].astype(str).str.strip()

    # Leer base actual
    base = leer_hoja("BASE_PIPELINE")
    filas_historial = []

    if base.empty:
        # Primera carga
        base = df_nuevo.copy()
        base.columns = base.columns.str.strip().str.lower()
        if "total_cargues" not in base.columns: base["total_cargues"] = 1
        base["etapa_pipeline"] = base["total_cargues"].apply(
            lambda x: _inferir_etapa(int(float(str(x))) if str(x).strip() else 0)
        )
        for c in ["estado_programacion","analista_asignado","proxima_gestion",
                  "fecha_paso_programacion","fecha_ultima_gestion"]:
            base[c] = ""
        reemplazar_hoja("BASE_PIPELINE", base)
        invalidar_pipeline()
        return len(base), 0, 0

    base.columns = base.columns.str.strip().str.lower()
    base["identificacion"] = base["identificacion"].astype(str).str.strip()
    if "total_cargues" not in base.columns: base["total_cargues"] = 0
    base["total_cargues"] = pd.to_numeric(base["total_cargues"], errors="coerce").fillna(0).astype(int)

    ids_existentes = set(base["identificacion"].unique())
    nuevos_count   = 0
    actualizados   = 0
    ascensos       = 0

    for _, row in df_nuevo.iterrows():
        idc = str(row["identificacion"]).strip()
        if idc not in ids_existentes:
            # Nuevo aliado
            nueva_fila = {c: _safe_str_p(row.get(c,"")) for c in df_nuevo.columns}
            if "total_cargues" not in nueva_fila or nueva_fila["total_cargues"] == "":
                nueva_fila["total_cargues"] = "1"
            tc = int(float(str(nueva_fila["total_cargues"])))
            nueva_fila["etapa_pipeline"]        = _inferir_etapa(tc)
            nueva_fila["estado_programacion"]   = ""
            nueva_fila["analista_asignado"]     = ""
            nueva_fila["proxima_gestion"]       = ""
            nueva_fila["fecha_paso_programacion"] = ""
            nueva_fila["fecha_ultima_gestion"]  = ""
            base = pd.concat([base, pd.DataFrame([nueva_fila])], ignore_index=True)
            ids_existentes.add(idc)
            nuevos_count += 1
            # Registro historial
            filas_historial.append([idc, _safe_str_p(now_col()), str(tc), _inferir_etapa(tc),
                                     _safe_str_p(row.get("zona","")), _safe_str_p(row.get("vehiculo",""))])
        else:
            # Aliado existente: actualizar campos operativos
            idx = base[base["identificacion"] == idc].index[0]
            fuc_ant = str(base.loc[idx, "fecha_ultimo_cargue"] if "fecha_ultimo_cargue" in base.columns else "")
            fuc_new = _safe_str_p(row.get("fecha_ultimo_cargue",""))
            tc_ant  = int(base.loc[idx, "total_cargues"])

            # Si la base nueva trae total_cargues úsalo; si no, incrementa si cambió fecha
            if "total_cargues" in df_nuevo.columns and str(row.get("total_cargues","")).strip():
                tc_new = int(float(str(row["total_cargues"])))
            else:
                tc_new = tc_ant + (1 if fuc_new and fuc_new != fuc_ant else 0)

            base.loc[idx, "total_cargues"]      = tc_new
            if "fecha_ultimo_cargue" in df_nuevo.columns:
                base.loc[idx, "fecha_ultimo_cargue"] = fuc_new

            # Calcular nueva etapa
            etapa_ant = str(base.loc[idx, "etapa_pipeline"] if "etapa_pipeline" in base.columns else "")
            etapa_new = _inferir_etapa(tc_new)

            # No retroceder etapa si ya está en FIDELIZADO o PROGRAMACION
            if etapa_ant in ("FIDELIZADO", "PROGRAMACION") and etapa_new not in ("FIDELIZADO",):
                etapa_new = etapa_ant
            if etapa_new != etapa_ant:
                base.loc[idx, "etapa_pipeline"] = etapa_new
                ascensos += 1
                # Si acaba de pasar a LISTO_PROGRAMACION o PROGRAMACION → registrar fecha
                if etapa_new in ("LISTO_PROGRAMACION","PROGRAMACION") and etapa_ant not in ("LISTO_PROGRAMACION","PROGRAMACION","FIDELIZADO"):
                    base.loc[idx, "fecha_paso_programacion"] = _safe_str_p(now_col())
                # Historial de ascenso
                filas_historial.append([idc, _safe_str_p(now_col()), str(tc_new), etapa_new,
                                         _safe_str_p(row.get("zona","")), _safe_str_p(row.get("vehiculo",""))])
            actualizados += 1

    reemplazar_hoja("BASE_PIPELINE", base)
    if filas_historial:
        agregar_filas("HISTORIAL_CARGUES", filas_historial)
    invalidar_pipeline()
    return nuevos_count, actualizados, ascensos


def leer_gestiones_prog() -> pd.DataFrame:
    """Lee GESTIONES_PROGRAMACION con TTL 30s."""
    from app_gestion_aliados import leer_hoja
    import time
    ahora  = time.time()
    ultima = st.session_state.get("gprog_last", 0)
    if "gprog_df" not in st.session_state or (ahora - ultima) > 30:
        cols = ["id_gestion","identificacion","analista","fecha_gestion",
                "resultado_llamada","estado_aliado","razon","observacion",
                "proxima_gestion","total_cargues_momento","dias_sin_cargar_momento"]
        df = leer_hoja("GESTIONES_PROGRAMACION", cols)
        if df.empty: df = pd.DataFrame(columns=cols)
        else:
            df["fecha_gestion"] = pd.to_datetime(df["fecha_gestion"], errors="coerce")
            df = df.dropna(subset=["fecha_gestion"])
        st.session_state["gprog_df"]   = df
        st.session_state["gprog_last"] = ahora
    return st.session_state["gprog_df"]

# ─────────────────────────────────────────────
# CALCULAR PRÓXIMA GESTIÓN (Programación)
# ─────────────────────────────────────────────

def calcular_proxima_prog(resultado, estado, dias_sin_cargar):
    hoy    = now_col()
    estado = str(estado or "")
    if estado in ("Perdido", "No interesado"):
        return "NO_VOLVER"
    no_resp = ["Apagado","Fuera de servicio","No contestó","Número errado"]
    if resultado in no_resp:
        if int(dias_sin_cargar or 0) >= 6:
            return hoy + timedelta(days=2)   # Alta prioridad, volver pronto
        return hoy + timedelta(days=3)
    if estado == "Reactivado":
        return hoy + timedelta(days=5)
    if estado == "En riesgo":
        return hoy + timedelta(days=2)
    return hoy + timedelta(days=5)

# ─────────────────────────────────────────────
# MÓDULO UI — COORDINADOR: PIPELINE DASHBOARD
# ─────────────────────────────────────────────

def render_pipeline_coordinador(nombres_analistas: list):
    """
    Renderiza las pestañas del pipeline para el Coordinador.
    Llamar desde el bloque del Coordinador en la app principal.
    """
    df = leer_pipeline()
    gest = leer_gestiones_prog()

    tab_emb, tab_sup, tab_impl, tab_prog, tab_carga_pip = st.tabs([
        "🔭 Embudo Pipeline",
        "📦 Supplay",
        "⚙️ Implementación",
        "🎯 Programación / Dashboard",
        "📤 Cargar Base Pipeline",
    ])

    # ── EMBUDO ──────────────────────────────
    with tab_emb:
        st.subheader("Embudo completo Supplay → Fidelizado")
        if df is None:
            st.warning("Carga la base pipeline primero (pestaña Cargar Base Pipeline).")
        else:
            c1,c2,c3,c4,c5 = st.columns(5)
            cnt = lambda etapa: len(df[df["etapa_pipeline"]==etapa])
            c1.metric("📦 Supplay",       cnt("SUPPLAY"))
            c2.metric("⚙️ Implementación", cnt("IMPLEMENTACION"))
            c3.metric("🎯 Programación",  cnt("PROGRAMACION") + cnt("LISTO_PROGRAMACION"))
            c4.metric("🏆 Fidelizados",   cnt("FIDELIZADO"))
            c5.metric("⚠️ En riesgo",     len(df[df["estado_semaforo"]=="🟡 Riesgo"]) +
                                          len(df[df["estado_semaforo"]=="🔴 Perdido"]))

            st.markdown("---")
            # Embudo visual
            total_sup  = cnt("SUPPLAY") + cnt("IMPLEMENTACION") + cnt("PROGRAMACION") + cnt("LISTO_PROGRAMACION") + cnt("FIDELIZADO")
            total_impl = cnt("IMPLEMENTACION") + cnt("PROGRAMACION") + cnt("LISTO_PROGRAMACION") + cnt("FIDELIZADO")
            total_prog = cnt("PROGRAMACION") + cnt("LISTO_PROGRAMACION") + cnt("FIDELIZADO")
            total_fid  = cnt("FIDELIZADO")
            emb = pd.DataFrame({
                "Etapa":    ["Supplay (1er cargue)","Implementación (2-7)","Programación (8+)","Fidelizado (15+)"],
                "Cantidad": [total_sup, total_impl, total_prog, total_fid],
            })
            emb["% conv."] = (emb["Cantidad"] / max(total_sup,1) * 100).round(1)
            st.dataframe(emb, use_container_width=True, hide_index=True)
            st.plotly_chart(
                px.funnel(emb, x="Cantidad", y="Etapa",
                          title="Embudo de conversión por etapa",
                          color_discrete_sequence=["#1D9E75","#534AB7","#185FA5","#3B6D11"]),
                use_container_width=True
            )
            # Semáforo solo para Programación
            prog_df = df[df["etapa_pipeline"].isin(["PROGRAMACION","LISTO_PROGRAMACION","FIDELIZADO"])].copy()
            if not prog_df.empty:
                st.markdown("---")
                st.markdown("#### Semáforo — aliados en Programación")
                ca,cm,cr = st.columns(3)
                ca.metric("🟢 Activos",  len(prog_df[prog_df["estado_semaforo"]=="🟢 Activo"]))
                cm.metric("🟡 Riesgo",   len(prog_df[prog_df["estado_semaforo"]=="🟡 Riesgo"]))
                cr.metric("🔴 Perdidos", len(prog_df[prog_df["estado_semaforo"]=="🔴 Perdido"]))
                dist = prog_df["estado_semaforo"].value_counts().reset_index()
                dist.columns = ["semaforo","cantidad"]
                st.plotly_chart(
                    px.pie(dist, values="cantidad", names="semaforo",
                           color="semaforo",
                           color_discrete_map={"🟢 Activo":"#28a745","🟡 Riesgo":"#ffc107","🔴 Perdido":"#dc3545"},
                           title="Distribución semáforo"),
                    use_container_width=True
                )

    # ── SUPPLAY ─────────────────────────────
    with tab_sup:
        st.subheader("📦 Métricas Supplay")
        if df is None:
            st.warning("Carga la base pipeline primero.")
        else:
            sup = df[df["etapa_pipeline"]=="SUPPLAY"].copy()
            impl_mas = df[df["etapa_pipeline"].isin(["IMPLEMENTACION","PROGRAMACION","LISTO_PROGRAMACION","FIDELIZADO"])].copy()
            total_entregados = len(df)  # Todos los que Supplay entregó históricamente
            total_segundo    = len(impl_mas)
            total_7          = len(df[df["total_cargues"] >= CARGUES_PROGRAMACION])
            total_fid2       = len(df[df["etapa_pipeline"]=="FIDELIZADO"])

            c1,c2,c3,c4 = st.columns(4)
            c1.metric("Entregados por Supplay",   total_entregados)
            c2.metric("Hicieron 2do+ cargue",     total_segundo,
                      delta=f"{round(total_segundo/max(total_entregados,1)*100,1)}%")
            c3.metric("Llegaron a 7 cargues",     total_7,
                      delta=f"{round(total_7/max(total_entregados,1)*100,1)}%")
            c4.metric("Fidelizados (15+)",         total_fid2,
                      delta=f"{round(total_fid2/max(total_entregados,1)*100,1)}%")

            st.markdown("---")
            st.markdown(f"**Aliados que NO volvieron a cargar:** {len(sup)} ({round(len(sup)/max(total_entregados,1)*100,1)}%)")
            if not sup.empty:
                cols_s = [c for c in ["identificacion","nombre","celular","zona","vehiculo",
                                       "fecha_ultimo_cargue","total_cargues"] if c in sup.columns]
                st.dataframe(sup[cols_s], use_container_width=True, hide_index=True)
            st.download_button("📥 Descargar aliados no convertidos (Supplay)",
                               sup.to_csv(index=False).encode("utf-8"),
                               "supplay_no_convertidos.csv","text/csv")

    # ── IMPLEMENTACIÓN ──────────────────────
    with tab_impl:
        st.subheader("⚙️ Métricas Implementación")
        if df is None:
            st.warning("Carga la base pipeline primero.")
        else:
            impl = df[df["etapa_pipeline"]=="IMPLEMENTACION"].copy()
            total_impl_hist = len(df[df["total_cargues"] >= CARGUES_IMPLEMENTACION])
            total_conv_7    = len(df[df["total_cargues"] >= CARGUES_PROGRAMACION])
            pct_conv        = round(total_conv_7 / max(total_impl_hist,1) * 100, 1)

            c1,c2,c3 = st.columns(3)
            c1.metric("En Implementación ahora",  len(impl))
            c2.metric("Lograron 7 cargues (hist.)", total_conv_7,
                      delta=f"{pct_conv}% conversión")
            c3.metric("Promedio cargues actuales",
                      f"{impl['total_cargues'].mean():.1f}" if not impl.empty else "N/A")

            st.markdown("---")
            if not impl.empty:
                st.markdown("#### Punto de abandono — distribución por cargue actual")
                hist_c = impl["total_cargues"].value_counts().sort_index().reset_index()
                hist_c.columns = ["cargue","cantidad"]
                st.plotly_chart(
                    px.bar(hist_c, x="cargue", y="cantidad",
                           title="¿En qué cargue están los aliados de Implementación?",
                           labels={"cargue":"Número de cargue","cantidad":"Aliados"},
                           color_discrete_sequence=["#534AB7"]),
                    use_container_width=True
                )
                st.markdown("#### Mejor conversión por vehículo")
                if "vehiculo" in impl.columns:
                    veh = impl.groupby("vehiculo")["total_cargues"].mean().round(1).reset_index()
                    veh.columns = ["Vehículo","Promedio cargues"]
                    st.dataframe(veh.sort_values("Promedio cargues", ascending=False),
                                 use_container_width=True, hide_index=True)
                st.markdown("#### Mejor conversión por zona")
                if "zona" in impl.columns:
                    zon = impl.groupby("zona")["total_cargues"].mean().round(1).reset_index()
                    zon.columns = ["Zona","Promedio cargues"]
                    st.dataframe(zon.sort_values("Promedio cargues", ascending=False),
                                 use_container_width=True, hide_index=True)
            # Listos para pasar
            listos = df[df["etapa_pipeline"]=="LISTO_PROGRAMACION"].copy()
            if not listos.empty:
                st.markdown(f"---\n#### ✅ Listos para Programación ({len(listos)} aliados con 7 cargues)")
                cols_l = [c for c in ["identificacion","nombre","celular","zona","vehiculo",
                                       "total_cargues","fecha_ultimo_cargue"] if c in listos.columns]
                st.dataframe(listos[cols_l], use_container_width=True, hide_index=True)

    # ── PROGRAMACIÓN DASHBOARD ──────────────
    with tab_prog:
        st.subheader("🎯 Dashboard Programación / Fidelización")
        if df is None:
            st.warning("Carga la base pipeline primero.")
        elif gest.empty and df is not None:
            prog_df2 = df[df["etapa_pipeline"].isin(["PROGRAMACION","LISTO_PROGRAMACION","FIDELIZADO"])].copy()
            st.info(f"{len(prog_df2)} aliados en Programación. Aún no hay gestiones registradas.")
        else:
            prog_df2 = df[df["etapa_pipeline"].isin(["PROGRAMACION","LISTO_PROGRAMACION","FIDELIZADO"])].copy()
            c1,c2,c3,c4,c5 = st.columns(5)
            c1.metric("Total en Programación", len(prog_df2))
            c2.metric("🏆 Fidelizados",
                      len(prog_df2[prog_df2["etapa_pipeline"]=="FIDELIZADO"]))
            c3.metric("🟡 En riesgo",
                      len(prog_df2[prog_df2["estado_semaforo"]=="🟡 Riesgo"]))
            c4.metric("🔴 Perdidos",
                      len(prog_df2[prog_df2["estado_semaforo"]=="🔴 Perdido"]))
            reactivados = len(gest[gest["estado_aliado"]=="Reactivado"].drop_duplicates("identificacion"))
            c5.metric("♻️ Reactivaciones", reactivados)

            st.markdown("---")
            # Ranking analistas
            if not gest.empty and "analista" in gest.columns:
                st.markdown("#### Ranking analistas — gestiones")
                rk = gest.groupby("analista").agg(
                    llamadas=("id_gestion","count"),
                    contactados=("resultado_llamada", lambda x: (x=="Sí contestó").sum()),
                    reactivados=("estado_aliado", lambda x: (x=="Reactivado").sum()),
                ).reset_index()
                rk["% contacto"] = (rk["contactados"]/rk["llamadas"]*100).round(1)
                st.dataframe(rk.sort_values("llamadas", ascending=False),
                             use_container_width=True, hide_index=True)

            # Aliados de alta prioridad
            alta = prog_df2[prog_df2["prioridad"]=="🔴 ALTA"].copy()
            if not alta.empty:
                st.markdown(f"---\n#### 🔴 Alta prioridad — {len(alta)} aliados con 6+ días sin cargar")
                cols_a = [c for c in ["identificacion","nombre","celular","zona","vehiculo",
                                       "total_cargues","dias_sin_cargar","estado_semaforo",
                                       "analista_asignado"] if c in alta.columns]
                st.dataframe(alta[cols_a].sort_values("dias_sin_cargar", ascending=False),
                             use_container_width=True, hide_index=True)

    # ── CARGAR BASE PIPELINE ─────────────────
    with tab_carga_pip:
        st.subheader("📤 Cargar Base Pipeline")
        st.info("""
**Columnas esperadas en el Excel:**
`identificacion`, `nombre`, `celular`, `vehiculo`, `zona`,
`fecha_ultimo_cargue`, `total_cargues` *(opcional pero recomendado)*,
`fecha_entrega_supplay` *(opcional)*

Si `total_cargues` no está disponible, la app lo irá acumulando
detectando cambios en `fecha_ultimo_cargue` en cada carga diaria.
""")
        archivo = st.file_uploader("Excel (.xlsx) — base diaria pipeline", type=["xlsx"], key="uploader_pip")
        if archivo:
            try:
                df_s = pd.read_excel(archivo, engine="openpyxl")
                df_s = df_s[[c for c in df_s.columns if not str(c).startswith("Unnamed")]]
                st.success(f"{len(df_s):,} registros leídos")
                st.dataframe(df_s.head(5), use_container_width=True)
                if st.button("🚀 Procesar carga pipeline"):
                    with st.spinner("Procesando..."):
                        nn, na, asc = procesar_carga_pipeline(df_s)
                    st.success(f"✅ {nn} nuevos · {na} actualizados · {asc} aliados ascendieron de etapa")
            except Exception as e:
                st.error(f"Error: {e}")

        # Asignación de analistas a Programación
        st.markdown("---")
        st.markdown("#### Asignación de analistas a Programación")
        pip_actual = leer_pipeline()
        if pip_actual is not None:
            prog_sin = pip_actual[
                pip_actual["etapa_pipeline"].isin(["PROGRAMACION","LISTO_PROGRAMACION"]) &
                (pip_actual.get("analista_asignado", pd.Series(dtype=str)).fillna("").str.strip() == "")
            ].copy() if "analista_asignado" in pip_actual.columns else pd.DataFrame()

            if not prog_sin.empty:
                st.warning(f"{len(prog_sin)} aliados en Programación sin analista asignado.")
            modo_asig = st.selectbox("Modo asignación", ["Automático (balanceado)", "Manual por zona"])
            if modo_asig == "Automático (balanceado)" and st.button("Asignar analistas automáticamente"):
                _asignar_analiistas_automatico(nombres_analistas)
            elif modo_asig == "Manual por zona":
                st.info("Configura asignaciones en la pestaña Asignación de la app principal.")


def _asignar_analiistas_automatico(nombres_analistas: list):
    """Distribuye aliados de Programación sin asignar entre los analistas de forma balanceada."""
    from app_gestion_aliados import conectar_sheets
    import gspread
    try:
        pip = leer_pipeline()
        if pip is None: return
        sin_asignar = pip[
            pip["etapa_pipeline"].isin(["PROGRAMACION","LISTO_PROGRAMACION"]) &
            (pip.get("analista_asignado", pd.Series(dtype=str)).fillna("").str.strip() == "")
        ].copy() if "analista_asignado" in pip.columns else pd.DataFrame()

        if sin_asignar.empty:
            st.info("Todos los aliados ya tienen analista asignado.")
            return
        # Round-robin
        asignaciones = {}
        for i, idc in enumerate(sin_asignar["identificacion"].tolist()):
            asignaciones[str(idc)] = nombres_analistas[i % len(nombres_analistas)]
        # Escribir en Sheets
        sh = conectar_sheets()
        if sh is None: return
        ws = sh.worksheet("BASE_PIPELINE")
        headers = ws.row_values(1)
        if "identificacion" not in headers or "analista_asignado" not in headers: return
        col_id  = headers.index("identificacion") + 1
        col_ana = headers.index("analista_asignado") + 1
        ids_col = ws.col_values(col_id)
        updates = []
        for idc, analista in asignaciones.items():
            if idc in ids_col:
                fila = ids_col.index(idc) + 1
                updates.append({"range": gspread.utils.rowcol_to_a1(fila, col_ana), "values": [[analista]]})
        if updates:
            ws.batch_update(updates)
        invalidar_pipeline()
        st.success(f"✅ {len(asignaciones)} aliados asignados.")
    except Exception as e:
        st.error(f"Error en asignación automática: {e}")


# ─────────────────────────────────────────────
# MÓDULO UI — ANALISTA: CRM PROGRAMACIÓN
# ─────────────────────────────────────────────

def render_crm_programacion(nombre_analista: str):
    """
    Renderiza el tab CRM de Programación para el Analista.
    Llamar desde el bloque del Analista en la app principal.
    """
    pip = leer_pipeline()
    gest = leer_gestiones_prog()

    tab_mis, tab_buscar, tab_res = st.tabs([
        "📞 Mis aliados — Programación",
        "🔍 Buscar aliado",
        "📊 Mi rendimiento",
    ])

    # ── MIS ALIADOS ──────────────────────────
    with tab_mis:
        if pip is None:
            st.warning("La base pipeline aún no está cargada.")
            return

        # Filtrar aliados de este analista en Programación
        mis_aliados = pip[
            (pip["etapa_pipeline"].isin(["PROGRAMACION","LISTO_PROGRAMACION"])) &
            (pip.get("analista_asignado", pd.Series(dtype=str)).fillna("") == nombre_analista)
        ].copy() if "analista_asignado" in pip.columns else pd.DataFrame()

        if mis_aliados.empty:
            st.info("No tienes aliados asignados en Programación. Pídele al coordinador que te asigne.")
            return

        # Excluir ya gestionados hoy
        if not gest.empty:
            gv = gest.copy()
            gv["fecha_gestion"] = pd.to_datetime(gv["fecha_gestion"], errors="coerce")
            gv = gv.dropna(subset=["fecha_gestion"])
            ya_hoy = gv[gv["fecha_gestion"].dt.date == now_col().date()]["identificacion"].astype(str).tolist()
            mis_aliados = mis_aliados[~mis_aliados["identificacion"].astype(str).isin(ya_hoy)]

        # Excluir NO_VOLVER
        if "proxima_gestion" in mis_aliados.columns:
            mis_aliados = mis_aliados[mis_aliados["proxima_gestion"].astype(str).str.upper() != "NO_VOLVER"]
            def disponible_hoy(v):
                v = str(v).strip()
                if v in ("","nan","None","0"): return True
                f = pd.to_datetime(v, errors="coerce")
                return pd.isna(f) or f <= now_col()
            mis_aliados = mis_aliados[mis_aliados["proxima_gestion"].apply(disponible_hoy)]

        # Ordenar por prioridad
        orden_p = {"🔴 ALTA":0, "🟡 MEDIA":1, "🟢 BAJA":2}
        mis_aliados["_ord"] = mis_aliados["prioridad"].map(orden_p).fillna(3)
        mis_aliados = mis_aliados.sort_values("_ord").drop(columns=["_ord"]).reset_index(drop=True)

        hechas = st.session_state.get("prog_hechas", 0)
        pend   = len(mis_aliados)
        pct    = int(hechas / max(hechas + pend, 1) * 100)
        st.progress(pct, text=f"Progreso: {hechas} gestionados · {pend} pendientes")

        # Tabla resumen
        cols_v = [c for c in ["identificacion","nombre","celular","zona","vehiculo",
                               "total_cargues","dias_sin_cargar","estado_semaforo","prioridad"]
                  if c in mis_aliados.columns]
        st.markdown(f"#### Pendientes hoy ({pend})")
        st.dataframe(mis_aliados[cols_v], use_container_width=True, hide_index=True)

        # Formulario de gestión
        st.markdown("---")
        st.markdown("#### 📞 Registrar gestión")
        ids_disp = mis_aliados["identificacion"].astype(str).tolist()

        with st.form("form_prog", clear_on_submit=True):
            c1, c2 = st.columns(2)
            with c1:
                ali    = st.selectbox("Cédula del aliado", ids_disp)
                res    = st.selectbox("Resultado de la llamada", RESULTADOS_PROG)
            with c2:
                est    = st.selectbox("Estado aliado", ["-"] + ESTADOS_PROGRAMACION)
                raz    = st.selectbox("Razón", ["-"] + RAZONES_PROG)

            # Ficha del aliado seleccionado
            fd = mis_aliados[mis_aliados["identificacion"].astype(str) == str(ali)]
            if not fd.empty:
                f = fd.iloc[0]
                st.markdown("**Ficha operativa**")
                metricas = [c for c in ["nombre","celular","total_cargues","dias_sin_cargar",
                                         "estado_semaforo","prioridad","fecha_ultimo_cargue"] if c in f.index]
                cols_m = st.columns(min(len(metricas), 4))
                for i, cm in enumerate(metricas):
                    cols_m[i % 4].metric(cm.replace("_"," ").title(), str(f[cm]))

            obs  = st.text_area("Observaciones")
            sub  = st.form_submit_button("💾 GUARDAR GESTIÓN")

        if sub:
            er = None if est == "-" else est
            rr = None if raz == "-" else raz
            if res == "Sí contestó" and er is None:
                st.error("Selecciona el estado del aliado.")
            else:
                dias_sc = int(fd.iloc[0].get("dias_sin_cargar", 0)) if not fd.empty else 0
                tc_m    = int(fd.iloc[0].get("total_cargues", 0)) if not fd.empty else 0
                prox    = calcular_proxima_prog(res, er, dias_sc)
                guardar_gestion_prog({
                    "identificacion":        ali,
                    "analista":              nombre_analista,
                    "resultado_llamada":     res,
                    "estado_aliado":         er,
                    "razon":                 rr,
                    "observacion":           obs,
                    "proxima_gestion":       prox,
                    "total_cargues_momento": tc_m,
                    "dias_sin_cargar_momento": dias_sc,
                })
                st.session_state["prog_hechas"] = st.session_state.get("prog_hechas", 0) + 1
                st.success(f"✅ Guardado. Próxima gestión: {_safe_str_p(prox)}")
                st.rerun()

    # ── BUSCAR ───────────────────────────────
    with tab_buscar:
        st.subheader("🔍 Buscar aliado en Programación")
        cedula_b = st.text_input("Cédula", "", key="prog_buscar_cc")
        if cedula_b.strip() and pip is not None:
            fila = pip[pip["identificacion"].astype(str) == cedula_b.strip()]
            if fila.empty:
                st.warning(f"No se encontró el aliado {cedula_b} en la base pipeline.")
            else:
                f = fila.iloc[0]
                st.success("✅ Aliado encontrado")
                cols_info = [c for c in ["identificacion","nombre","celular","zona","vehiculo",
                                          "etapa_pipeline","total_cargues","fecha_ultimo_cargue",
                                          "dias_sin_cargar","estado_semaforo","prioridad",
                                          "fecha_paso_programacion","analista_asignado"] if c in f.index]
                c1, c2 = st.columns(2)
                mid = len(cols_info) // 2
                with c1:
                    for col in cols_info[:mid]: st.metric(col.replace("_"," ").title(), str(f[col]))
                with c2:
                    for col in cols_info[mid:]: st.metric(col.replace("_"," ").title(), str(f[col]))

                # Historial de gestiones del aliado
                st.markdown("---")
                if not gest.empty:
                    h_ali = gest[gest["identificacion"].astype(str) == cedula_b.strip()].copy()
                    if h_ali.empty:
                        st.info("Sin gestiones en Programación para este aliado.")
                    else:
                        h_ali["Hora"] = h_ali["fecha_gestion"].dt.strftime("%d/%m/%Y %I:%M %p")
                        st.dataframe(
                            h_ali[["Hora","analista","resultado_llamada","estado_aliado","razon","observacion"]].rename(
                                columns={"analista":"Analista","resultado_llamada":"Resultado",
                                         "estado_aliado":"Estado","razon":"Razón","observacion":"Obs"}
                            ), use_container_width=True, hide_index=True
                        )

    # ── MI RENDIMIENTO ───────────────────────
    with tab_res:
        st.subheader(f"Mi rendimiento — {nombre_analista.split()[0]}")
        if gest.empty:
            st.info("Aún no tienes gestiones en Programación.")
        else:
            mis_g = gest[gest["analista"] == nombre_analista].copy()
            if mis_g.empty:
                st.info("Aún no tienes gestiones registradas.")
            else:
                t  = len(mis_g)
                sc = len(mis_g[mis_g["resultado_llamada"] == "Sí contestó"])
                re = len(mis_g[mis_g["estado_aliado"] == "Reactivado"])
                nr = len(mis_g[mis_g["resultado_llamada"].isin(["Apagado","Fuera de servicio","No contestó","Número errado"])])
                c1,c2,c3,c4 = st.columns(4)
                c1.metric("📞 Total llamadas", t)
                c2.metric("✅ Contactados",    sc)
                c3.metric("♻️ Reactivados",    re)
                c4.metric("📵 No responde",    nr)
                tend = mis_g.groupby(mis_g["fecha_gestion"].dt.date).size().reset_index(name="gestiones")
                tend.columns = ["fecha","gestiones"]
                st.plotly_chart(
                    px.bar(tend, x="fecha", y="gestiones", title="Mis gestiones por día",
                           color_discrete_sequence=["#185FA5"]),
                    use_container_width=True
                )


# ─────────────────────────────────────────────
# INSTRUCCIONES DE INTEGRACIÓN
# ─────────────────────────────────────────────
"""
CÓMO INTEGRAR ESTE MÓDULO EN app_gestion_aliados.py
=====================================================

1. IMPORTS — al inicio de app_gestion_aliados.py agregar:
   from modulo_pipeline import (
       render_pipeline_coordinador,
       render_crm_programacion,
       leer_pipeline,
       invalidar_pipeline,
   )

2. EN EL BLOQUE COORDINADOR — después de las tabs actuales agregar:
   st.markdown("---")
   st.markdown("## 🔭 Pipeline Supplay → Implementación → Programación")
   render_pipeline_coordinador(NOMBRES_ANALISTAS)

3. EN EL BLOQUE ANALISTA — agregar una tab nueva:
   tab_g, tab_h, tab_his, tab_bus, tab_prog_crm = st.tabs([
       "📞 Gestión del Día",
       "📊 Mi Resumen de Hoy",
       "📅 Mi Histórico",
       "🔍 Buscar Aliado",
       "🎯 Programación CRM",   # ← nueva
   ])
   with tab_prog_crm:
       render_crm_programacion(nombre)

4. HOJAS NUEVAS EN GOOGLE SHEETS — crear con estas columnas:

   BASE_PIPELINE:
   identificacion | nombre | celular | vehiculo | zona | etapa_pipeline |
   total_cargues | fecha_ultimo_cargue | fecha_entrega_supplay |
   fecha_entrega_impl | fecha_paso_programacion | dias_sin_cargar |
   estado_semaforo | prioridad | analista_asignado | estado_programacion |
   fecha_ultima_gestion | proxima_gestion

   GESTIONES_PROGRAMACION:
   id_gestion | identificacion | analista | fecha_gestion |
   resultado_llamada | estado_aliado | razon | observacion |
   proxima_gestion | total_cargues_momento | dias_sin_cargar_momento

   HISTORIAL_CARGUES:
   identificacion | fecha_cargue | numero_cargue | etapa_en_momento | zona | vehiculo

   KPI_DIARIO:
   fecha | total_supplay | total_implementacion | total_programacion |
   total_fidelizados | total_en_riesgo | total_perdidos |
   conv_supplay_impl | conv_impl_prog | reactivaciones_dia

=====================================================
"""
