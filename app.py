
NO_RESPONDEN      = ["Apagado","Fuera de servicio","No contestó","Número errado"]
NO_VOLVER_ESTADOS = ["Aliado Rechaza la oferta","Empleado","Point"]
NO_VOLVER_RAZONES = ["No le interesa / cuestiones personales"]
COLS_CRM = ["intentos","ultimo_resultado","ultimo_estado","ultima_razon","fecha_gestion","proxima_gestion"]
COLS_CRM = ["intentos","ultimo_resultado","ultimo_estado","ultima_razon","fecha_gestion","proxima_gestion"]

def excluir_aliados_inactivos(df: pd.DataFrame) -> pd.DataFrame:
    """Excluye aliados cuyo Estado en BASE sea Inactivo de toda operación."""
    if df is None or df.empty or "estado" not in df.columns:
        return df
    estado = df["estado"].fillna("").astype(str).str.strip().str.casefold()
    return df.loc[estado != "inactivo"].copy()

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
                    if a in df.columns: df["celular"] = df[a]; break
            if "zona" not in df.columns and "municipio" in df.columns:
                df["zona"] = df["municipio"]
            if "zona" not in df.columns:
                df["zona"] = "Sin zona"
            if "zona" not in df.columns:
                df["zona"] = "Sin zona"
            # El descarte se realiza al consultar BASE, antes de poblar vistas,
            # pools, indicadores, búsquedas o asignaciones.
            df = excluir_aliados_inactivos(df)
            df["vehiculo_norm"] = df["vehiculo"].apply(_norm_vh) if "vehiculo" in df.columns else "Sin vehículo"
            df["dias"] = 0
            col_f = next((c for c in ["fecha_ultimo_cargue","fecha ultimo cargue","fechaultimocargue"]
        return hoy + timedelta(days=5)
    return hoy + timedelta(days=3)

def filtrar_pool(df):
    if "proxima_gestion" not in df.columns: return df
def filtrar_pool(df):
    df = excluir_aliados_inactivos(df)
    if df is None:
        return df
    if "proxima_gestion" not in df.columns: return df
    df = df.copy()
    df = df[df["proxima_gestion"].astype(str).str.upper() != "NO_VOLVER"]
    def disponible(v):
        return 0, 0
    df_nuevo = df_nuevo.rename(columns={col_id: "identificacion"})
    df_nuevo = _df_safe_str(df_nuevo)
    df_nuevo["identificacion"] = df_nuevo["identificacion"].str.strip()
    df_nuevo = df_nuevo.fillna("")
    if base_actual.empty:
        for col in COLS_CRM:
    df_nuevo["identificacion"] = df_nuevo["identificacion"].str.strip()
    df_nuevo = df_nuevo.fillna("")
    if base_actual.empty:
        df_nuevo = excluir_aliados_inactivos(df_nuevo)
        for col in COLS_CRM:
            df_nuevo[col] = "0" if col == "intentos" else ""
        reemplazar_hoja("BASE", df_nuevo)
        _invalidar_base()
        else:
            base_idx = base_idx.join(col_data, how="left")
    base_actualizada = base_idx.reset_index()
    base_final = pd.concat([base_actualizada, nuevos], ignore_index=True)
    base_final = base_final.fillna("")
    base_final = base_final.loc[:, ~base_final.columns.duplicated()]
    reemplazar_hoja("BASE", base_final)
    base_final = pd.concat([base_actualizada, nuevos], ignore_index=True)
    base_final = base_final.fillna("")
    base_final = base_final.loc[:, ~base_final.columns.duplicated()]
    # Impide que nuevos inactivos entren y elimina los que cambien a Inactivo.
    base_final = excluir_aliados_inactivos(base_final)
    reemplazar_hoja("BASE", base_final)
    _invalidar_base()
    return len(nuevos), len(existentes_datos)

# ================================================================
# MÓDULO IMPLEMENTACIÓN
# ================================================================
CARGUES_META_IMPL = 20  # Meta final: pasa a Programación
CARGUES_MIN_IMPL  = 7   # Los analistas reciben desde el cargue 7
PASS_IMPL_COORD   = "impl_coord"
PASS_IMPL_ANA     = "impl2024"
ANALISTAS_IMPL    = ["Analista Impl 1","Analista Impl 2","Analista Impl 3","Analista Impl 4"]
CARGUES_META_IMPL = 20  # Meta final: pasa a Programación
CARGUES_MIN_IMPL  = 7   # Los analistas reciben desde el cargue 7
# Implementación se accede desde el mismo perfil autenticado de Programación;
# no conserva credenciales ni usuarios independientes.

RESULTADOS_IMPL = ["Apagado","Fuera de servicio","No contestó","Número errado","Sí contestó"]
ESTADOS_IMPL = [
    ultima = st.session_state.get("impl_last_load", 0)
    if force or "impl_df" not in st.session_state or (ahora - ultima) > 30:
        df = leer_hoja("BASE_IMPLEMENTACION")
        if df.empty:
            st.session_state["impl_df"] = None
        else:
            df.columns = df.columns.str.strip().str.lower()

            # FIX zona: alias múltiples
        if df.empty:
            st.session_state["impl_df"] = None
        else:
            df.columns = df.columns.str.strip().str.lower()
