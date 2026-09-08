import unicodedata
from datetime import date, datetime, timedelta

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials

# =========================================================================
# CONFIGURACIÓN GENERAL
# =========================================================================
st.set_page_config("Planeación de Aliados", "🚚", layout="wide")

SHEET_ID = "1ySPooqSBmL3yJTyPONwdM7__RYwunge669xQg7Ngrao"  # mismo libro de Google Sheets de siempre
META = 20  # cargues/rutas que un aliado debe alcanzar para "graduarse"

ANALISTAS = [
    "Deisy Liliana Garcia", "Erica Tatiana Garzon", "Dayan Stefany Suarez",
    "Carlos Andres Loaiza", "Diana Paola Rueda Jimenez",
]
VEHICULOS = ["Moto", "Carry / Van", "Camión", "Otro"]

# -------------------------------------------------------------------------
# Catálogos: módulo GESTIÓN DE ALIADOS (analista - Planeación)
# -------------------------------------------------------------------------
RESULTADOS = ["Apagado", "Fuera de servicio", "No contestó", "Número errado", "Sí contestó"]

ESTADOS_FINALES_ALIADOS = [
    "Aliado Rechaza la oferta",
    "Aliado Fleet/Delivery no acepta hub/Carga en otra operacion",
    "Interesado Carga/Reserva",
    "Interesado esporádico no fijo",
    "Point",
]

RAZONES = [
    "—",
    "Interesado carga hoy",
    "No le interesa / cuestiones personales",
    "No tiene Vh / Vh dañado",
    "Peso / Volumen / recorrido",
    "Tarifa",
    "Tiene trabajo fijo",
    "Fuera de la ciudad",
    "Aliado no carga en HUB",
    "Ocasional no fijo",
    "Se reserva",
    "Point",
]

# Resultados de llamada que se consideran "sin contacto real" -> alimentan el contador de bloqueo
SIN_CONTACTO = {"Apagado", "Fuera de servicio", "No contestó", "Número errado"}

# Si el estado final (o la razón) cae aquí => bloqueo permanente inmediato, sin importar los intentos
BLOQUEO_INMEDIATO_ALIADOS = {"Aliado Rechaza la oferta", "Point"}
RAZONES_BLOQUEO_ALIADOS = {"No le interesa / cuestiones personales"}

# -------------------------------------------------------------------------
# Catálogos: módulo REQUERIMIENTOS SUPPLY
# -------------------------------------------------------------------------
ESTADOS_FINALES_REQ = [
    "Aliado Contactado para primer cargue",
    "Aliado Rechaza la oferta",
    "Interesado Carga",
    "Pendiente gestion area encargada",
    "Requerimiento cerrado/finalizado",
    "Aliado nunca lo usaron",
]
BLOQUEO_INMEDIATO_REQ = {"Aliado Rechaza la oferta"}
CIERRE_REQ = {"Requerimiento cerrado/finalizado", "Aliado nunca lo usaron"}
VALIDACION_REQ = {"Aliado Contactado para primer cargue", "Interesado Carga"}

# -------------------------------------------------------------------------
# Catálogos: módulo COORDINADOR
# -------------------------------------------------------------------------
ESTADO_CLICOH = ["Activo", "Inactivo"]
ESTADO_IMPLEMENTACION = [
    "Activo", "Deserta", "En implementacion", "No responde",
    "Rechazado por Clicoh", "Se reserva", "Supera Implementacion",
]

# Alias de encabezados que puede traer la base que envía el área de Implementación.
# _normalizar() quita tildes/mayúsculas para que el "match" no se rompa por formato.
ALIAS_COORDINADOR = {
    "nombre": "nombre",
    "documento": "documento",
    "cedula": "documento",
    "celular": "celular",
    "telefono": "celular",
    "ciudad": "ciudad",
    "vehiculo": "vehiculo",
    "# rutas": "rutas",
    "rutas": "rutas",
    "numero de rutas": "rutas",
    "cantidad de rutas": "rutas",
    "estado clicoh": "estado_clicoh",
    "estado implementacion": "estado_implementacion",
    "fecha ultimo cargue": "fecha_ultimo_cargue",
}

# =========================================================================
# ESTRUCTURA DE HOJAS EN GOOGLE SHEETS
# =========================================================================
HOJAS = {
    "planeacion": ("PLANEACION_ALIADOS", [
        "identificacion", "nombre", "celular", "zona", "vehiculo", "analista",
        "estado_planeacion", "categoria", "razon",
        "intentos_llamada", "intentos_sin_contacto",
        "ultimo_resultado", "proxima_gestion",
        "bloqueado", "fecha_ingreso", "ultima_gestion", "observaciones",
    ]),
    "planeacion_gestiones": ("PLANEACION_GESTIONES", [
        "fecha", "identificacion", "analista", "resultado",
        "estado_final", "razon", "proxima_gestion", "observaciones",
    ]),
    "requerimientos": ("REQUERIMIENTOS_ALIADOS", [
        "numero_requerimiento", "nombre", "telefono", "vehiculo", "cantidad_rutas",
        "estado_gestion", "ultimo_estado", "intentos_llamada", "intentos_sin_contacto",
        "ultimo_resultado", "proxima_gestion",
        "bloqueado", "fecha_ingreso", "ultima_gestion", "observaciones",
    ]),
    "requerimientos_gestiones": ("REQUERIMIENTOS_GESTIONES", [
        "fecha", "numero_requerimiento", "nombre", "resultado",
        "estado_final", "proxima_gestion", "observaciones",
    ]),
    "coordinador": ("COORDINADOR_ALIADOS", [
        "documento", "nombre", "celular", "ciudad", "vehiculo", "rutas",
        "estado_clicoh", "estado_implementacion", "fecha_ultimo_cargue",
        "proxima_gestion", "ultima_gestion", "observaciones",
    ]),
    "coordinador_gestiones": ("COORDINADOR_GESTIONES", [
        "fecha", "documento", "nombre", "estado_implementacion", "rutas", "observaciones",
    ]),
}

# =========================================================================
# CONEXIÓN A GOOGLE SHEETS  (mismo patrón que ya usábamos)
# =========================================================================
@st.cache_resource
def libro():
    try:
        creds = Credentials.from_service_account_info(
            dict(st.secrets["gcp_service_account"]),
            scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
        )
        return gspread.authorize(creds).open_by_key(st.secrets.get("planeacion_sheet_id", SHEET_ID))
    except Exception as e:
        st.error(f"Error conectando con Google Sheets: {e}")
        st.stop()


def hoja(clave):
    nombre, columnas = HOJAS[clave]
    try:
        ws = libro().worksheet(nombre)
    except gspread.WorksheetNotFound:
        ws = libro().add_worksheet(nombre, rows=2000, cols=len(columnas) + 2)
        ws.append_row(columnas)
    if not ws.row_values(1):
        ws.append_row(columnas)
    return ws, columnas


def cargar(clave):
    ws, columnas = hoja(clave)
    df = pd.DataFrame(ws.get_all_records(default_blank=""))
    for c in columnas:
        if c not in df:
            df[c] = ""
    return df[columnas].fillna("")


def guardar(clave, df):
    ws, columnas = hoja(clave)
    limpio = df.reindex(columns=columnas).fillna("").astype(str)
    ws.clear()
    ws.update([columnas] + limpio.values.tolist())


def agregar(clave, fila):
    ws, columnas = hoja(clave)
    ws.append_row([str(fila.get(c, "")) for c in columnas], value_input_option="USER_ENTERED")


def es_verdadero(valor):
    """Convierte lo que venga de la hoja ('True', '1', 'sí'...) en booleano real."""
    return str(valor).strip().lower() in ("true", "1", "sí", "si", "yes")


def a_entero(valor, defecto=0):
    try:
        return int(float(valor))
    except (ValueError, TypeError):
        return defecto


def _normalizar(texto):
    """Quita tildes, pasa a minúsculas y recorta espacios, para que 'Vehículo' == 'vehiculo'."""
    texto = str(texto).strip().lower()
    return "".join(c for c in unicodedata.normalize("NFKD", texto) if not unicodedata.combining(c))


def mapear_columnas(df, alias):
    nuevas = {}
    for col in df.columns:
        clave = _normalizar(col)
        if clave in alias:
            nuevas[col] = alias[clave]
    return df.rename(columns=nuevas)


# =========================================================================
# REGLAS DE NEGOCIO
# =========================================================================
def regla_intentos_sin_contacto(intentos):
    """
    Devuelve (dias_de_espera, estado_workflow, bloqueado) según el número
    ACUMULADO de intentos sin contacto real (nunca se reinicia, ni tras pausas).
    """
    if intentos >= 15:
        return None, "Bloqueado permanente", True
    if intentos >= 10:
        return 15, "Pausado", False
    return 1, "En gestión", False


def procesar_gestion_planeacion(fila, resultado, estado_final, razon, nota):
    """Aplica las reglas de Gestión de Aliados (Planeación) a un evento de llamada."""
    hoy = date.today()
    intentos_llamada = a_entero(fila.get("intentos_llamada")) + 1
    intentos_sin_contacto = a_entero(fila.get("intentos_sin_contacto"))

    actualizacion = {
        "intentos_llamada": intentos_llamada,
        "ultimo_resultado": resultado,
        "ultima_gestion": hoy,
        "observaciones": nota or fila.get("observaciones", ""),
    }
    estado_final_log, razon_log = "", ""

    if resultado in SIN_CONTACTO:
        intentos_sin_contacto += 1
        dias, estado_planeacion, bloqueado = regla_intentos_sin_contacto(intentos_sin_contacto)
        actualizacion.update({
            "intentos_sin_contacto": intentos_sin_contacto,
            "estado_planeacion": estado_planeacion,
            "bloqueado": bloqueado,
            "proxima_gestion": "" if bloqueado else hoy + timedelta(days=dias),
        })
    else:  # "Sí contestó" -> requiere estado_final
        estado_final_log, razon_log = estado_final, razon
        bloqueo_directo = (estado_final in BLOQUEO_INMEDIATO_ALIADOS) or (razon in RAZONES_BLOQUEO_ALIADOS)

        if bloqueo_directo:
            actualizacion.update({
                "estado_planeacion": "Bloqueado permanente", "bloqueado": True,
                "proxima_gestion": "", "categoria": estado_final, "razon": razon,
            })
        elif estado_final == "Aliado Fleet/Delivery no acepta hub/Carga en otra operacion":
            actualizacion.update({
                "estado_planeacion": "Pausado", "bloqueado": False,
                "proxima_gestion": hoy + timedelta(days=5),
                "categoria": estado_final, "razon": razon,
            })
        elif estado_final == "Interesado esporádico no fijo":
            actualizacion.update({
                "estado_planeacion": "En gestión", "bloqueado": False,
                "proxima_gestion": hoy + timedelta(days=3),
                "categoria": estado_final, "razon": razon,
            })
        elif estado_final == "Interesado Carga/Reserva":
            actualizacion.update({
                "estado_planeacion": "Validación pendiente", "bloqueado": False,
                "proxima_gestion": hoy + timedelta(days=1),
                "categoria": estado_final, "razon": razon,
            })
        else:
            actualizacion.update({
                "estado_planeacion": "En gestión", "bloqueado": False,
                "proxima_gestion": hoy + timedelta(days=1),
                "categoria": estado_final, "razon": razon,
            })

    log = {
        "fecha": datetime.now(), "identificacion": fila.get("identificacion"),
        "analista": fila.get("analista"), "resultado": resultado,
        "estado_final": estado_final_log, "razon": razon_log,
        "proxima_gestion": actualizacion.get("proxima_gestion", ""), "observaciones": nota,
    }
    return actualizacion, log


def procesar_validacion_planeacion(fila, cargo, nota):
    """Para aliados en 'Validación pendiente': ¿llegó a hacer el cargue o no?"""
    hoy = date.today()
    if cargo:
        actualizacion = {
            "estado_planeacion": "Completado", "proxima_gestion": "",
            "ultima_gestion": hoy, "observaciones": nota or fila.get("observaciones", ""),
        }
        estado_log = "Validado - inició cargue"
    else:
        actualizacion = {
            "estado_planeacion": "En gestión", "proxima_gestion": hoy + timedelta(days=1),
            "ultima_gestion": hoy, "observaciones": nota or fila.get("observaciones", ""),
        }
        estado_log = "No llegó a cargue - recontacto"

    log = {
        "fecha": datetime.now(), "identificacion": fila.get("identificacion"),
        "analista": fila.get("analista"), "resultado": "Sí contestó",
        "estado_final": estado_log, "razon": "",
        "proxima_gestion": actualizacion.get("proxima_gestion", ""), "observaciones": nota,
    }
    return actualizacion, log


def procesar_gestion_requerimiento(fila, resultado, estado_final, nota):
    hoy = date.today()
    intentos_llamada = a_entero(fila.get("intentos_llamada")) + 1
    intentos_sin_contacto = a_entero(fila.get("intentos_sin_contacto"))

    actualizacion = {
        "intentos_llamada": intentos_llamada, "ultimo_resultado": resultado,
        "ultima_gestion": hoy, "observaciones": nota or fila.get("observaciones", ""),
    }
    estado_final_log = ""

    if resultado in SIN_CONTACTO:
        intentos_sin_contacto += 1
        dias, estado_gestion, bloqueado = regla_intentos_sin_contacto(intentos_sin_contacto)
        actualizacion.update({
            "intentos_sin_contacto": intentos_sin_contacto,
            "estado_gestion": estado_gestion, "bloqueado": bloqueado,
            "proxima_gestion": "" if bloqueado else hoy + timedelta(days=dias),
        })
    else:
        estado_final_log = estado_final
        if estado_final in BLOQUEO_INMEDIATO_REQ:
            actualizacion.update({"estado_gestion": "Bloqueado permanente", "bloqueado": True,
                                   "proxima_gestion": "", "ultimo_estado": estado_final})
        elif estado_final in CIERRE_REQ:
            actualizacion.update({"estado_gestion": "Cerrado", "bloqueado": False,
                                   "proxima_gestion": "", "ultimo_estado": estado_final})
        elif estado_final == "Pendiente gestion area encargada":
            actualizacion.update({"estado_gestion": "Pendiente área encargada", "bloqueado": False,
                                   "proxima_gestion": hoy + timedelta(days=5), "ultimo_estado": estado_final})
        elif estado_final in VALIDACION_REQ:
            actualizacion.update({"estado_gestion": "Validación pendiente", "bloqueado": False,
                                   "proxima_gestion": hoy + timedelta(days=1), "ultimo_estado": estado_final})
        else:
            actualizacion.update({"estado_gestion": "En gestión", "bloqueado": False,
                                   "proxima_gestion": hoy + timedelta(days=1), "ultimo_estado": estado_final})

    log = {
        "fecha": datetime.now(), "numero_requerimiento": fila.get("numero_requerimiento"),
        "nombre": fila.get("nombre"), "resultado": resultado, "estado_final": estado_final_log,
        "proxima_gestion": actualizacion.get("proxima_gestion", ""), "observaciones": nota,
    }
    return actualizacion, log


def procesar_validacion_requerimiento(fila, uso_cupo, nota):
    hoy = date.today()
    if uso_cupo:
        actualizacion = {
            "estado_gestion": "Cerrado", "ultimo_estado": "Requerimiento cerrado/finalizado",
            "proxima_gestion": "", "ultima_gestion": hoy,
            "observaciones": nota or fila.get("observaciones", ""),
        }
    else:
        actualizacion = {
            "estado_gestion": "En gestión", "proxima_gestion": hoy + timedelta(days=1),
            "ultima_gestion": hoy, "observaciones": nota or fila.get("observaciones", ""),
        }
    log = {
        "fecha": datetime.now(), "numero_requerimiento": fila.get("numero_requerimiento"),
        "nombre": fila.get("nombre"), "resultado": "Sí contestó",
        "estado_final": actualizacion.get("ultimo_estado", "Recontacto - no usó el cupo"),
        "proxima_gestion": actualizacion.get("proxima_gestion", ""), "observaciones": nota,
    }
    return actualizacion, log


def aplicar_actualizacion(df, columna_clave, valor_clave, actualizacion):
    idx = df[df[columna_clave].astype(str) == str(valor_clave)].index[0]
    for campo, valor in actualizacion.items():
        df.loc[idx, campo] = valor
    return df


# =========================================================================
# INTERFAZ
# =========================================================================
st.title("🚚 Planeación de Aliados")
st.caption("Gestión de aliados y requerimientos de Supply hasta alcanzar la meta de 20 cargues, y seguimiento de Coordinador.")

tablero, analista_tab, requerimientos_tab, coordinador_tab, kpis_tab = st.tabs(
    ["📊 Tablero", "🧑‍💼 Gestión de Aliados", "📨 Requerimientos", "🧑‍🏭 Coordinador", "📘 KPIs"]
)

# -------------------------------------------------------------------------
# TABLERO
# -------------------------------------------------------------------------
with tablero:
    df_plan = cargar("planeacion")
    df_req = cargar("requerimientos")
    df_coord = cargar("coordinador")
    hoy = date.today()

    st.subheader("Gestión de Aliados (Planeación)")
    if df_plan.empty:
        st.info("Aún no hay aliados registrados en Planeación.")
    else:
        pend = df_plan[df_plan.proxima_gestion.astype(str).apply(
            lambda v: pd.to_datetime(v, errors="coerce")) <= pd.Timestamp(hoy)]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total en Planeación", len(df_plan))
        c2.metric("Pendientes de gestión hoy", len(pend[~pend.bloqueado.apply(es_verdadero)]))
        c3.metric("Pausados", int((df_plan.estado_planeacion == "Pausado").sum()))
        c4.metric("Bloqueados permanentes", int(df_plan.bloqueado.apply(es_verdadero).sum()))

    st.subheader("Requerimientos Supply")
    if df_req.empty:
        st.info("Aún no hay requerimientos registrados.")
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total requerimientos", len(df_req))
        c2.metric("Cerrados", int((df_req.estado_gestion == "Cerrado").sum()))
        c3.metric("Pendientes área encargada", int((df_req.estado_gestion == "Pendiente área encargada").sum()))
        c4.metric("Bloqueados permanentes", int(df_req.bloqueado.apply(es_verdadero).sum()))

    st.subheader("Coordinador")
    if df_coord.empty:
        st.info("Aún no se ha cargado ninguna base de Implementación.")
    else:
        rutas_num = df_coord.rutas.apply(a_entero)
        c1, c2, c3 = st.columns(3)
        c1.metric("Aliados en seguimiento", len(df_coord))
        c2.metric("Superaron la meta (20 rutas)", int((rutas_num >= META).sum()))
        c3.metric("Activos clicOH", int((df_coord.estado_clicoh == "Activo").sum()))

# -------------------------------------------------------------------------
# GESTIÓN DE ALIADOS (ANALISTA - PLANEACIÓN)
# -------------------------------------------------------------------------
with analista_tab:
    df_plan = cargar("planeacion")

    with st.expander("➕ Registrar aliado nuevo en Planeación", expanded=df_plan.empty):
        with st.form("form_nuevo_planeacion"):
            c1, c2, c3 = st.columns(3)
            ident = c1.text_input("Identificación (cédula) *", key="p_ident")
            nombre = c2.text_input("Nombre *", key="p_nombre")
            celular = c3.text_input("Celular *", key="p_celular")
            zona = c1.text_input("Zona / HUB", key="p_zona")
            vehiculo = c2.selectbox("Vehículo", VEHICULOS, key="p_vehiculo")
            analista = c3.selectbox("Analista", ANALISTAS, key="p_analista")
            crear = st.form_submit_button("Guardar aliado")
        if crear:
            if not ident or not nombre or not celular:
                st.error("Identificación, nombre y celular son obligatorios.")
            elif ident in df_plan.identificacion.astype(str).tolist():
                st.error("Ese aliado ya está registrado en Planeación.")
            else:
                agregar("planeacion", {
                    "identificacion": ident, "nombre": nombre, "celular": celular,
                    "zona": zona, "vehiculo": vehiculo, "analista": analista,
                    "estado_planeacion": "Nuevo", "categoria": "", "razon": "",
                    "intentos_llamada": 0, "intentos_sin_contacto": 0,
                    "ultimo_resultado": "", "proxima_gestion": date.today(),
                    "bloqueado": False, "fecha_ingreso": date.today(),
                    "ultima_gestion": "", "observaciones": "",
                })
                st.rerun()

    st.markdown("### 🔎 Buscar aliado para gestionar")
    busqueda = st.text_input("Cédula o celular", key="buscar_planeacion")

    if busqueda:
        df_plan = cargar("planeacion")
        coincidencias = df_plan[
            (df_plan.identificacion.astype(str) == busqueda.strip())
            | (df_plan.celular.astype(str).str.contains(busqueda.strip(), na=False))
        ]
        if coincidencias.empty:
            st.warning("No se encontró ningún aliado con ese dato. Regístralo arriba si es nuevo.")
        else:
            fila = coincidencias.iloc[0]
            st.markdown(f"""
- **Identificación:** {fila.identificacion}
- **Vehículo:** {fila.vehiculo}
- **Estado Aliado:** {fila.estado_planeacion or "Nuevo"}
- **Categoría:** {fila.categoria or "—"}
- **Nombre del mensajero:** {fila.nombre}
- **Celular:** {fila.celular}
- **Zona:** {fila.zona}
- **Intentos de llamada:** {a_entero(fila.intentos_llamada)}
- **Último resultado:** {fila.ultimo_resultado or "—"}
- **Último estado:** {fila.categoria or "—"}
- **Próxima gestión:** {fila.proxima_gestion or "—"}
""")

            if es_verdadero(fila.bloqueado):
                st.error("🚫 Este aliado está **bloqueado permanentemente** para Planeación. No se puede volver a gestionar aquí.")

            elif fila.estado_planeacion == "Validación pendiente":
                st.info("Este aliado dijo estar interesado en cargar. Confirma si efectivamente llegó a hacer el cargue.")
                with st.form("form_validacion_planeacion"):
                    cargo = st.radio("¿El aliado cargó?", ["Sí", "No"], horizontal=True)
                    nota = st.text_area("Observación", key="nota_validacion_plan")
                    enviar = st.form_submit_button("Guardar validación")
                if enviar:
                    actualizacion, log = procesar_validacion_planeacion(fila, cargo == "Sí", nota)
                    df_plan = aplicar_actualizacion(df_plan, "identificacion", fila.identificacion, actualizacion)
                    guardar("planeacion", df_plan)
                    agregar("planeacion_gestiones", log)
                    st.rerun()

            else:
                with st.form("form_gestion_planeacion"):
                    resultado = st.selectbox("Resultado de la llamada", RESULTADOS, key="res_plan")
                    estado_final, razon = "", ""
                    if resultado == "Sí contestó":
                        estado_final = st.selectbox("Categoría / Estado final", ESTADOS_FINALES_ALIADOS, key="estf_plan")
                        razon = st.selectbox("Razón", RAZONES, key="razon_plan")
                    nota = st.text_area("Observación", key="nota_plan")
                    enviar = st.form_submit_button("Guardar gestión")
                if enviar:
                    if resultado == "Sí contestó" and not estado_final:
                        st.error("Selecciona la categoría / estado final.")
                    else:
                        razon_final = "" if razon == "—" else razon
                        actualizacion, log = procesar_gestion_planeacion(fila, resultado, estado_final, razon_final, nota)
                        df_plan = aplicar_actualizacion(df_plan, "identificacion", fila.identificacion, actualizacion)
                        guardar("planeacion", df_plan)
                        agregar("planeacion_gestiones", log)
                        st.rerun()

    st.markdown("### 📋 Pendientes de gestión")
    df_plan = cargar("planeacion")
    if not df_plan.empty:
        analista_f = st.selectbox("Filtrar por analista", ["Todos"] + ANALISTAS, key="filtro_analista_plan")
        vista = df_plan[~df_plan.bloqueado.apply(es_verdadero)]
        if analista_f != "Todos":
            vista = vista[vista.analista == analista_f]
        st.dataframe(
            vista[["identificacion", "nombre", "celular", "zona", "analista", "estado_planeacion",
                   "categoria", "intentos_llamada", "proxima_gestion"]],
            hide_index=True, use_container_width=True,
        )

# -------------------------------------------------------------------------
# REQUERIMIENTOS SUPPLY
# -------------------------------------------------------------------------
with requerimientos_tab:
    df_req = cargar("requerimientos")

    with st.expander("➕ Registrar requerimiento", expanded=df_req.empty):
        with st.form("form_nuevo_requerimiento"):
            c1, c2, c3 = st.columns(3)
            numero = c1.text_input("Número de requerimiento *", key="r_numero")
            nombre = c2.text_input("Nombre *", key="r_nombre")
            telefono = c3.text_input("Teléfono *", key="r_telefono")
            vehiculo = c1.selectbox("Vehículo", VEHICULOS, key="r_vehiculo")
            rutas = c2.number_input("Cantidad de rutas", min_value=0, value=1, key="r_rutas")
            crear = st.form_submit_button("Guardar requerimiento")
        if crear:
            if not numero or not nombre or not telefono:
                st.error("Número de requerimiento, nombre y teléfono son obligatorios.")
            elif numero in df_req.numero_requerimiento.astype(str).tolist():
                st.error("Ya existe un requerimiento con ese número.")
            else:
                agregar("requerimientos", {
                    "numero_requerimiento": numero, "nombre": nombre, "telefono": telefono,
                    "vehiculo": vehiculo, "cantidad_rutas": rutas,
                    "estado_gestion": "Nuevo", "ultimo_estado": "",
                    "intentos_llamada": 0, "intentos_sin_contacto": 0,
                    "ultimo_resultado": "", "proxima_gestion": date.today(),
                    "bloqueado": False, "fecha_ingreso": date.today(),
                    "ultima_gestion": "", "observaciones": "",
                })
                st.rerun()

    st.markdown("### 🔎 Buscar requerimiento para gestionar")
    busqueda_r = st.text_input("Número de requerimiento o teléfono", key="buscar_requerimiento")

    if busqueda_r:
        df_req = cargar("requerimientos")
        coincidencias = df_req[
            (df_req.numero_requerimiento.astype(str) == busqueda_r.strip())
            | (df_req.telefono.astype(str).str.contains(busqueda_r.strip(), na=False))
        ]
        if coincidencias.empty:
            st.warning("No se encontró ningún requerimiento con ese dato.")
        else:
            fila = coincidencias.iloc[0]
            st.markdown(f"""
- **Número de requerimiento:** {fila.numero_requerimiento}
- **Nombre:** {fila.nombre}
- **Teléfono:** {fila.telefono}
- **Vehículo:** {fila.vehiculo}
- **Cantidad de rutas:** {fila.cantidad_rutas}
- **Estado de gestión:** {fila.estado_gestion or "Nuevo"}
- **Último estado:** {fila.ultimo_estado or "—"}
- **Intentos de llamada:** {a_entero(fila.intentos_llamada)}
- **Próxima gestión:** {fila.proxima_gestion or "—"}
""")

            if es_verdadero(fila.bloqueado):
                st.error("🚫 Este requerimiento está **bloqueado permanentemente**.")

            elif fila.estado_gestion == "Validación pendiente":
                st.info("El aliado dijo que iba a usar el cupo del requerimiento. Confirma si lo usó.")
                with st.form("form_validacion_requerimiento"):
                    uso = st.radio("¿El aliado usó el cupo / hizo el cargue?", ["Sí", "No"], horizontal=True)
                    nota = st.text_area("Observación", key="nota_validacion_req")
                    enviar = st.form_submit_button("Guardar validación")
                if enviar:
                    actualizacion, log = procesar_validacion_requerimiento(fila, uso == "Sí", nota)
                    df_req = aplicar_actualizacion(df_req, "numero_requerimiento", fila.numero_requerimiento, actualizacion)
                    guardar("requerimientos", df_req)
                    agregar("requerimientos_gestiones", log)
                    st.rerun()

            elif fila.estado_gestion == "Cerrado":
                st.success("Este requerimiento ya está cerrado. No requiere más gestión.")

            else:
                with st.form("form_gestion_requerimiento"):
                    resultado = st.selectbox("Resultado de la llamada", RESULTADOS, key="res_req")
                    estado_final = ""
                    if resultado == "Sí contestó":
                        estado_final = st.selectbox("Estado final", ESTADOS_FINALES_REQ, key="estf_req")
                    nota = st.text_area("Observación", key="nota_req")
                    enviar = st.form_submit_button("Guardar gestión")
                if enviar:
                    if resultado == "Sí contestó" and not estado_final:
                        st.error("Selecciona el estado final.")
                    else:
                        actualizacion, log = procesar_gestion_requerimiento(fila, resultado, estado_final, nota)
                        df_req = aplicar_actualizacion(df_req, "numero_requerimiento", fila.numero_requerimiento, actualizacion)
                        guardar("requerimientos", df_req)
                        agregar("requerimientos_gestiones", log)
                        st.rerun()

    st.markdown("### 📋 Requerimientos pendientes de gestión")
    df_req = cargar("requerimientos")
    if not df_req.empty:
        vista = df_req[~df_req.bloqueado.apply(es_verdadero) & (df_req.estado_gestion != "Cerrado")]
        st.dataframe(
            vista[["numero_requerimiento", "nombre", "telefono", "vehiculo", "cantidad_rutas",
                   "estado_gestion", "ultimo_estado", "intentos_llamada", "proxima_gestion"]],
            hide_index=True, use_container_width=True,
        )

# -------------------------------------------------------------------------
# COORDINADOR
# -------------------------------------------------------------------------
with coordinador_tab:
    st.markdown("### 📥 Cargar base de Implementación")
    archivo = st.file_uploader("Sube el archivo (Excel o CSV) que envía el área de Implementación", type=["xlsx", "xls", "csv"])

    if archivo is not None:
        try:
            nuevos = pd.read_csv(archivo) if archivo.name.lower().endswith("csv") else pd.read_excel(archivo)
            nuevos = mapear_columnas(nuevos, ALIAS_COORDINADOR)
            faltantes = {"nombre", "documento"} - set(nuevos.columns)
            if faltantes:
                st.error(f"El archivo no tiene las columnas mínimas requeridas: {', '.join(faltantes)}.")
            else:
                existente = cargar("coordinador")
                existentes_doc = set(existente.documento.astype(str))
                for _, fila_nueva in nuevos.iterrows():
                    doc = str(fila_nueva.get("documento", "")).strip()
                    if not doc:
                        continue
                    rutas_nuevas = a_entero(fila_nueva.get("rutas", 0))
                    estado_impl = str(fila_nueva.get("estado_implementacion", "") or "").strip()
                    if rutas_nuevas >= META and estado_impl not in {"Rechazado por Clicoh", "Deserta"}:
                        estado_impl = "Supera Implementacion"
                    datos = {
                        "nombre": fila_nueva.get("nombre", ""), "celular": fila_nueva.get("celular", ""),
                        "ciudad": fila_nueva.get("ciudad", ""), "vehiculo": fila_nueva.get("vehiculo", ""),
                        "rutas": rutas_nuevas, "estado_clicoh": fila_nueva.get("estado_clicoh", ""),
                        "estado_implementacion": estado_impl,
                        "fecha_ultimo_cargue": fila_nueva.get("fecha_ultimo_cargue", ""),
                    }
                    if doc in existentes_doc:
                        idx = existente[existente.documento.astype(str) == doc].index[0]
                        for campo, valor in datos.items():
                            existente.loc[idx, campo] = valor
                    else:
                        nueva_fila = {c: "" for c in HOJAS["coordinador"][1]}
                        nueva_fila.update({"documento": doc, **datos})
                        existente = pd.concat([existente, pd.DataFrame([nueva_fila])], ignore_index=True)
                        existentes_doc.add(doc)
                guardar("coordinador", existente)
                st.success(f"Base cargada: {len(nuevos)} registros procesados.")
                st.rerun()
        except Exception as e:
            st.error(f"No se pudo leer el archivo: {e}")

    st.markdown("### 📋 Seguimiento a la meta de 20 rutas")
    df_coord = cargar("coordinador")
    if df_coord.empty:
        st.info("Todavía no se ha cargado ninguna base.")
    else:
        vista_sel = st.radio("Vista", ["Gestión de hoy", "Histórico completo"], horizontal=True, key="vista_coord")
        if vista_sel == "Gestión de hoy":
            hoy_str = str(date.today())
            mostrar = df_coord[df_coord.ultima_gestion.astype(str).str.startswith(hoy_str)]
        else:
            mostrar = df_coord
        st.dataframe(
            mostrar[["documento", "nombre", "ciudad", "vehiculo", "rutas", "estado_clicoh",
                     "estado_implementacion", "fecha_ultimo_cargue", "proxima_gestion"]],
            hide_index=True, use_container_width=True,
        )

        st.markdown("#### ✏️ Actualizar un aliado")
        doc_sel = st.selectbox("Documento", df_coord.documento.astype(str).tolist(), key="doc_sel_coord")
        fila = df_coord[df_coord.documento.astype(str) == doc_sel].iloc[0]
        with st.form("form_actualizar_coordinador"):
            c1, c2 = st.columns(2)
            rutas_nueva = c1.number_input("Rutas actuales", min_value=0, value=a_entero(fila.rutas), key="rutas_coord")
            estado_nuevo = c2.selectbox(
                "Estado Implementación", ESTADO_IMPLEMENTACION,
                index=ESTADO_IMPLEMENTACION.index(fila.estado_implementacion) if fila.estado_implementacion in ESTADO_IMPLEMENTACION else 0,
                key="estado_coord",
            )
            nota = st.text_area("Observación", key="nota_coord")
            enviar = st.form_submit_button("Guardar gestión")
        if enviar:
            if rutas_nueva >= META and estado_nuevo not in {"Rechazado por Clicoh", "Deserta"}:
                estado_nuevo = "Supera Implementacion"
            df_coord = aplicar_actualizacion(df_coord, "documento", doc_sel, {
                "rutas": rutas_nueva, "estado_implementacion": estado_nuevo,
                "ultima_gestion": date.today(), "observaciones": nota or fila.observaciones,
            })
            guardar("coordinador", df_coord)
            agregar("coordinador_gestiones", {
                "fecha": datetime.now(), "documento": doc_sel, "nombre": fila.nombre,
                "estado_implementacion": estado_nuevo, "rutas": rutas_nueva, "observaciones": nota,
            })
            st.rerun()

# -------------------------------------------------------------------------
# KPIs
# -------------------------------------------------------------------------
with kpis_tab:
    st.markdown("""
**Gestión de Aliados (Planeación)**
- Aliados nuevos vs. en gestión vs. pausados vs. bloqueados permanentemente
- Tasa de rechazo (Aliado Rechaza la oferta / Point) sobre el total gestionado
- Tasa de conversión: aliados que llegan a "Completado" (validaron su primer cargue)
- Promedio de intentos de llamada antes de contacto efectivo

**Requerimientos Supply**
- Requerimientos cerrados/finalizados vs. abiertos vs. bloqueados
- Requerimientos pendientes de gestión de otra área
- Tiempo promedio entre solicitud y cierre

**Coordinador**
- Aliados que superaron la meta de 20 rutas ("Supera Implementacion")
- Aliados activos vs. inactivos en clicOH
- Aliados en estado "Deserta" / "No responde" / "Rechazado por Clicoh"
""")
