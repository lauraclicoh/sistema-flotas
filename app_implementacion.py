import streamlit as st
import pandas as pd
import gspread
from datetime import date, datetime, timedelta
from google.oauth2.service_account import Credentials

st.set_page_config("Implementación de Aliados", "🚚", layout="wide")
SHEET_ID = "1ySPooqSBmL3yJTyPONwdM7__RYwunge669xQg7Ngrao"
META = 20
ANALISTAS = ["Deisy Liliana Garcia", "Erica Tatiana Garzon", "Dayan Stefany Suarez", "Carlos Andres Loaiza", "Diana Paola Rueda Jimenez"]
ESTADOS = ["Nuevo", "Contactado", "Documentación", "Activo", "En seguimiento", "Completado", "Descartado"]
ESTADOS_SUPPLY = ["Solicitado", "En búsqueda", "Recibido", "Validado", "Cerrado", "No disponible"]
HOJAS = {
    "aliados": ("IMPLEMENTACION_ALIADOS", ["id_aliado","nombre","celular","ciudad","zona","vehiculo","analista","estado","cargues_actuales","meta_cargues","fecha_asignacion","proxima_accion","ultima_gestion","observaciones"]),
    "gestiones": ("IMPLEMENTACION_GESTIONES", ["fecha","id_aliado","analista","tipo","resultado","cargues_reportados","proxima_accion","observaciones"]),
    "supply": ("REQUERIMIENTOS_SUPPLY", ["id_requerimiento","fecha_solicitud","ciudad","zona","vehiculo","cantidad_solicitada","cantidad_recibida","responsable_supply","estado","fecha_compromiso","analista_solicitante","observaciones"]),
}

@st.cache_resource
def libro():
    try:
        creds = Credentials.from_service_account_info(dict(st.secrets["gcp_service_account"]), scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        return gspread.authorize(creds).open_by_key(st.secrets.get("implementation_sheet_id", SHEET_ID))
    except Exception as e:
        st.error(f"Error conectando con Google Sheets: {e}")
        st.stop()

def hoja(clave):
    nombre, columnas = HOJAS[clave]
    try:
        ws = libro().worksheet(nombre)
    except gspread.WorksheetNotFound:
        ws = libro().add_worksheet(nombre, rows=1000, cols=len(columnas) + 2)
        ws.append_row(columnas)
    if not ws.row_values(1): ws.append_row(columnas)
    return ws, columnas

def cargar(clave):
    ws, columnas = hoja(clave)
    df = pd.DataFrame(ws.get_all_records(default_blank=""))
    for c in columnas:
        if c not in df: df[c] = ""
    return df[columnas].fillna("")

def guardar(clave, df):
    ws, columnas = hoja(clave)
    limpio = df.reindex(columns=columnas).fillna("").astype(str)
    ws.clear(); ws.update([columnas] + limpio.values.tolist())

def agregar(clave, fila):
    ws, columnas = hoja(clave)
    ws.append_row([str(fila.get(c, "")) for c in columnas], value_input_option="USER_ENTERED")

def preparar(df):
    if df.empty: return df
    df = df.copy()
    df["cargues_actuales"] = pd.to_numeric(df.cargues_actuales, errors="coerce").fillna(0).astype(int)
    df["meta_cargues"] = pd.to_numeric(df.meta_cargues, errors="coerce").fillna(META).astype(int)
    df["pendientes"] = (df.meta_cargues - df.cargues_actuales).clip(lower=0)
    df["avance"] = (100 * df.cargues_actuales / df.meta_cargues).clip(upper=100).round(0).astype(int)
    df["proxima_dt"] = pd.to_datetime(df.proxima_accion, errors="coerce")
    df["vencido"] = (df.proxima_dt.notna()) & (df.proxima_dt.dt.date < date.today()) & (df.pendientes > 0)
    return df

st.title("🚚 Implementación de Aliados")
st.caption("Seguimiento desde el cargue inicial hasta la meta mínima de 20, y requerimientos a Supply por ciudad.")
aliados = cargar("aliados")
supply = cargar("supply")
tablero, gestion, supply_tab, kpis = st.tabs(["📊 Tablero", "👥 Aliados", "📨 Supply", "📘 KPIs"])

with tablero:
    if aliados.empty:
        st.info("Registra el primer aliado en la pestaña Aliados.")
    else:
        a = preparar(aliados); activos = a[~a.estado.isin(["Completado", "Descartado"])]
        meta, logrados = int(a.meta_cargues.sum()), int(a.cargues_actuales.sum())
        c1,c2,c3,c4,c5 = st.columns(5)
        c1.metric("En implementación", len(activos)); c2.metric("Meta cumplida", int((a.pendientes == 0).sum()))
        c3.metric("Cargues logrados", logrados); c4.metric("Cargues faltantes", int(a.pendientes.sum())); c5.metric("Acciones vencidas", int(a.vencido.sum()))
        st.progress(logrados / meta if meta else 0, text=f"Avance global: {logrados}/{meta} cargues")
        izq, der = st.columns(2)
        with izq:
            ciudad = a.groupby("ciudad").agg(Cargues=("cargues_actuales","sum"), Pendientes=("pendientes","sum"))
            st.subheader("Brecha por ciudad"); st.bar_chart(ciudad)
        with der:
            st.subheader("Atención prioritaria")
            st.dataframe(a[(a.vencido) | (a.cargues_actuales < 7)][["nombre","ciudad","analista","cargues_actuales","pendientes","proxima_accion"]], hide_index=True, use_container_width=True)
    if not supply.empty:
        s=supply.copy(); s["solicitados"]=pd.to_numeric(s.cantidad_solicitada,errors="coerce").fillna(0); s["recibidos"]=pd.to_numeric(s.cantidad_recibida,errors="coerce").fillna(0); s["faltantes"]=(s.solicitados-s.recibidos).clip(lower=0)
        st.subheader("Requerimientos abiertos a Supply")
        st.dataframe(s[~s.estado.isin(["Cerrado","No disponible"])][["id_requerimiento","ciudad","vehiculo","solicitados","recibidos","faltantes","estado","fecha_compromiso"]], hide_index=True, use_container_width=True)

with gestion:
    with st.expander("➕ Registrar aliado", expanded=aliados.empty):
        with st.form("nuevo"):
            c1,c2,c3=st.columns(3)
            ident=c1.text_input("ID / cédula *"); nombre=c2.text_input("Nombre *"); celular=c3.text_input("Celular")
            ciudad=c1.text_input("Ciudad *"); zona=c2.text_input("Zona / HUB"); vehiculo=c3.selectbox("Vehículo",["Moto","Carry / Van","Camión","Otro"])
            analista=c1.selectbox("Analista",ANALISTAS); cargues=c2.number_input("Cargues iniciales",0,value=7); proxima=c3.date_input("Próxima acción",date.today()+timedelta(days=2))
            nota=st.text_area("Observaciones"); crear=st.form_submit_button("Guardar aliado")
        if crear:
            if not ident or not nombre or not ciudad: st.error("ID, nombre y ciudad son obligatorios.")
            elif ident in aliados.id_aliado.astype(str).tolist(): st.error("Ese aliado ya existe.")
            else:
                agregar("aliados", {"id_aliado":ident,"nombre":nombre,"celular":celular,"ciudad":ciudad,"zona":zona,"vehiculo":vehiculo,"analista":analista,"estado":"En seguimiento","cargues_actuales":cargues,"meta_cargues":META,"fecha_asignacion":date.today(),"proxima_accion":proxima,"ultima_gestion":date.today(),"observaciones":nota}); st.rerun()
    if not aliados.empty:
        a=preparar(aliados); ciudad_f=st.selectbox("Filtrar ciudad",["Todas"]+sorted(a.ciudad.unique().tolist()))
        vista=a if ciudad_f=="Todas" else a[a.ciudad==ciudad_f]
        st.dataframe(vista[["id_aliado","nombre","ciudad","analista","estado","cargues_actuales","meta_cargues","pendientes","avance","proxima_accion"]],hide_index=True,use_container_width=True)
        elegido=st.selectbox("Actualizar aliado",vista.id_aliado.astype(str).tolist())
        fila=a[a.id_aliado.astype(str)==elegido].iloc[0]
        with st.form("actualizar"):
            c1,c2,c3=st.columns(3); nuevos=c1.number_input("Cargues acumulados",0,value=int(fila.cargues_actuales)); estado=c2.selectbox("Estado",ESTADOS,index=ESTADOS.index(fila.estado) if fila.estado in ESTADOS else 0); proxima=c3.date_input("Próxima acción",fila.proxima_dt.date() if pd.notna(fila.proxima_dt) else date.today()+timedelta(days=2)); nota=st.text_area("Nota de gestión"); actualizar=st.form_submit_button("Guardar actualización")
        if actualizar:
            idx=aliados[aliados.id_aliado.astype(str)==elegido].index[0]; aliados.loc[idx,["cargues_actuales","estado","proxima_accion","ultima_gestion","observaciones"]]=[nuevos,"Completado" if nuevos>=int(fila.meta_cargues) else estado,proxima,date.today(),nota or fila.observaciones]; guardar("aliados",aliados); agregar("gestiones",{"fecha":datetime.now(),"id_aliado":elegido,"analista":fila.analista,"tipo":"Seguimiento","resultado":aliados.loc[idx,"estado"],"cargues_reportados":nuevos,"proxima_accion":proxima,"observaciones":nota}); st.rerun()

with supply_tab:
    with st.form("solicitud"):
        c1,c2,c3=st.columns(3); ciudad=c1.text_input("Ciudad *",key="sc"); zona=c2.text_input("Zona / HUB",key="sz"); vehiculo=c3.selectbox("Vehículo",["Moto","Carry / Van","Camión","Otro"],key="sv"); cantidad=c1.number_input("Aliados requeridos",1,value=1); compromiso=c2.date_input("Fecha compromiso",date.today()+timedelta(days=7)); analista=c3.selectbox("Solicita",ANALISTAS,key="sa"); nota=st.text_area("Contexto"); enviar=st.form_submit_button("Registrar requerimiento")
    if enviar:
        if not ciudad: st.error("La ciudad es obligatoria.")
        else: agregar("supply",{"id_requerimiento":f"SUP-{date.today():%Y%m%d}-{len(supply)+1:03d}","fecha_solicitud":date.today(),"ciudad":ciudad,"zona":zona,"vehiculo":vehiculo,"cantidad_solicitada":cantidad,"cantidad_recibida":0,"responsable_supply":"","estado":"Solicitado","fecha_compromiso":compromiso,"analista_solicitante":analista,"observaciones":nota}); st.rerun()
    if not supply.empty: st.dataframe(supply,hide_index=True,use_container_width=True)

with kpis:
    st.markdown("**KPIs:** avance contra meta de 20 cargues, aliados bajo 7 cargues, tasa de finalización, días a meta, cargues faltantes por ciudad, requerimientos vencidos, cumplimiento de Supply y productividad por analista.")
