"""
Sistema de Registro y Legalización de Anticipos - Transporte de Carga
Colombia - Conectado a Supabase (PostgreSQL)
v3: pestaña clientes, lista desplegable clientes, conductor auto por placa
"""

import streamlit as st
import psycopg2
import pandas as pd
from datetime import datetime, timedelta

# ==================== CONFIGURACIÓN ====================
SUPABASE_DB_URL = "postgresql://postgres.ntnpckmbyfmjhfskfwyu:Conejito100#@aws-1-us-east-1.pooler.supabase.com:6543/postgres"  # <- Pega aquí tu URL de conexión de Supabase

# ==================== FORMATO COLOMBIANO ====================
def fmt(valor):
    if valor is None:
        return "0"
    try:
        return f"{int(float(valor)):,}".replace(',', '.')
    except:
        return str(valor)

def limpiar(texto):
    if not texto:
        return 0.0
    try:
        return float(str(texto).replace('.', '').replace(',', '.'))
    except:
        return 0.0

def hora_colombia():
    return datetime.utcnow() - timedelta(hours=5)

# ==================== PLACAS Y CONDUCTORES ====================
PLACAS = [
    "NOX459", "NOX460", "NOX461", "SON047", "SON048",
    "SOP148", "SOP149", "SOP150", "SRO661", "SRO672",
    "TMW882", "TRL282", "TRL298", "UYQ308", "UYV084",
    "UYY788", "PSX350"
]

PLACA_CONDUCTOR = {
    "NOX459": "GONZALO",
    "NOX460": "JOSE ORTEGA",
    "NOX461": "CARLOS TAFUR",
    "SON047": "ISAIAS VESGA",
    "SON048": "FLAVIO MALTE",
    "SOP148": "SHITH",
    "SOP149": "",
    "SOP150": "RAMON TAFUR",
    "SRO661": "JULIAN CALETH",
    "SRO672": "PEDRO JR",
    "TMW882": "REIMUR MANUEL",
    "TRL282": "CHRISTIAN MARTINEZ",
    "TRL298": "YEIMI DUQUE",
    "UYQ308": "",
    "UYV084": "",
    "UYY788": "EDUARDO OLIVARES",
    "PSX350": "EDGAR DE JESUS",
}

# Clientes predeterminados del sistema
CLIENTES_DEFAULT = [
    "GLOBO EXPRESS",
    "MOTOTRANSPORTAMOS",
    "CARGO ANDINA",
    "TRANSOLICAR",
    "SUCLOGISTIC",
]

# ==================== BASE DE DATOS ====================
class DB:
    def __init__(self):
        self.url = SUPABASE_DB_URL
        self._init_tablas()

    def conn(self):
        return psycopg2.connect(self.url)

    def _init_tablas(self):
        try:
            c = self.conn()
            cur = c.cursor()

            # Tabla principal de anticipos
            cur.execute("""
                CREATE TABLE IF NOT EXISTS anticipos_v1 (
                    id SERIAL PRIMARY KEY,
                    fecha_viaje DATE NOT NULL,
                    fecha_registro TIMESTAMP NOT NULL,
                    placa TEXT NOT NULL,
                    conductor TEXT NOT NULL,
                    cliente TEXT NOT NULL,
                    origen TEXT NOT NULL,
                    destino TEXT NOT NULL,
                    valor_anticipo BIGINT NOT NULL,
                    observaciones TEXT,
                    legalizado BOOLEAN DEFAULT FALSE,
                    fecha_legalizacion TIMESTAMP,
                    legalizado_por TEXT,
                    obs_legalizacion TEXT
                )
            """)
            cur.execute("""
                ALTER TABLE anticipos_v1
                ADD COLUMN IF NOT EXISTS manifiesto TEXT DEFAULT ''
            """)

            # Tabla de clientes adicionales
            cur.execute("""
                CREATE TABLE IF NOT EXISTS clientes_extra (
                    id SERIAL PRIMARY KEY,
                    nombre TEXT UNIQUE NOT NULL,
                    fecha_registro TIMESTAMP NOT NULL
                )
            """)

            c.commit()
            c.close()
        except Exception as e:
            st.error(f"Error inicializando tablas: {e}")

    # ---- Clientes ----
    def obtener_clientes_extra(self):
        try:
            c = self.conn()
            df = pd.read_sql_query(
                "SELECT * FROM clientes_extra ORDER BY nombre", c
            )
            c.close()
            return df
        except:
            return pd.DataFrame(columns=['id', 'nombre', 'fecha_registro'])

    def agregar_cliente(self, nombre):
        try:
            c = self.conn()
            cur = c.cursor()
            cur.execute(
                "INSERT INTO clientes_extra (nombre, fecha_registro) VALUES (%s, %s)",
                (nombre.strip().upper(), hora_colombia())
            )
            c.commit()
            c.close()
            return True
        except Exception as e:
            return False

    def eliminar_cliente(self, cliente_id):
        try:
            c = self.conn()
            cur = c.cursor()
            cur.execute("DELETE FROM clientes_extra WHERE id = %s", (cliente_id,))
            c.commit()
            c.close()
        except Exception as e:
            st.error(f"Error eliminando cliente: {e}")

    # ---- Viajes ----
    def registrar_viaje(self, data):
        try:
            c = self.conn()
            cur = c.cursor()
            cur.execute("""
                INSERT INTO anticipos_v1
                (fecha_viaje, fecha_registro, placa, conductor, cliente,
                 origen, destino, valor_anticipo, observaciones, manifiesto, legalizado)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE)
                RETURNING id
            """, (
                data['fecha_viaje'],
                hora_colombia(),
                data['placa'],
                data['conductor'],
                data['cliente'],
                data['origen'],
                data['destino'],
                int(data['valor_anticipo']),
                data.get('observaciones', ''),
                data.get('manifiesto', '').strip().upper()
            ))
            nuevo_id = cur.fetchone()[0]
            c.commit()
            c.close()
            return nuevo_id
        except Exception as e:
            st.error(f"Error guardando viaje: {e}")
            return None

    def editar_viaje(self, viaje_id, data):
        try:
            c = self.conn()
            cur = c.cursor()
            cur.execute("""
                UPDATE anticipos_v1 SET
                    fecha_viaje = %s, placa = %s, conductor = %s, cliente = %s,
                    origen = %s, destino = %s, valor_anticipo = %s,
                    observaciones = %s, manifiesto = %s
                WHERE id = %s
            """, (
                data['fecha_viaje'], data['placa'], data['conductor'], data['cliente'],
                data['origen'], data['destino'], int(data['valor_anticipo']),
                data.get('observaciones', ''), data.get('manifiesto', '').strip().upper(),
                viaje_id
            ))
            c.commit()
            c.close()
            return True
        except Exception as e:
            st.error(f"Error editando viaje: {e}")
            return False

    def legalizar(self, viaje_id, nombre_quien_legaliza, obs_legalizacion=""):
        try:
            c = self.conn()
            cur = c.cursor()
            cur.execute("""
                UPDATE anticipos_v1
                SET legalizado = TRUE, fecha_legalizacion = %s,
                    legalizado_por = %s, obs_legalizacion = %s
                WHERE id = %s
            """, (hora_colombia(), nombre_quien_legaliza, obs_legalizacion, viaje_id))
            c.commit()
            c.close()
            return True
        except Exception as e:
            st.error(f"Error legalizando: {e}")
            return False

    def buscar(self, estado=None, fecha_ini=None, fecha_fin=None,
               placa=None, conductor=None, manifiesto=None):
        try:
            c = self.conn()
            q = "SELECT * FROM anticipos_v1 WHERE 1=1"
            params = []
            if estado == "legalizado":
                q += " AND legalizado = TRUE"
            elif estado == "pendiente":
                q += " AND legalizado = FALSE"
            if fecha_ini:
                q += " AND fecha_viaje >= %s"
                params.append(fecha_ini)
            if fecha_fin:
                q += " AND fecha_viaje <= %s"
                params.append(fecha_fin)
            if placa:
                q += " AND placa = %s"
                params.append(placa)
            if conductor:
                q += " AND conductor ILIKE %s"
                params.append(f"%{conductor}%")
            if manifiesto:
                q += " AND manifiesto ILIKE %s"
                params.append(f"%{manifiesto}%")
            q += " ORDER BY fecha_registro DESC"
            df = pd.read_sql_query(q, c, params=params)
            c.close()
            return df
        except Exception as e:
            st.error(f"Error buscando: {e}")
            return pd.DataFrame()

    def eliminar(self, viaje_id):
        try:
            c = self.conn()
            cur = c.cursor()
            cur.execute("DELETE FROM anticipos_v1 WHERE id = %s", (viaje_id,))
            c.commit()
            c.close()
        except Exception as e:
            st.error(f"Error eliminando: {e}")

    def obtener_por_id(self, viaje_id):
        try:
            c = self.conn()
            df = pd.read_sql_query(
                "SELECT * FROM anticipos_v1 WHERE id = %s", c, params=(viaje_id,)
            )
            c.close()
            return df.iloc[0] if not df.empty else None
        except:
            return None


# ==================== HELPER: lista completa de clientes ====================
def get_lista_clientes(db):
    """Combina clientes default + clientes extra de la BD, ordenados"""
    extras_df = db.obtener_clientes_extra()
    extras = extras_df['nombre'].tolist() if not extras_df.empty else []
    todos = sorted(set(CLIENTES_DEFAULT + extras))
    return todos


# ==================== APP PRINCIPAL ====================
def main():
    st.set_page_config(
        page_title="Anticipos - Transporte de Carga",
        layout="wide",
        page_icon="🚛"
    )

    st.title("🚛 Gestión de Anticipos - Transporte de Carga")

    if 'db' not in st.session_state:
        st.session_state.db = DB()
    if 'confirmar_eliminar' not in st.session_state:
        st.session_state.confirmar_eliminar = None
    if 'editando_id' not in st.session_state:
        st.session_state.editando_id = None
    if 'confirmar_eliminar_cliente' not in st.session_state:
        st.session_state.confirmar_eliminar_cliente = None

    db = st.session_state.db

    tab_reg, tab_leg, tab_hist, tab_clientes = st.tabs([
        "📝 Registrar Viaje",
        "✅ Legalizar Anticipos",
        "📋 Historial",
        "🏢 Clientes"
    ])

    # ==================== TAB 1: REGISTRAR ====================
    with tab_reg:
        st.header("Registrar nuevo viaje con anticipo")

        lista_clientes = get_lista_clientes(db)

        # ── Placa y conductor FUERA del form para actualización en tiempo real ──
        col_pre1, col_pre2 = st.columns(2)
        with col_pre1:
            placa_sel = st.selectbox(
                "Placa de la tractomula",
                PLACAS,
                key="reg_placa"
            )
        with col_pre2:
            conductor_mostrado = PLACA_CONDUCTOR.get(placa_sel, "")
            st.text_input(
                "Conductor (automático)",
                value=conductor_mostrado if conductor_mostrado else "Sin conductor asignado",
                disabled=True,
                key="reg_conductor_display"
            )

        st.divider()

        with st.form("form_registro", clear_on_submit=True):
            col1, col2 = st.columns(2)

            with col1:
                fecha_viaje = st.date_input("Fecha del viaje", value=datetime.today())

                cliente = st.selectbox(
                    "Cliente",
                    lista_clientes,
                    help="Si no aparece el cliente, agrégalo en la pestaña 🏢 Clientes"
                )

                manifiesto = st.text_input(
                    "Número de manifiesto ✱",
                    placeholder="Ej: 1234567",
                    help="Campo obligatorio"
                )

            with col2:
                origen = st.text_input("Origen", placeholder="Ciudad de origen")
                destino = st.text_input("Destino", placeholder="Ciudad de destino")
                anticipo_txt = st.text_input(
                    "Valor del anticipo (COP)",
                    placeholder="Ejemplo: 1.500.000"
                )
                anticipo = limpiar(anticipo_txt)
                if anticipo > 0:
                    st.caption(f"💵 {fmt(anticipo)} COP")
                observaciones = st.text_area(
                    "Observaciones",
                    placeholder="Notas adicionales del viaje...",
                    height=80
                )

            submitted = st.form_submit_button("💾 Registrar Viaje", type="primary")

            if submitted:
                # Leer placa y conductor del session_state (definidos fuera del form)
                placa       = st.session_state.get("reg_placa", PLACAS[0])
                conductor_final = PLACA_CONDUCTOR.get(placa, "")
                errores = []
                if not manifiesto.strip():
                    errores.append("El número de manifiesto es obligatorio")
                if not origen.strip():
                    errores.append("Origen es obligatorio")
                if not destino.strip():
                    errores.append("Destino es obligatorio")
                if anticipo <= 0:
                    errores.append("El valor del anticipo debe ser mayor a 0")

                if errores:
                    for e in errores:
                        st.error(f"⚠️ {e}")
                else:
                    nuevo_id = db.registrar_viaje({
                        'fecha_viaje': fecha_viaje,
                        'placa': placa,
                        'conductor': conductor_final.strip().upper(),
                        'cliente': cliente.strip().upper(),
                        'origen': origen.strip().upper(),
                        'destino': destino.strip().upper(),
                        'valor_anticipo': anticipo,
                        'observaciones': observaciones.strip(),
                        'manifiesto': manifiesto.strip()
                    })
                    if nuevo_id:
                        st.success(f"""
✅ **Viaje registrado exitosamente (ID: {nuevo_id})**

- Manifiesto: **{manifiesto.strip().upper()}**
- Placa: {placa} | Conductor: {conductor_final.upper() or '(sin asignar)'}
- Ruta: {origen.upper()} → {destino.upper()}
- Cliente: {cliente.upper()}
- Anticipo: **${fmt(anticipo)} COP**
- Estado: 🔴 Pendiente de legalización
                        """)

    # ==================== TAB 2: LEGALIZAR ====================
    with tab_leg:
        st.header("Legalizar anticipos pendientes")
        st.info("Solo los viajes en estado **Pendiente** aparecen aquí para legalizar.")

        col_f1, col_f2, col_f3, col_f4 = st.columns(4)
        with col_f1:
            fecha_ini_leg = st.date_input("Desde", value=None, key="leg_fi")
        with col_f2:
            fecha_fin_leg = st.date_input("Hasta", value=None, key="leg_ff")
        with col_f3:
            placa_leg = st.selectbox("Placa", ["Todas"] + PLACAS, key="leg_placa")
        with col_f4:
            manifiesto_leg = st.text_input(
                "Buscar por manifiesto", placeholder="Nº manifiesto...", key="leg_manif"
            )

        fi  = fecha_ini_leg.strftime('%Y-%m-%d') if fecha_ini_leg else None
        ff  = fecha_fin_leg.strftime('%Y-%m-%d') if fecha_fin_leg else None
        pl  = None if placa_leg == "Todas" else placa_leg
        mf  = manifiesto_leg.strip() if manifiesto_leg else None

        df_pendientes = db.buscar(
            estado="pendiente", fecha_ini=fi, fecha_fin=ff, placa=pl, manifiesto=mf
        )

        if df_pendientes.empty:
            st.success("✅ No hay anticipos pendientes de legalización.")
        else:
            st.warning(f"🔴 {len(df_pendientes)} viaje(s) pendiente(s) de legalización")

            for _, row in df_pendientes.iterrows():
                manif_label = f"Manif: {row.get('manifiesto','—')} | " if row.get('manifiesto') else ""
                with st.expander(
                    f"ID {row['id']} | {manif_label}{row['fecha_viaje']} | "
                    f"{row['placa']} | {row['conductor']} | "
                    f"{row['origen']} → {row['destino']} | ${fmt(row['valor_anticipo'])} COP"
                ):
                    col_info, col_form = st.columns([2, 2])

                    with col_info:
                        st.markdown("**Datos del viaje:**")
                        st.write(f"📄 Manifiesto: **{row.get('manifiesto', '—')}**")
                        st.write(f"📅 Fecha: {row['fecha_viaje']}")
                        st.write(f"🚛 Placa: {row['placa']}")
                        st.write(f"👤 Conductor: {row['conductor']}")
                        st.write(f"🏢 Cliente: {row['cliente']}")
                        st.write(f"📍 Ruta: {row['origen']} → {row['destino']}")
                        st.write(f"💰 Anticipo: **${fmt(row['valor_anticipo'])} COP**")
                        if row.get('observaciones'):
                            st.write(f"📝 Obs: {row['observaciones']}")

                    with col_form:
                        st.markdown("**Legalizar este viaje:**")
                        nombre_leg = st.text_input(
                            "Tu nombre completo (obligatorio)",
                            placeholder="Escribe tu nombre para legalizar",
                            key=f"nombre_leg_{row['id']}"
                        )
                        obs_leg = st.text_area(
                            "Observaciones de legalización (opcional)",
                            placeholder="Notas sobre la legalización...",
                            height=80,
                            key=f"obs_leg_{row['id']}"
                        )
                        if st.button(
                            "✅ Marcar como LEGALIZADO",
                            key=f"btn_leg_{row['id']}",
                            type="primary"
                        ):
                            if not nombre_leg.strip():
                                st.error("⚠️ Debes escribir tu nombre para poder legalizar.")
                            else:
                                ok = db.legalizar(
                                    row['id'],
                                    nombre_leg.strip().upper(),
                                    obs_leg.strip()
                                )
                                if ok:
                                    st.success(
                                        f"✅ Viaje ID {row['id']} | Manifiesto {row.get('manifiesto','—')} "
                                        f"legalizado por **{nombre_leg.upper()}** a las "
                                        f"{hora_colombia().strftime('%H:%M')} (hora Colombia)"
                                    )
                                    st.rerun()

    # ==================== TAB 3: HISTORIAL ====================
    with tab_hist:
        st.header("Historial de viajes")

        col1, col2, col3 = st.columns(3)
        with col1:
            estado_filtro = st.selectbox(
                "Estado", ["Todos", "Pendientes", "Legalizados"], key="hist_estado"
            )
        with col2:
            fecha_ini_h = st.date_input("Desde", value=None, key="hist_fi")
        with col3:
            fecha_fin_h = st.date_input("Hasta", value=None, key="hist_ff")

        col4, col5, col6 = st.columns(3)
        with col4:
            placa_h = st.selectbox("Placa", ["Todas"] + PLACAS, key="hist_placa")
        with col5:
            conductor_h = st.text_input(
                "Buscar conductor", placeholder="Nombre parcial...", key="hist_cond"
            )
        with col6:
            manifiesto_h = st.text_input(
                "Buscar por manifiesto", placeholder="Nº manifiesto...", key="hist_manif"
            )

        estado_map = {"Todos": None, "Pendientes": "pendiente", "Legalizados": "legalizado"}
        fi_h  = fecha_ini_h.strftime('%Y-%m-%d') if fecha_ini_h else None
        ff_h  = fecha_fin_h.strftime('%Y-%m-%d') if fecha_fin_h else None
        pl_h  = None if placa_h == "Todas" else placa_h
        cond_h = conductor_h if conductor_h else None
        mf_h  = manifiesto_h.strip() if manifiesto_h else None

        df_hist = db.buscar(
            estado=estado_map[estado_filtro],
            fecha_ini=fi_h, fecha_fin=ff_h,
            placa=pl_h, conductor=cond_h, manifiesto=mf_h
        )

        if df_hist.empty:
            st.info("No se encontraron viajes con los filtros aplicados.")
        else:
            total_anticipo = df_hist['valor_anticipo'].sum()
            legalizados    = int(df_hist['legalizado'].sum())
            pendientes     = len(df_hist) - legalizados

            col_m1, col_m2, col_m3, col_m4 = st.columns(4)
            col_m1.metric("Total viajes", len(df_hist))
            col_m2.metric("Legalizados", legalizados)
            col_m3.metric("Pendientes", pendientes)
            col_m4.metric("Total anticipos", f"${fmt(total_anticipo)}")

            cols_tabla = [
                'id', 'manifiesto', 'fecha_viaje', 'placa', 'conductor', 'cliente',
                'origen', 'destino', 'valor_anticipo', 'legalizado',
                'legalizado_por', 'fecha_legalizacion'
            ]
            cols_ok = [c for c in cols_tabla if c in df_hist.columns]
            df_show = df_hist[cols_ok].copy()
            df_show['valor_anticipo'] = df_show['valor_anticipo'].apply(lambda x: f"${fmt(x)}")
            df_show['legalizado'] = df_show['legalizado'].apply(
                lambda x: "✅ Legalizado" if x else "🔴 Pendiente"
            )
            rename_map = {
                'id': 'ID', 'manifiesto': 'Manifiesto', 'fecha_viaje': 'Fecha viaje',
                'placa': 'Placa', 'conductor': 'Conductor', 'cliente': 'Cliente',
                'origen': 'Origen', 'destino': 'Destino', 'valor_anticipo': 'Anticipo',
                'legalizado': 'Estado', 'legalizado_por': 'Legalizado por',
                'fecha_legalizacion': 'Fecha legalización'
            }
            df_show.rename(columns=rename_map, inplace=True)
            st.dataframe(df_show, use_container_width=True, hide_index=True, height=350)

            st.divider()
            st.subheader("Acciones sobre un viaje")

            viaje_sel = st.selectbox(
                "Selecciona un viaje por ID",
                df_hist['id'].tolist(),
                format_func=lambda x: (
                    f"ID {x} | "
                    f"Manif: {df_hist[df_hist['id']==x]['manifiesto'].values[0] or '—'} | "
                    f"{df_hist[df_hist['id']==x]['placa'].values[0]} | "
                    f"{df_hist[df_hist['id']==x]['conductor'].values[0]}"
                ),
                key="hist_sel"
            )

            row_sel = df_hist[df_hist['id'] == viaje_sel].iloc[0]

            col_det, col_acc = st.columns([3, 1])
            with col_det:
                estado_tag = "✅ **LEGALIZADO**" if row_sel['legalizado'] else "🔴 **PENDIENTE**"
                st.markdown(f"**Estado:** {estado_tag}")
                st.write(f"📄 Manifiesto: **{row_sel.get('manifiesto', '—')}**")
                st.write(f"Fecha viaje: {row_sel['fecha_viaje']}")
                st.write(f"Placa: {row_sel['placa']} | Conductor: {row_sel['conductor']}")
                st.write(f"Cliente: {row_sel['cliente']}")
                st.write(f"Ruta: {row_sel['origen']} → {row_sel['destino']}")
                st.write(f"Anticipo: **${fmt(row_sel['valor_anticipo'])} COP**")
                if row_sel.get('observaciones'):
                    st.write(f"Observaciones: {row_sel['observaciones']}")
                if row_sel['legalizado']:
                    st.success(
                        f"Legalizado por: **{row_sel['legalizado_por']}** | "
                        f"Fecha: {row_sel['fecha_legalizacion']}"
                    )
                    if row_sel.get('obs_legalizacion'):
                        st.write(f"Obs legalización: {row_sel['obs_legalizacion']}")

            with col_acc:
                st.markdown("&nbsp;")
                if st.button("✏️ Editar viaje", key="btn_editar"):
                    st.session_state.editando_id = viaje_sel
                    st.rerun()

                st.markdown("&nbsp;")

                if st.session_state.confirmar_eliminar == viaje_sel:
                    st.warning(f"¿Seguro que quieres eliminar el viaje ID **{viaje_sel}**?")
                    col_si, col_no = st.columns(2)
                    with col_si:
                        if st.button("Sí, eliminar", key="btn_si_eliminar", type="primary"):
                            db.eliminar(viaje_sel)
                            st.session_state.confirmar_eliminar = None
                            st.success(f"Viaje ID {viaje_sel} eliminado.")
                            st.rerun()
                    with col_no:
                        if st.button("Cancelar", key="btn_no_eliminar"):
                            st.session_state.confirmar_eliminar = None
                            st.rerun()
                else:
                    if st.button("🗑️ Eliminar viaje", key="btn_eliminar", type="secondary"):
                        st.session_state.confirmar_eliminar = viaje_sel
                        st.rerun()

        # ---- Formulario de edición ----
        if st.session_state.editando_id is not None:
            eid = st.session_state.editando_id
            viaje_edit = db.obtener_por_id(eid)

            if viaje_edit is not None:
                st.divider()
                st.subheader(
                    f"✏️ Editando viaje ID {eid} | "
                    f"Manifiesto: {viaje_edit.get('manifiesto','—')}"
                )

                lista_clientes_edit = get_lista_clientes(db)
                cliente_actual = viaje_edit['cliente']
                if cliente_actual not in lista_clientes_edit:
                    lista_clientes_edit = [cliente_actual] + lista_clientes_edit
                idx_cliente = lista_clientes_edit.index(cliente_actual)

                with st.form(f"form_editar_{eid}"):
                    col1, col2 = st.columns(2)

                    with col1:
                        fecha_e = st.date_input(
                            "Fecha del viaje",
                            value=pd.to_datetime(viaje_edit['fecha_viaje']).date()
                        )
                        idx_placa = PLACAS.index(viaje_edit['placa']) if viaje_edit['placa'] in PLACAS else 0
                        placa_e = st.selectbox("Placa", PLACAS, index=idx_placa)
                        conductor_e_val = PLACA_CONDUCTOR.get(placa_e, viaje_edit['conductor'])
                        st.text_input(
                            "Conductor (automático)",
                            value=conductor_e_val,
                            disabled=True
                        )
                        cliente_e = st.selectbox(
                            "Cliente", lista_clientes_edit, index=idx_cliente
                        )
                        manifiesto_e = st.text_input(
                            "Número de manifiesto ✱",
                            value=viaje_edit.get('manifiesto', '') or '',
                            help="Obligatorio"
                        )

                    with col2:
                        origen_e = st.text_input("Origen", value=viaje_edit['origen'])
                        destino_e = st.text_input("Destino", value=viaje_edit['destino'])
                        anticipo_e_txt = st.text_input(
                            "Valor del anticipo (COP)",
                            value=fmt(viaje_edit['valor_anticipo'])
                        )
                        anticipo_e = limpiar(anticipo_e_txt)
                        if anticipo_e > 0:
                            st.caption(f"💵 {fmt(anticipo_e)} COP")
                        obs_e = st.text_area(
                            "Observaciones",
                            value=viaje_edit.get('observaciones', '') or '',
                            height=80
                        )

                    col_g, col_c = st.columns(2)
                    with col_g:
                        guardar_edit = st.form_submit_button("💾 Guardar cambios", type="primary")
                    with col_c:
                        cancelar_edit = st.form_submit_button("✖ Cancelar")

                    if guardar_edit:
                        conductor_edit_final = PLACA_CONDUCTOR.get(placa_e, viaje_edit['conductor'])
                        errores_e = []
                        if not manifiesto_e.strip():
                            errores_e.append("El número de manifiesto es obligatorio")
                        if not origen_e.strip():
                            errores_e.append("Origen es obligatorio")
                        if not destino_e.strip():
                            errores_e.append("Destino es obligatorio")
                        if anticipo_e <= 0:
                            errores_e.append("El valor del anticipo debe ser mayor a 0")

                        if errores_e:
                            for err in errores_e:
                                st.error(f"⚠️ {err}")
                        else:
                            ok = db.editar_viaje(eid, {
                                'fecha_viaje': fecha_e,
                                'placa': placa_e,
                                'conductor': conductor_edit_final.strip().upper(),
                                'cliente': cliente_e.strip().upper(),
                                'origen': origen_e.strip().upper(),
                                'destino': destino_e.strip().upper(),
                                'valor_anticipo': anticipo_e,
                                'observaciones': obs_e.strip(),
                                'manifiesto': manifiesto_e.strip()
                            })
                            if ok:
                                st.success(f"✅ Viaje ID {eid} actualizado correctamente.")
                                st.session_state.editando_id = None
                                st.rerun()

                    if cancelar_edit:
                        st.session_state.editando_id = None
                        st.rerun()

    # ==================== TAB 4: CLIENTES ====================
    with tab_clientes:
        st.header("🏢 Gestión de Clientes")
        st.markdown(
            "Agrega aquí los clientes adicionales. "
            "Los clientes predeterminados siempre estarán disponibles."
        )

        # Clientes predeterminados (solo lectura)
        st.subheader("Clientes predeterminados")
        cols = st.columns(len(CLIENTES_DEFAULT))
        for i, c_def in enumerate(CLIENTES_DEFAULT):
            with cols[i]:
                st.info(c_def)

        st.divider()

        # Agregar cliente nuevo
        st.subheader("Agregar cliente nuevo")
        with st.form("form_nuevo_cliente", clear_on_submit=True):
            nuevo_cliente = st.text_input(
                "Nombre del cliente",
                placeholder="Ej: LOGÍSTICA DEL NORTE"
            )
            agregar_btn = st.form_submit_button("➕ Agregar Cliente", type="primary")

            if agregar_btn:
                if not nuevo_cliente.strip():
                    st.error("⚠️ El nombre del cliente no puede estar vacío.")
                elif nuevo_cliente.strip().upper() in [c.upper() for c in CLIENTES_DEFAULT]:
                    st.warning("⚠️ Ese cliente ya existe en la lista predeterminada.")
                else:
                    ok = db.agregar_cliente(nuevo_cliente.strip())
                    if ok:
                        st.success(f"✅ Cliente **{nuevo_cliente.strip().upper()}** agregado correctamente.")
                        st.rerun()
                    else:
                        st.error("❌ Ese cliente ya existe o hubo un error al guardarlo.")

        st.divider()

        # Clientes adicionales guardados
        st.subheader("Clientes adicionales registrados")
        df_extras = db.obtener_clientes_extra()

        if df_extras.empty:
            st.info("No hay clientes adicionales registrados aún.")
        else:
            for _, row in df_extras.iterrows():
                col_nombre, col_fecha, col_btn = st.columns([3, 2, 1])
                with col_nombre:
                    st.write(f"**{row['nombre']}**")
                with col_fecha:
                    st.write(f"Registrado: {str(row['fecha_registro'])[:16]}")
                with col_btn:
                    if st.session_state.confirmar_eliminar_cliente == row['id']:
                        st.warning("¿Eliminar?")
                        c_si, c_no = st.columns(2)
                        with c_si:
                            if st.button("Sí", key=f"si_cli_{row['id']}"):
                                db.eliminar_cliente(row['id'])
                                st.session_state.confirmar_eliminar_cliente = None
                                st.success(f"Cliente eliminado.")
                                st.rerun()
                        with c_no:
                            if st.button("No", key=f"no_cli_{row['id']}"):
                                st.session_state.confirmar_eliminar_cliente = None
                                st.rerun()
                    else:
                        if st.button("🗑️", key=f"del_cli_{row['id']}"):
                            st.session_state.confirmar_eliminar_cliente = row['id']
                            st.rerun()

        st.divider()
        st.subheader("Lista completa disponible en el sistema")
        todos = get_lista_clientes(db)
        for c in todos:
            st.write(f"• {c}")


if __name__ == "__main__":
    main()
