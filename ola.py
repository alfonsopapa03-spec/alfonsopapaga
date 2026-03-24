"""
Reporte de Gastos - Conductor / Tractomula
Sistema de Gestión de Transporte de Carga - Colombia
Versión 1.0
"""

import streamlit as st
from datetime import datetime
import io
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
import psycopg2
import pandas as pd

# ==================== CONFIGURACIÓN ====================
SUPABASE_DB_URL = "postgresql://postgres.verwlkgitpllyneqxlao:Conejito800$@aws-0-us-west-2.pooler.supabase.com:6543/postgres?sslmode=require"

CLIENTES = [
    "(Escribir nuevo)",
    "CLIENTE 1",
    "CLIENTE 2",
    "CLIENTE 3",
]

CONDUCTORES = [
    "(Escribir nuevo)",
    "HABID CAMACHO",
    "JOSE ORTEGA PEREZ",
    "ISAAC TAFUR",
    "ISAIAS VESGA",
    "FLAVIO ROSENDO MALTE TUTALCHA",
    "SLITH JOSE ORTEGA PACHECO",
    "ABRAHAM SEGUNDO ALVAREZ VALLE",
    "RAMON TAFUR HERNANDEZ",
    "PEDRO VILLAMIL",
    "JESUS DAVID MONTE MOSQUERA",
    "CHRISTIAN MARTINEZ NAVARRO",
    "YEIMI DUQUE ZULUAGA",
    "JULIAN CALETH CORONADO",
    "CARLOS TAFUR",
    "EDUARDO RAFAEL OLIVARES ALCAZAR",
]

TIPOS_CARGA = [
    "(Escribir nuevo)",
    "General",
    "Granel",
    "Refrigerada",
    "Peligrosa",
    "Sobredimensionada",
    "Líquidos",
    "Ganado",
    "Maquinaria",
]

# ==================== FORMATO ====================
def fmt(valor):
    """Formatea número estilo colombiano: 1.500.000"""
    if valor is None or valor == 0:
        return ""
    try:
        return f"{int(valor):,}".replace(",", ".")
    except:
        return str(valor)

def limpiar(texto):
    """Convierte texto colombiano a float"""
    if not texto:
        return 0.0
    try:
        return float(str(texto).replace(".", "").replace(",", "."))
    except:
        return 0.0

def campo_dinero(label, key, col=None):
    """Input de dinero con preview formateado"""
    target = col if col else st
    val_texto = target.text_input(label, value="", placeholder="0", key=key)
    num = limpiar(val_texto)
    if num > 0:
        target.caption(f"💵 $ {fmt(num)}")
    return num

# ==================== BASE DE DATOS ====================
def init_db():
    try:
        conn = psycopg2.connect(SUPABASE_DB_URL)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS reportes_gastos (
                id SERIAL PRIMARY KEY,
                fecha TEXT NOT NULL,
                cliente TEXT NOT NULL,
                conductor TEXT NOT NULL,
                tipo_carga TEXT NOT NULL,
                origen TEXT NOT NULL,
                destino TEXT NOT NULL,
                acpm REAL DEFAULT 0,
                peaje REAL DEFAULT 0,
                carpada REAL DEFAULT 0,
                descarpada REAL DEFAULT 0,
                amarre REAL DEFAULT 0,
                desamarre REAL DEFAULT 0,
                comida REAL DEFAULT 0,
                transporte REAL DEFAULT 0,
                arreglo_llantas REAL DEFAULT 0,
                lavado REAL DEFAULT 0,
                parque REAL DEFAULT 0,
                reparacion REAL DEFAULT 0,
                obs_reparacion TEXT DEFAULT '',
                repuesto REAL DEFAULT 0,
                hotel REAL DEFAULT 0,
                comision REAL DEFAULT 0,
                propina REAL DEFAULT 0,
                cambio_cheque REAL DEFAULT 0,
                policia_escolta REAL DEFAULT 0,
                engrase REAL DEFAULT 0,
                cargue REAL DEFAULT 0,
                descargue REAL DEFAULT 0,
                bascula REAL DEFAULT 0,
                otros REAL DEFAULT 0,
                total_gastos REAL DEFAULT 0,
                anticipo REAL DEFAULT 0,
                resultado REAL DEFAULT 0,
                fecha_registro TEXT NOT NULL
            )
        """)
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Error BD: {e}")
        return False

def guardar_reporte(datos):
    try:
        conn = psycopg2.connect(SUPABASE_DB_URL)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO reportes_gastos (
                fecha, cliente, conductor, tipo_carga, origen, destino,
                acpm, peaje, carpada, descarpada, amarre, desamarre,
                comida, transporte, arreglo_llantas, lavado, parque,
                reparacion, obs_reparacion, repuesto, hotel, comision,
                propina, cambio_cheque, policia_escolta, engrase,
                cargue, descargue, bascula, otros,
                total_gastos, anticipo, resultado, fecha_registro
            ) VALUES (
                %s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,%s
            ) RETURNING id
        """, (
            datos["fecha"], datos["cliente"], datos["conductor"],
            datos["tipo_carga"], datos["origen"], datos["destino"],
            datos["acpm"], datos["peaje"], datos["carpada"], datos["descarpada"],
            datos["amarre"], datos["desamarre"], datos["comida"], datos["transporte"],
            datos["arreglo_llantas"], datos["lavado"], datos["parque"],
            datos["reparacion"], datos["obs_reparacion"], datos["repuesto"],
            datos["hotel"], datos["comision"], datos["propina"],
            datos["cambio_cheque"], datos["policia_escolta"], datos["engrase"],
            datos["cargue"], datos["descargue"], datos["bascula"], datos["otros"],
            datos["total_gastos"], datos["anticipo"], datos["resultado"],
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ))
        reporte_id = cursor.fetchone()[0]
        conn.commit()
        conn.close()
        return reporte_id
    except Exception as e:
        st.error(f"Error guardando: {e}")
        return None

def obtener_reportes():
    try:
        conn = psycopg2.connect(SUPABASE_DB_URL)
        df = pd.read_sql_query("SELECT * FROM reportes_gastos ORDER BY id DESC", conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error cargando reportes: {e}")
        return pd.DataFrame()

def eliminar_reporte(rid):
    try:
        conn = psycopg2.connect(SUPABASE_DB_URL)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM reportes_gastos WHERE id = %s", (rid,))
        conn.commit()
        conn.close()
    except Exception as e:
        st.error(f"Error eliminando: {e}")

# ==================== EXCEL ====================
def generar_excel(datos):
    output = io.BytesIO()
    wb = Workbook()
    ws = wb.active
    ws.title = "Reporte de Gastos"

    # Estilos
    azul_oscuro = PatternFill("solid", fgColor="1F3864")
    azul_medio  = PatternFill("solid", fgColor="2E75B6")
    azul_claro  = PatternFill("solid", fgColor="D6E4F0")
    amarillo    = PatternFill("solid", fgColor="FFC000")
    verde       = PatternFill("solid", fgColor="70AD47")
    rojo        = PatternFill("solid", fgColor="FF0000")
    gris        = PatternFill("solid", fgColor="F2F2F2")

    font_titulo   = Font(name="Calibri", bold=True, size=14, color="FFFFFF")
    font_header   = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
    font_label    = Font(name="Calibri", bold=True, size=10, color="1F3864")
    font_valor    = Font(name="Calibri", size=10)
    font_total    = Font(name="Calibri", bold=True, size=11, color="FFFFFF")
    font_resultado= Font(name="Calibri", bold=True, size=12, color="FFFFFF")

    borde = Border(
        left=Side(style="thin", color="BFBFBF"),
        right=Side(style="thin", color="BFBFBF"),
        top=Side(style="thin", color="BFBFBF"),
        bottom=Side(style="thin", color="BFBFBF"),
    )
    borde_grueso = Border(
        left=Side(style="medium", color="1F3864"),
        right=Side(style="medium", color="1F3864"),
        top=Side(style="medium", color="1F3864"),
        bottom=Side(style="medium", color="1F3864"),
    )

    fmt_cop = '"$"#,##0'

    def cel(row, col, value, fill=None, font=None, align="left", number_format=None, border=None):
        c = ws.cell(row=row, column=col, value=value)
        if fill:   c.fill = fill
        if font:   c.font = font
        if border: c.border = border
        c.alignment = Alignment(horizontal=align, vertical="center", wrap_text=True)
        if number_format: c.number_format = number_format
        return c

    # ── TÍTULO ──────────────────────────────────────────────
    ws.merge_cells("A1:F1")
    cel(1, 1, "🚛  REPORTE DE GASTOS — TRANSPORTE DE CARGA",
        fill=azul_oscuro, font=font_titulo, align="center")
    ws.row_dimensions[1].height = 30

    ws.merge_cells("A2:F2")
    cel(2, 1, f"Generado: {datetime.now().strftime('%d/%m/%Y %H:%M')}",
        fill=azul_claro, font=Font(italic=True, size=9, color="555555"), align="center")

    # ── ENCABEZADO DEL VIAJE ─────────────────────────────────
    ws.merge_cells("A4:F4")
    cel(4, 1, "INFORMACIÓN DEL VIAJE",
        fill=azul_medio, font=font_header, align="center", border=borde)
    ws.row_dimensions[4].height = 18

    info = [
        ("Fecha",         datos["fecha"]),
        ("Cliente",       datos["cliente"]),
        ("Conductor",     datos["conductor"]),
        ("Tipo de Carga", datos["tipo_carga"]),
        ("Origen",        datos["origen"]),
        ("Destino",       datos["destino"]),
    ]
    r = 5
    for i, (lab, val) in enumerate(info):
        col_label = 1 + (i % 2) * 3
        col_val   = col_label + 1
        ws.merge_cells(start_row=r, start_column=col_val, end_row=r, end_column=col_val + 1)
        cel(r, col_label, lab, fill=azul_claro, font=font_label, align="right", border=borde)
        cel(r, col_val,   val, fill=gris,        font=font_valor, align="left",  border=borde)
        if i % 2 == 1:
            r += 1
    ws.row_dimensions[5].height = 16
    ws.row_dimensions[6].height = 16
    ws.row_dimensions[7].height = 16

    # ── TABLA DE GASTOS ──────────────────────────────────────
    r += 1
    ws.merge_cells(f"A{r}:F{r}")
    cel(r, 1, "DETALLE DE GASTOS",
        fill=azul_medio, font=font_header, align="center", border=borde)
    ws.row_dimensions[r].height = 18
    r += 1

    # Cabecera columnas
    for c, txt in enumerate(["#", "CONCEPTO", "VALOR (COP)", "", "OBSERVACIONES", ""], start=1):
        ws.merge_cells(start_row=r, start_column=c, end_row=r, end_column=c) if c not in (3,5) else None
        cel(r, c, txt, fill=azul_oscuro, font=font_header, align="center", border=borde)
    ws.merge_cells(f"C{r}:D{r}")
    ws.merge_cells(f"E{r}:F{r}")
    ws.row_dimensions[r].height = 16
    r += 1

    partidas = [
        ("1",  "ACPM (Combustible)",         datos["acpm"],           ""),
        ("2",  "Peaje",                       datos["peaje"],          ""),
        ("3",  "Carpada",                     datos["carpada"],        ""),
        ("4",  "Descarpada",                  datos["descarpada"],     ""),
        ("5",  "Amarre",                      datos["amarre"],         ""),
        ("6",  "Desamarre",                   datos["desamarre"],      ""),
        ("7",  "Comida",                      datos["comida"],         ""),
        ("8",  "Transporte",                  datos["transporte"],     ""),
        ("9",  "Arreglo de Llantas",          datos["arreglo_llantas"],""),
        ("10", "Lavado",                      datos["lavado"],         ""),
        ("11", "Parque",                      datos["parque"],         ""),
        ("12", "Reparación",                  datos["reparacion"],     datos["obs_reparacion"]),
        ("13", "Repuesto",                    datos["repuesto"],       ""),
        ("14", "Hotel",                       datos["hotel"],          ""),
        ("15", "Comisión",                    datos["comision"],       ""),
        ("16", "Propina",                     datos["propina"],        ""),
        ("17", "Cambio de Cheque",            datos["cambio_cheque"],  ""),
        ("18", "Policía / Escolta",           datos["policia_escolta"],""),
        ("19", "Engrase",                     datos["engrase"],        ""),
        ("20", "Cargue",                      datos["cargue"],         ""),
        ("21", "Descargue",                   datos["descargue"],      ""),
        ("22", "Báscula",                     datos["bascula"],        ""),
        ("23", "Otros",                       datos["otros"],          ""),
    ]

    for num, concepto, valor, obs in partidas:
        bg = gris if int(num) % 2 == 0 else None
        cel(r, 1, num,      fill=bg, font=font_valor, align="center", border=borde)
        cel(r, 2, concepto, fill=bg, font=font_label,  align="left",   border=borde)
        ws.merge_cells(f"C{r}:D{r}")
        c = ws.cell(row=r, column=3, value=valor if valor else None)
        c.fill = bg or PatternFill()
        c.font = font_valor
        c.border = borde
        c.alignment = Alignment(horizontal="right", vertical="center")
        c.number_format = fmt_cop
        ws.merge_cells(f"E{r}:F{r}")
        cel(r, 5, obs, fill=bg, font=font_valor, align="left", border=borde)
        ws.row_dimensions[r].height = 15
        r += 1

    # ── TOTALES ──────────────────────────────────────────────
    r += 1
    # Total Gastos
    ws.merge_cells(f"A{r}:B{r}")
    cel(r, 1, "TOTAL DE GASTOS", fill=amarillo,
        font=Font(bold=True, size=11, color="1F3864"), align="right", border=borde_grueso)
    ws.merge_cells(f"C{r}:D{r}")
    c = ws.cell(row=r, column=3, value=datos["total_gastos"])
    c.fill = amarillo; c.font = font_total; c.border = borde_grueso
    c.alignment = Alignment(horizontal="right", vertical="center")
    c.number_format = fmt_cop
    ws.row_dimensions[r].height = 20
    r += 1

    # Anticipo
    ws.merge_cells(f"A{r}:B{r}")
    cel(r, 1, "VALOR DEL ANTICIPO", fill=azul_claro,
        font=font_label, align="right", border=borde)
    ws.merge_cells(f"C{r}:D{r}")
    c = ws.cell(row=r, column=3, value=datos["anticipo"])
    c.fill = azul_claro; c.font = font_valor; c.border = borde
    c.alignment = Alignment(horizontal="right", vertical="center")
    c.number_format = fmt_cop
    ws.row_dimensions[r].height = 18
    r += 1

    # Resultado
    resultado = datos["resultado"]
    color_resultado = verde if resultado >= 0 else rojo
    etiqueta_resultado = "SOBRANTE (Anticipo > Gastos)" if resultado >= 0 else "FALTANTE (Gastos > Anticipo)"
    ws.merge_cells(f"A{r}:B{r}")
    cel(r, 1, etiqueta_resultado, fill=color_resultado,
        font=font_resultado, align="right", border=borde_grueso)
    ws.merge_cells(f"C{r}:D{r}")
    c = ws.cell(row=r, column=3, value=abs(resultado))
    c.fill = color_resultado; c.font = font_resultado; c.border = borde_grueso
    c.alignment = Alignment(horizontal="right", vertical="center")
    c.number_format = fmt_cop
    ws.row_dimensions[r].height = 22

    # ── ANCHOS DE COLUMNAS ───────────────────────────────────
    ws.column_dimensions["A"].width = 5
    ws.column_dimensions["B"].width = 28
    ws.column_dimensions["C"].width = 18
    ws.column_dimensions["D"].width = 4
    ws.column_dimensions["E"].width = 30
    ws.column_dimensions["F"].width = 4

    wb.save(output)
    output.seek(0)
    return output


# ==================== APP PRINCIPAL ====================
def main():
    st.set_page_config(
        page_title="Reporte de Gastos - Transporte",
        page_icon="🚛",
        layout="wide"
    )

    # CSS personalizado
    st.markdown("""
    <style>
        .stApp { background-color: #F0F4F8; }
        h1 { color: #1F3864; }
        h2, h3 { color: #2E75B6; }
        .resultado-box {
            padding: 20px;
            border-radius: 10px;
            text-align: center;
            font-size: 22px;
            font-weight: bold;
            margin-top: 10px;
        }
        .sobrante { background-color: #D5F5E3; color: #1E8449; border: 2px solid #27AE60; }
        .faltante { background-color: #FADBD8; color: #C0392B; border: 2px solid #E74C3C; }
        div[data-testid="metric-container"] {
            background-color: white;
            border: 1px solid #D6E4F0;
            border-radius: 8px;
            padding: 12px;
        }
    </style>
    """, unsafe_allow_html=True)

    st.title("🚛 Reporte de Gastos — Transporte de Carga")
    st.markdown("**Registro de gastos por viaje | Colombia**")
    st.divider()

    init_db()

    # ── TABS ─────────────────────────────────────────────────
    tab_nuevo, tab_historial = st.tabs(["📝 Nuevo Reporte", "📂 Historial"])

    # ═══════════════════════════════════════════════════════
    # TAB 1: NUEVO REPORTE
    # ═══════════════════════════════════════════════════════
    with tab_nuevo:

        # ── BLOQUE 1: DATOS DEL VIAJE ────────────────────────
        st.subheader("📋 Datos del Viaje")

        col1, col2, col3 = st.columns(3)

        with col1:
            fecha = st.date_input("📅 Fecha", value=datetime.today())

            sel_cliente = st.selectbox("🏢 Cliente", CLIENTES, key="sel_cliente")
            if sel_cliente == "(Escribir nuevo)":
                cliente = st.text_input("Nombre del cliente", key="cliente_manual")
            else:
                cliente = sel_cliente

        with col2:
            sel_conductor = st.selectbox("👤 Conductor", CONDUCTORES, key="sel_conductor")
            if sel_conductor == "(Escribir nuevo)":
                conductor = st.text_input("Nombre del conductor", key="conductor_manual")
            else:
                conductor = sel_conductor

            sel_tipo = st.selectbox("📦 Tipo de Carga", TIPOS_CARGA, key="sel_tipo")
            if sel_tipo == "(Escribir nuevo)":
                tipo_carga = st.text_input("Tipo de carga", key="tipo_manual")
            else:
                tipo_carga = sel_tipo

        with col3:
            origen  = st.text_input("🏁 Origen",  placeholder="Ej: Bogotá")
            destino = st.text_input("🎯 Destino", placeholder="Ej: Medellín")

        st.divider()

        # ── BLOQUE 2: GASTOS ─────────────────────────────────
        st.subheader("💸 Detalle de Gastos")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.markdown("**🛢️ Operación**")
            acpm         = campo_dinero("ACPM (Combustible)",   "acpm",         col1)
            peaje        = campo_dinero("Peaje",                "peaje",        col1)
            carpada      = campo_dinero("Carpada",              "carpada",      col1)
            descarpada   = campo_dinero("Descarpada",           "descarpada",   col1)
            amarre       = campo_dinero("Amarre",               "amarre",       col1)
            desamarre    = campo_dinero("Desamarre",            "desamarre",    col1)

        with col2:
            st.markdown("**🍽️ Viáticos**")
            comida       = campo_dinero("Comida",               "comida",       col2)
            transporte   = campo_dinero("Transporte",           "transporte",   col2)
            hotel        = campo_dinero("Hotel",                "hotel",        col2)
            comision     = campo_dinero("Comisión",             "comision",     col2)
            propina      = campo_dinero("Propina",              "propina",      col2)

        with col3:
            st.markdown("**🔧 Mantenimiento**")
            arreglo_llantas = campo_dinero("Arreglo de Llantas",  "arreglo_llantas", col3)
            lavado          = campo_dinero("Lavado",               "lavado",          col3)
            parque          = campo_dinero("Parque",               "parque",          col3)
            engrase         = campo_dinero("Engrase",              "engrase",         col3)
            repuesto        = campo_dinero("Repuesto",             "repuesto",        col3)

            st.markdown("**🔩 Reparación**")
            reparacion      = campo_dinero("Reparación",           "reparacion",      col3)
            obs_reparacion  = col3.text_input("Observaciones reparación", placeholder="Describir...", key="obs_rep")

        with col4:
            st.markdown("**📋 Otros Gastos**")
            cambio_cheque   = campo_dinero("Cambio de Cheque",    "cambio_cheque",   col4)
            policia_escolta = campo_dinero("Policía / Escolta",   "policia_escolta", col4)
            cargue          = campo_dinero("Cargue",              "cargue",          col4)
            descargue       = campo_dinero("Descargue",           "descargue",       col4)
            bascula         = campo_dinero("Báscula",             "bascula",         col4)
            otros           = campo_dinero("Otros",               "otros",           col4)

        st.divider()

        # ── BLOQUE 3: RESUMEN Y RESULTADO ────────────────────
        st.subheader("📊 Resumen")

        # Calcular total de gastos
        total_gastos = (
            acpm + peaje + carpada + descarpada + amarre + desamarre +
            comida + transporte + hotel + comision + propina +
            arreglo_llantas + lavado + parque + engrase + repuesto +
            reparacion + cambio_cheque + policia_escolta +
            cargue + descargue + bascula + otros
        )

        col_t1, col_t2, col_t3, col_t4 = st.columns(4)
        with col_t1:
            st.metric("💸 Total Gastos", f"$ {fmt(total_gastos)}" if total_gastos else "$ 0")
        with col_t2:
            anticipo_texto = st.text_input("💰 Valor del Anticipo (COP)", value="", placeholder="0", key="anticipo_input")
            anticipo = limpiar(anticipo_texto)
            if anticipo > 0:
                st.caption(f"💵 $ {fmt(anticipo)}")

        resultado = anticipo - total_gastos

        with col_t3:
            st.metric("🎯 Anticipo", f"$ {fmt(anticipo)}" if anticipo else "$ 0")
        with col_t4:
            if resultado >= 0:
                st.metric("✅ Sobrante", f"$ {fmt(resultado)}")
            else:
                st.metric("⚠️ Faltante", f"$ {fmt(abs(resultado))}")

        # Caja de resultado visual
        if total_gastos > 0 or anticipo > 0:
            if resultado >= 0:
                st.markdown(f"""
                <div class="resultado-box sobrante">
                    ✅ SOBRANTE: El anticipo cubre los gastos<br>
                    Anticipo ($ {fmt(anticipo)}) − Gastos ($ {fmt(total_gastos)}) = <strong>$ {fmt(resultado)}</strong>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div class="resultado-box faltante">
                    ⚠️ FALTANTE: Los gastos superan el anticipo<br>
                    Anticipo ($ {fmt(anticipo)}) − Gastos ($ {fmt(total_gastos)}) = <strong>−$ {fmt(abs(resultado))}</strong>
                </div>
                """, unsafe_allow_html=True)

        st.divider()

        # ── BOTONES ──────────────────────────────────────────
        col_b1, col_b2 = st.columns(2)

        datos_reporte = {
            "fecha":           fecha.strftime("%Y-%m-%d"),
            "cliente":         cliente,
            "conductor":       conductor,
            "tipo_carga":      tipo_carga,
            "origen":          origen,
            "destino":         destino,
            "acpm":            acpm,
            "peaje":           peaje,
            "carpada":         carpada,
            "descarpada":      descarpada,
            "amarre":          amarre,
            "desamarre":       desamarre,
            "comida":          comida,
            "transporte":      transporte,
            "arreglo_llantas": arreglo_llantas,
            "lavado":          lavado,
            "parque":          parque,
            "reparacion":      reparacion,
            "obs_reparacion":  obs_reparacion,
            "repuesto":        repuesto,
            "hotel":           hotel,
            "comision":        comision,
            "propina":         propina,
            "cambio_cheque":   cambio_cheque,
            "policia_escolta": policia_escolta,
            "engrase":         engrase,
            "cargue":          cargue,
            "descargue":       descargue,
            "bascula":         bascula,
            "otros":           otros,
            "total_gastos":    total_gastos,
            "anticipo":        anticipo,
            "resultado":       resultado,
        }

        with col_b1:
            if st.button("💾 Guardar Reporte", type="primary", use_container_width=True):
                if not cliente or not conductor or not origen or not destino:
                    st.error("⚠️ Completa los campos: Cliente, Conductor, Origen y Destino")
                else:
                    rid = guardar_reporte(datos_reporte)
                    if rid:
                        st.success(f"✅ Reporte guardado correctamente (ID: {rid})")
                    else:
                        st.error("❌ Error al guardar. Revisa la conexión.")

        with col_b2:
            excel_bytes = generar_excel(datos_reporte)
            nombre_archivo = f"Gastos_{conductor.replace(' ','_')}_{fecha.strftime('%d-%m-%Y')}.xlsx"
            st.download_button(
                label="📥 Descargar en Excel",
                data=excel_bytes,
                file_name=nombre_archivo,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

    # ═══════════════════════════════════════════════════════
    # TAB 2: HISTORIAL
    # ═══════════════════════════════════════════════════════
    with tab_historial:
        st.subheader("📂 Historial de Reportes")

        col1, col2, col3 = st.columns(3)
        with col1:
            filtro_conductor = st.text_input("🔍 Filtrar por Conductor", key="hist_conductor")
        with col2:
            filtro_cliente   = st.text_input("🔍 Filtrar por Cliente",   key="hist_cliente")
        with col3:
            if st.button("🔄 Actualizar", type="primary"):
                st.rerun()

        df = obtener_reportes()

        if df.empty:
            st.info("No hay reportes guardados aún.")
        else:
            if filtro_conductor:
                df = df[df["conductor"].str.contains(filtro_conductor, case=False, na=False)]
            if filtro_cliente:
                df = df[df["cliente"].str.contains(filtro_cliente, case=False, na=False)]

            # Métricas resumen
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("📋 Reportes", len(df))
            with col2:
                st.metric("💸 Total Gastos", f"$ {fmt(df['total_gastos'].sum())}")
            with col3:
                st.metric("💰 Total Anticipos", f"$ {fmt(df['anticipo'].sum())}")
            with col4:
                res = df["resultado"].sum()
                label = "✅ Sobrante Total" if res >= 0 else "⚠️ Faltante Total"
                st.metric(label, f"$ {fmt(abs(res))}")

            st.divider()

            # Tabla resumen
            cols_mostrar = ["id", "fecha", "cliente", "conductor", "tipo_carga",
                            "origen", "destino", "total_gastos", "anticipo", "resultado"]
            df_vis = df[cols_mostrar].copy()
            df_vis.columns = ["ID", "Fecha", "Cliente", "Conductor", "Tipo Carga",
                               "Origen", "Destino", "Total Gastos", "Anticipo", "Resultado"]
            df_vis["Total Gastos"] = df_vis["Total Gastos"].apply(lambda x: f"$ {fmt(x)}")
            df_vis["Anticipo"]     = df_vis["Anticipo"].apply(lambda x: f"$ {fmt(x)}")
            df_vis["Resultado"]    = df_vis["Resultado"].apply(
                lambda x: f"✅ $ {fmt(x)}" if x >= 0 else f"⚠️ -$ {fmt(abs(x))}"
            )

            st.dataframe(df_vis, use_container_width=True, hide_index=True, height=350)

            st.divider()

            # Detalle + eliminar
            st.subheader("🔎 Ver / Eliminar Reporte")
            ids_disponibles = df["id"].tolist()
            id_sel = st.selectbox("Selecciona ID", ids_disponibles)

            col_ver, col_del = st.columns(2)

            with col_ver:
                if st.button("👁️ Ver Detalle", use_container_width=True):
                    fila = df[df["id"] == id_sel].iloc[0]
                    with st.expander(f"Detalle Reporte #{id_sel}", expanded=True):
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write(f"**Fecha:** {fila['fecha']}")
                            st.write(f"**Cliente:** {fila['cliente']}")
                            st.write(f"**Conductor:** {fila['conductor']}")
                            st.write(f"**Tipo Carga:** {fila['tipo_carga']}")
                            st.write(f"**Origen → Destino:** {fila['origen']} → {fila['destino']}")
                            st.divider()
                            st.write(f"**ACPM:** $ {fmt(fila['acpm'])}")
                            st.write(f"**Peaje:** $ {fmt(fila['peaje'])}")
                            st.write(f"**Carpada:** $ {fmt(fila['carpada'])}")
                            st.write(f"**Descarpada:** $ {fmt(fila['descarpada'])}")
                            st.write(f"**Amarre:** $ {fmt(fila['amarre'])}")
                            st.write(f"**Desamarre:** $ {fmt(fila['desamarre'])}")
                            st.write(f"**Comida:** $ {fmt(fila['comida'])}")
                            st.write(f"**Transporte:** $ {fmt(fila['transporte'])}")
                            st.write(f"**Arreglo Llantas:** $ {fmt(fila['arreglo_llantas'])}")
                            st.write(f"**Lavado:** $ {fmt(fila['lavado'])}")
                            st.write(f"**Parque:** $ {fmt(fila['parque'])}")
                        with col2:
                            st.write(f"**Reparación:** $ {fmt(fila['reparacion'])}")
                            if fila['obs_reparacion']:
                                st.info(f"📝 Obs. Reparación: {fila['obs_reparacion']}")
                            st.write(f"**Repuesto:** $ {fmt(fila['repuesto'])}")
                            st.write(f"**Hotel:** $ {fmt(fila['hotel'])}")
                            st.write(f"**Comisión:** $ {fmt(fila['comision'])}")
                            st.write(f"**Propina:** $ {fmt(fila['propina'])}")
                            st.write(f"**Cambio de Cheque:** $ {fmt(fila['cambio_cheque'])}")
                            st.write(f"**Policía / Escolta:** $ {fmt(fila['policia_escolta'])}")
                            st.write(f"**Engrase:** $ {fmt(fila['engrase'])}")
                            st.write(f"**Cargue:** $ {fmt(fila['cargue'])}")
                            st.write(f"**Descargue:** $ {fmt(fila['descargue'])}")
                            st.write(f"**Báscula:** $ {fmt(fila['bascula'])}")
                            st.write(f"**Otros:** $ {fmt(fila['otros'])}")
                            st.divider()
                            st.success(f"**💸 Total Gastos:** $ {fmt(fila['total_gastos'])}")
                            st.info(f"**💰 Anticipo:** $ {fmt(fila['anticipo'])}")
                            res = fila['resultado']
                            if res >= 0:
                                st.success(f"**✅ Sobrante:** $ {fmt(res)}")
                            else:
                                st.error(f"**⚠️ Faltante:** $ {fmt(abs(res))}")

                        # Botón descargar Excel del detalle
                        datos_excel = {
                            "fecha": fila["fecha"], "cliente": fila["cliente"],
                            "conductor": fila["conductor"], "tipo_carga": fila["tipo_carga"],
                            "origen": fila["origen"], "destino": fila["destino"],
                            "acpm": fila["acpm"], "peaje": fila["peaje"],
                            "carpada": fila["carpada"], "descarpada": fila["descarpada"],
                            "amarre": fila["amarre"], "desamarre": fila["desamarre"],
                            "comida": fila["comida"], "transporte": fila["transporte"],
                            "arreglo_llantas": fila["arreglo_llantas"], "lavado": fila["lavado"],
                            "parque": fila["parque"], "reparacion": fila["reparacion"],
                            "obs_reparacion": fila["obs_reparacion"], "repuesto": fila["repuesto"],
                            "hotel": fila["hotel"], "comision": fila["comision"],
                            "propina": fila["propina"], "cambio_cheque": fila["cambio_cheque"],
                            "policia_escolta": fila["policia_escolta"], "engrase": fila["engrase"],
                            "cargue": fila["cargue"], "descargue": fila["descargue"],
                            "bascula": fila["bascula"], "otros": fila["otros"],
                            "total_gastos": fila["total_gastos"], "anticipo": fila["anticipo"],
                            "resultado": fila["resultado"],
                        }
                        xl = generar_excel(datos_excel)
                        st.download_button(
                            "📥 Descargar este reporte en Excel",
                            data=xl,
                            file_name=f"Gastos_{fila['conductor'].replace(' ','_')}_{fila['fecha']}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )

            with col_del:
                if st.button("🗑️ Eliminar Reporte", type="secondary", use_container_width=True):
                    eliminar_reporte(id_sel)
                    st.success(f"Reporte #{id_sel} eliminado.")
                    st.rerun()


if __name__ == "__main__":
    main()
