
import os
import re
import unicodedata
from datetime import date, datetime
from io import BytesIO
from difflib import SequenceMatcher

import bcrypt
import pandas as pd
import requests
import streamlit as st
import psycopg
from psycopg.rows import dict_row

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, HRFlowable

APP_TITLE = "Sistema de Gestão de Contratos, Requisições e Catálogo"
LOGO_URL = "https://centraldecompras.sead.pi.gov.br/wp-content/uploads/2023/10/logo-centra-de-compras.svg"

COR_AZUL = "#164194"
COR_VERMELHO = "#E63312"
COR_VERDE = "#107527"
COR_TEXTO = "#1D1D1B"
COR_FUNDO = "#F6F8FC"
COR_CARD = "#FFFFFF"
COR_BORDA = "#D9E1F2"

st.set_page_config(page_title=APP_TITLE, layout="wide")

def get_database_url():
    if "DATABASE_URL" in st.secrets:
        return st.secrets["DATABASE_URL"]
    env_url = os.getenv("DATABASE_URL")
    if env_url:
        return env_url
    raise RuntimeError("DATABASE_URL não configurada. Defina em secrets ou variável de ambiente.")

def get_conn():
    url = get_database_url()
    return psycopg.connect(
        url,
        row_factory=dict_row,
        connect_timeout=15
    )

def run_query(query, params=None, fetch="all"):
    params = params or ()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            if fetch == "none":
                return None
            if fetch == "one":
                return cur.fetchone()
            return cur.fetchall()

def execute_query(query, params=None):
    params = params or ()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
        conn.commit()

def run_many(statements):
    with get_conn() as conn:
        with conn.cursor() as cur:
            for sql, params in statements:
                cur.execute(sql, params)
        conn.commit()

def testar_conexao_banco():
    try:
        row = run_query("SELECT 1 AS ok", fetch="one")
        return True, row
    except Exception as e:
        return False, str(e)

def brl(valor):
    try:
        valor = float(valor or 0)
    except Exception:
        valor = 0.0
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def data_br(data):
    if not data:
        return ""
    try:
        return pd.to_datetime(data).strftime("%d-%m-%Y")
    except Exception:
        return str(data)

def datahora_br(data):
    if not data:
        return ""
    try:
        return pd.to_datetime(data).strftime("%d-%m-%Y %H:%M:%S")
    except Exception:
        return str(data)

def parse_data_br(texto):
    try:
        return datetime.strptime(str(texto).strip(), "%d-%m-%Y").date()
    except Exception:
        return None

def normalizar_status(inicio, fim):
    try:
        fim_dt = pd.to_datetime(fim, dayfirst=True).date()
    except Exception:
        return "VIGENTE"
    return "VENCIDA" if fim_dt < date.today() else "VIGENTE"

def normalizar_texto(txt):
    txt = str(txt or "").lower().strip()
    txt = unicodedata.normalize("NFKD", txt).encode("ascii", "ignore").decode("ascii")
    txt = re.sub(r"[^a-z0-9\s]", " ", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt

def similaridade(a, b):
    a = normalizar_texto(a)
    b = normalizar_texto(b)
    if not a or not b:
        return 0
    return SequenceMatcher(None, a, b).ratio()

def match_inteligente(consulta, texto):
    consulta_n = normalizar_texto(consulta)
    texto_n = normalizar_texto(texto)
    if not consulta_n:
        return True
    if consulta_n in texto_n:
        return True
    termos = [t for t in consulta_n.split() if len(t) > 1]
    if termos:
        hits = sum(1 for t in termos if t in texto_n)
        if hits >= max(1, len(termos) - 1):
            return True
    if similaridade(consulta_n, texto_n) >= 0.72:
        return True
    for trecho in texto_n.split():
        if similaridade(consulta_n, trecho) >= 0.82:
            return True
    return False

def status_badge_html(status):
    status = str(status or "").upper()
    classe = "status-vigente" if status == "VIGENTE" else "status-vencida" if status == "VENCIDA" else "status-pendente"
    return f'<span class="status-pill {classe}">{status}</span>'

def status_badge_df(status):
    return str(status or "").upper()

@st.cache_data(show_spinner=False, ttl=3600)
def fetch_logo_bytes():
    try:
        response = requests.get(LOGO_URL, timeout=20)
        response.raise_for_status()
        return response.content
    except Exception:
        return None

def rows_to_df(rows):
    return pd.DataFrame(rows) if rows else pd.DataFrame()

def init_db():
    ddl = [
        """CREATE TABLE IF NOT EXISTS usuarios(
            id BIGSERIAL PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            nivel INTEGER NOT NULL CHECK (nivel IN (0,1,2)),
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS contratos(
            id BIGSERIAL PRIMARY KEY,
            cod_unico TEXT UNIQUE NOT NULL,
            numero_sei TEXT NOT NULL,
            inicio_vigencia DATE NOT NULL,
            fim_vigencia DATE NOT NULL,
            titulo TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('VIGENTE','VENCIDA')),
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS categorias(
            id BIGSERIAL PRIMARY KEY,
            codigo_categoria TEXT UNIQUE NOT NULL,
            nome_categoria TEXT NOT NULL,
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS classes(
            id BIGSERIAL PRIMARY KEY,
            codigo_classe TEXT UNIQUE NOT NULL,
            nome_classe TEXT NOT NULL,
            categoria_id BIGINT NOT NULL REFERENCES categorias(id) ON DELETE CASCADE,
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS padroes_descritivos(
            id BIGSERIAL PRIMARY KEY,
            codigo_padrao_descritivo TEXT UNIQUE NOT NULL,
            nome_padrao_descritivo TEXT NOT NULL,
            classe_id BIGINT NOT NULL REFERENCES classes(id) ON DELETE CASCADE,
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS catalogo(
            id BIGSERIAL PRIMARY KEY,
            codigo_item TEXT UNIQUE NOT NULL,
            nome_item TEXT NOT NULL,
            padrao_descritivo_id BIGINT NOT NULL REFERENCES padroes_descritivos(id) ON DELETE RESTRICT,
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS itens(
            id BIGSERIAL PRIMARY KEY,
            contrato_cod_unico TEXT REFERENCES contratos(cod_unico) ON DELETE CASCADE,
            codigo_item TEXT REFERENCES catalogo(codigo_item) ON DELETE RESTRICT,
            detalhes_item TEXT,
            quantidade NUMERIC(18,2) DEFAULT 0,
            valor_unitario NUMERIC(18,2) DEFAULT 0,
            valor_total NUMERIC(18,2) DEFAULT 0,
            saldo_quantidade NUMERIC(18,2) DEFAULT 0,
            saldo_valor NUMERIC(18,2) DEFAULT 0,
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS requisicoes(
            id BIGSERIAL PRIMARY KEY,
            item_id BIGINT NOT NULL REFERENCES itens(id) ON DELETE CASCADE,
            contrato_cod_unico TEXT NOT NULL REFERENCES contratos(cod_unico) ON DELETE CASCADE,
            codigo_item TEXT NOT NULL REFERENCES catalogo(codigo_item) ON DELETE RESTRICT,
            quantidade_solicitada NUMERIC(18,2) NOT NULL,
            valor_estimado NUMERIC(18,2) NOT NULL DEFAULT 0,
            justificativa TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDENTE' CHECK (status IN ('PENDENTE','APROVADA','REJEITADA')),
            usuario_solicitante TEXT NOT NULL,
            data_solicitacao TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            usuario_aprovador TEXT,
            data_aprovacao TIMESTAMP,
            observacao_aprovacao TEXT
        )"""
    ]
    with get_conn() as conn:
        with conn.cursor() as cur:
            for sql in ddl:
                cur.execute(sql)
        conn.commit()
    row = run_query("SELECT id FROM usuarios WHERE username = %s", ("AndersonMPMelo",), fetch="one")
    if row is None:
        senha = bcrypt.hashpw("Tomatinho".encode(), bcrypt.gensalt()).decode()
        execute_query("INSERT INTO usuarios(username, password, nivel) VALUES (%s, %s, %s)", ("AndersonMPMelo", senha, 0))

def recalc_item_balance(item_id):
    row = run_query("SELECT id, quantidade, valor_unitario FROM itens WHERE id = %s", (int(item_id),), fetch="one")
    if row is None:
        return
    qtd_total = float(row["quantidade"] or 0)
    valor_unit = float(row["valor_unitario"] or 0)
    aprovado = run_query(
        "SELECT COALESCE(SUM(quantidade_solicitada), 0) AS total_aprovado FROM requisicoes WHERE item_id = %s AND status = 'APROVADA'",
        (int(item_id),), fetch="one"
    )
    aprovado = float((aprovado or {}).get("total_aprovado", 0) or 0)
    saldo_qtd = max(qtd_total - aprovado, 0)
    saldo_valor = saldo_qtd * valor_unit
    execute_query(
        "UPDATE itens SET saldo_quantidade = %s, saldo_valor = %s, valor_total = %s WHERE id = %s",
        (saldo_qtd, saldo_valor, qtd_total * valor_unit, int(item_id))
    )

def excluir_contrato(cod_unico):
    execute_query("DELETE FROM contratos WHERE cod_unico = %s", (cod_unico,))

def excluir_item(item_id):
    execute_query("DELETE FROM itens WHERE id = %s", (int(item_id),))

def excluir_catalogo(codigo_item):
    execute_query("DELETE FROM catalogo WHERE codigo_item = %s", (codigo_item,))

def _pdf_add_logo(elements, styles):
    # Compatível com Streamlit Cloud: não usa svglib/pycairo.
    elements.append(Paragraph("<b>Central de Compras</b>", styles["PdfHeader"]))

def gerar_pdf_consulta_contratos(df, filtros_texto, texto_inexistencia=None, justificativa=""):
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, leftMargin=1.5 * cm, rightMargin=1.5 * cm, topMargin=1.2 * cm, bottomMargin=1.2 * cm)
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="PdfHeader", fontSize=18, leading=22, textColor=colors.HexColor(COR_AZUL), spaceAfter=8))
    styles.add(ParagraphStyle(name="PdfSmall", fontSize=9, leading=12, textColor=colors.HexColor("#4b5563"), spaceAfter=4))
    styles.add(ParagraphStyle(name="PdfTitle", fontSize=15, leading=18, textColor=colors.HexColor(COR_AZUL), spaceAfter=8))
    styles.add(ParagraphStyle(name="PdfSection", fontSize=11, leading=14, textColor=colors.HexColor(COR_TEXTO), spaceAfter=5))
    styles.add(ParagraphStyle(name="PdfBody", fontSize=9, leading=12, textColor=colors.HexColor(COR_TEXTO), spaceAfter=3))
    styles.add(ParagraphStyle(name="PdfItem", fontSize=9, leading=12, leftIndent=12, textColor=colors.HexColor(COR_TEXTO), spaceAfter=2))
    elementos = []
    agora = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    _pdf_add_logo(elementos, styles)
    elementos.append(Paragraph("<b>GOVERNO DO ESTADO</b>", styles["PdfHeader"]))
    elementos.append(Paragraph("Consulta Pública de Contratos e Itens", styles["PdfTitle"]))
    elementos.append(Paragraph(f"Filtros aplicados: {filtros_texto}", styles["PdfSmall"]))
    if justificativa.strip():
        elementos.append(Paragraph(f"Justificativa: {justificativa}", styles["PdfSmall"]))
    elementos.append(Paragraph(f"Emitido em: {agora}", styles["PdfSmall"]))
    elementos.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor(COR_AZUL), spaceBefore=6, spaceAfter=8))
    if texto_inexistencia:
        elementos.append(Paragraph(f"<b>Resultado:</b> {texto_inexistencia}", styles["PdfBody"]))
    elif df.empty:
        elementos.append(Paragraph("<b>Resultado:</b> Nenhum registro localizado para os filtros informados.", styles["PdfBody"]))
    else:
        elementos.append(Paragraph("<b>Resultado:</b> Foram localizados os registros abaixo.", styles["PdfBody"]))
    elementos.append(Spacer(1, 0.2 * cm))
    if not df.empty:
        for _, row in df.iterrows():
            elementos.append(Paragraph(f"<b>{row['numero_sei']} | {row['titulo']}</b> | Status: {row['status']}", styles["PdfSection"]))
            elementos.append(Paragraph(f"Vigência: {data_br(row['inicio_vigencia'])} até {data_br(row['fim_vigencia'])}", styles["PdfBody"]))
            itens = row.get("itens_exportacao", [])
            if itens:
                elementos.append(Paragraph("<b>Itens localizados:</b>", styles["PdfBody"]))
                for item in itens:
                    elementos.append(Paragraph(
                        f"• <b>{item['nome_item']}</b> | Padrão Descritivo: {item['nome_padrao_descritivo']} | Detalhes: {item['detalhes_item']} | Saldo Atual: {item['saldo_quantidade']}",
                        styles["PdfItem"]
                    ))
            else:
                elementos.append(Paragraph("• Nenhum item vinculado.", styles["PdfItem"]))
            elementos.append(Spacer(1, 0.18 * cm))
    doc.build(elementos)
    buffer.seek(0)
    return buffer.getvalue()

def apply_custom_css():
    st.markdown(f"""
    <style>
    .stApp {{ background: {COR_FUNDO}; color: {COR_TEXTO}; }}
    .block-container {{ padding-top: 1.1rem; padding-bottom: 2rem; }}
    .status-pill {{ display:inline-block; padding:6px 12px; border-radius:999px; font-weight:700; font-size:12px; border:1px solid transparent; margin-bottom:8px; }}
    .status-vigente {{ background: rgba(16,117,39,0.12); color: {COR_VERDE}; border-color: rgba(16,117,39,0.25); }}
    .status-vencida {{ background: rgba(230,51,18,0.10); color: {COR_VERMELHO}; border-color: rgba(230,51,18,0.25); }}
    .status-pendente {{ background: rgba(247,182,0,0.18); color: #7a5900; border-color: rgba(247,182,0,0.35); }}
    .topo-sistema {{ background: linear-gradient(135deg, {COR_CARD} 0%, #eef3ff 100%); border: 1px solid {COR_BORDA}; border-radius: 24px; padding: 20px 24px; margin-bottom: 18px; box-shadow: 0 8px 24px rgba(22,65,148,0.08); }}
    .topo-texto h1 {{ margin:0; color:{COR_AZUL}; font-size:28px; line-height:1.1; }}
    .topo-texto p {{ margin:6px 0 0 0; color:#4b5563; font-size:14px; }}
    .filtro-box {{ background:{COR_CARD}; border:1px solid {COR_BORDA}; border-radius:18px; padding:12px 14px 6px 14px; margin-bottom:14px; box-shadow:0 2px 10px rgba(0,0,0,0.03); }}
    .section-card {{ background:{COR_CARD}; border:1px solid {COR_BORDA}; border-radius:20px; padding:18px; box-shadow:0 8px 22px rgba(22,65,148,0.05); margin-bottom:14px; }}
    div[data-testid="stDownloadButton"] > button {{ background: linear-gradient(135deg, {COR_AZUL} 0%, {COR_VERDE} 100%); color: white; border: none; border-radius: 12px; font-weight: 700; }}
    div[data-testid="stButton"] > button, div[data-testid="stFormSubmitButton"] > button {{ border-radius: 12px; font-weight: 600; }}
    div[data-baseweb="select"] > div, .stTextInput input, .stTextArea textarea {{ border-radius: 12px !important; }}
    </style>
    """, unsafe_allow_html=True)

def render_header():
    st.markdown(f"""
    <div class="topo-sistema">
        <div class="topo-texto">
            <h1>{APP_TITLE}</h1>
            <p>Consulta pública, requisições e gestão operacional da Central de Compras.</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

def section_box_start():
    st.markdown('<div class="section-card">', unsafe_allow_html=True)

def section_box_end():
    st.markdown('</div>', unsafe_allow_html=True)

def carregar_contratos():
    df = rows_to_df(run_query("SELECT id, cod_unico, numero_sei, inicio_vigencia, fim_vigencia, titulo, status FROM contratos ORDER BY numero_sei, titulo"))
    if not df.empty:
        df["status"] = df.apply(lambda x: normalizar_status(x["inicio_vigencia"], x["fim_vigencia"]), axis=1)
    return df

def carregar_catalogo():
    return rows_to_df(run_query("""
        SELECT cat.id, cat.codigo_item, cat.nome_item, cat.padrao_descritivo_id,
               pd.codigo_padrao_descritivo, pd.nome_padrao_descritivo,
               cl.codigo_classe, cl.nome_classe, cg.codigo_categoria, cg.nome_categoria
        FROM catalogo cat
        JOIN padroes_descritivos pd ON pd.id = cat.padrao_descritivo_id
        JOIN classes cl ON cl.id = pd.classe_id
        JOIN categorias cg ON cg.id = cl.categoria_id
        ORDER BY cg.nome_categoria, cl.nome_classe, pd.nome_padrao_descritivo, cat.nome_item
    """))

def carregar_itens():
    df = rows_to_df(run_query("""
        SELECT i.id, i.contrato_cod_unico, i.codigo_item, cat.nome_item,
               pd.codigo_padrao_descritivo, pd.nome_padrao_descritivo,
               cl.codigo_classe, cl.nome_classe, cg.codigo_categoria, cg.nome_categoria,
               i.detalhes_item, i.quantidade, i.valor_unitario, i.valor_total, i.saldo_quantidade, i.saldo_valor,
               ct.numero_sei, ct.titulo, ct.inicio_vigencia, ct.fim_vigencia, ct.status
        FROM itens i
        LEFT JOIN catalogo cat ON cat.codigo_item = i.codigo_item
        LEFT JOIN padroes_descritivos pd ON pd.id = cat.padrao_descritivo_id
        LEFT JOIN classes cl ON cl.id = pd.classe_id
        LEFT JOIN categorias cg ON cg.id = cl.categoria_id
        LEFT JOIN contratos ct ON ct.cod_unico = i.contrato_cod_unico
        ORDER BY ct.numero_sei, cat.nome_item, i.id
    """))
    if not df.empty:
        df["status"] = df.apply(lambda x: normalizar_status(x["inicio_vigencia"], x["fim_vigencia"]), axis=1)
    return df

def carregar_requisicoes():
    return rows_to_df(run_query("""
        SELECT r.id, r.item_id, r.contrato_cod_unico, r.codigo_item, r.quantidade_solicitada,
               r.valor_estimado, r.justificativa, r.status, r.usuario_solicitante, r.data_solicitacao,
               r.usuario_aprovador, r.data_aprovacao, r.observacao_aprovacao,
               ct.numero_sei, ct.titulo, cat.nome_item, pd.nome_padrao_descritivo
        FROM requisicoes r
        LEFT JOIN contratos ct ON ct.cod_unico = r.contrato_cod_unico
        LEFT JOIN catalogo cat ON cat.codigo_item = r.codigo_item
        LEFT JOIN padroes_descritivos pd ON pd.id = cat.padrao_descritivo_id
        ORDER BY r.id DESC
    """))

def carregar_categorias():
    return rows_to_df(run_query("SELECT id, codigo_categoria, nome_categoria FROM categorias ORDER BY codigo_categoria, nome_categoria"))

def carregar_classes():
    return rows_to_df(run_query("""
        SELECT cl.id, cl.codigo_classe, cl.nome_classe, cl.categoria_id, cg.codigo_categoria, cg.nome_categoria
        FROM classes cl
        JOIN categorias cg ON cg.id = cl.categoria_id
        ORDER BY cg.nome_categoria, cl.nome_classe
    """))

def carregar_padroes():
    return rows_to_df(run_query("""
        SELECT pd.id, pd.codigo_padrao_descritivo, pd.nome_padrao_descritivo, pd.classe_id,
               cl.codigo_classe, cl.nome_classe, cg.codigo_categoria, cg.nome_categoria
        FROM padroes_descritivos pd
        JOIN classes cl ON cl.id = pd.classe_id
        JOIN categorias cg ON cg.id = cl.categoria_id
        ORDER BY cg.nome_categoria, cl.nome_classe, pd.nome_padrao_descritivo
    """))

def build_dropdown_options(contratos_df, itens_df):
    options = [{"tipo": "Todos", "label": "Todos os Padrões Descritivos", "valor": ""}]
    if not itens_df.empty and "nome_padrao_descritivo" in itens_df.columns:
        padroes = (
            itens_df[["nome_padrao_descritivo"]]
            .dropna()
            .drop_duplicates()
            .sort_values("nome_padrao_descritivo")
        )
        for _, row in padroes.iterrows():
            nome = str(row["nome_padrao_descritivo"] or "").strip()
            if nome:
                options.append({
                    "tipo": "Padrão Descritivo",
                    "label": nome,
                    "valor": nome
                })
    return options

def aplicar_filtro_lista(contratos_df, itens_df, selecao):
    if not selecao or selecao.get("tipo") == "Todos" or not selecao.get("valor"):
        return contratos_df, itens_df

    valor = str(selecao.get("valor") or "").strip()

    itens_filtrados = itens_df[
        itens_df["nome_padrao_descritivo"]
        .fillna("")
        .astype(str)
        .str.strip()
        .eq(valor)
    ].copy()

    cods = itens_filtrados["contrato_cod_unico"].dropna().astype(str).unique().tolist()
    contratos_filtrados = contratos_df[contratos_df["cod_unico"].astype(str).isin(cods)].copy()

    return contratos_filtrados, itens_filtrados


def aplicar_filtros_consulta(contratos_df, itens_df, busca_geral="", numero_sei="Todos", filtro_status="Todos", padrao_texto=""):
    contratos_filtrados = contratos_df.copy()
    itens_filtrados = itens_df.copy()
    if numero_sei != "Todos":
        contratos_filtrados = contratos_filtrados[contratos_filtrados["numero_sei"].astype(str) == str(numero_sei)]
        itens_filtrados = itens_filtrados[itens_filtrados["numero_sei"].astype(str) == str(numero_sei)]
    if filtro_status != "Todos":
        contratos_filtrados = contratos_filtrados[contratos_filtrados["status"] == filtro_status]
        itens_filtrados = itens_filtrados[itens_filtrados["status"] == filtro_status]
    if padrao_texto:
        itens_filtrados = itens_filtrados[
            itens_filtrados["nome_padrao_descritivo"]
            .fillna("")
            .astype(str)
            .apply(lambda x: match_inteligente(padrao_texto, x) or normalizar_texto(padrao_texto) in normalizar_texto(x))
        ]
        cods_padrao = itens_filtrados["contrato_cod_unico"].dropna().astype(str).unique().tolist()
        contratos_filtrados = contratos_filtrados[contratos_filtrados["cod_unico"].astype(str).isin(cods_padrao)]
    if busca_geral:
        mask_contrato = contratos_filtrados.apply(lambda row: (
            match_inteligente(busca_geral, row["titulo"]) or
            match_inteligente(busca_geral, row["numero_sei"]) or
            match_inteligente(busca_geral, row["cod_unico"])
        ), axis=1)
        contratos_por_texto = contratos_filtrados[mask_contrato]
        mask_itens = itens_filtrados.apply(lambda row: (
            match_inteligente(busca_geral, row["nome_item"]) or
            match_inteligente(busca_geral, row["detalhes_item"]) or
            match_inteligente(busca_geral, row["nome_padrao_descritivo"]) or
            match_inteligente(busca_geral, row["nome_classe"]) or
            match_inteligente(busca_geral, row["nome_categoria"])
        ), axis=1)
        itens_por_texto = itens_filtrados[mask_itens]
        cods_contrato = set(contratos_por_texto["cod_unico"].tolist()) | set(itens_por_texto["contrato_cod_unico"].dropna().tolist())
        contratos_filtrados = contratos_filtrados[contratos_filtrados["cod_unico"].isin(cods_contrato)]
        itens_filtrados = itens_filtrados[itens_filtrados["contrato_cod_unico"].isin(cods_contrato)]
        if not itens_por_texto.empty:
            itens_filtrados = itens_filtrados[itens_filtrados["id"].isin(itens_por_texto["id"].tolist())]
    return contratos_filtrados, itens_filtrados

def card_contrato_html(numero_sei, titulo, inicio, fim, status):
    cor = COR_VERDE if status == "VIGENTE" else COR_VERMELHO
    return f"""
    <div style="border:1px solid {COR_BORDA}; border-radius:20px; padding:18px; background:linear-gradient(135deg,#ffffff 0%,#f7faff 100%); box-shadow:0 8px 22px rgba(22,65,148,0.06); margin-bottom:8px;">
        <div style="display:flex;justify-content:space-between;gap:16px;align-items:center;flex-wrap:wrap;">
            <div>
                <div style="font-size:14px;color:#475569;"><b>Nº SEI:</b> {numero_sei}</div>
                <div style="font-size:20px;color:{COR_AZUL};font-weight:800;margin-top:6px;">{titulo}</div>
                <div style="font-size:13px;color:#64748b;margin-top:8px;">Vigência: {inicio} até {fim}</div>
            </div>
            <div style="background:{cor}; color:white; padding:8px 14px; border-radius:999px; font-size:12px; font-weight:700; white-space:nowrap;">{status}</div>
        </div>
    </div>
    """

if "logado" not in st.session_state:
    st.session_state.logado = False
if "usuario" not in st.session_state:
    st.session_state.usuario = "Visitante"
if "nivel" not in st.session_state:
    st.session_state.nivel = None

def is_admin():
    return st.session_state.logado and st.session_state.nivel == 0

def pode_cadastrar_contrato():
    return st.session_state.logado and st.session_state.nivel in [0, 2]

def pode_cadastrar_item():
    return st.session_state.logado and st.session_state.nivel in [0, 2]

def pode_cadastrar_codificacao():
    return st.session_state.logado and st.session_state.nivel == 0

def pode_editar_dados():
    return st.session_state.logado and st.session_state.nivel in [0, 1]

def pode_requisitar():
    return st.session_state.logado and st.session_state.nivel in [0, 1, 2]

def pode_aprovar():
    return st.session_state.logado and st.session_state.nivel in [0, 1]

def login_sidebar():
    with st.sidebar:
        st.markdown("## Acesso")
        if not st.session_state.logado:
            modo = st.radio("Escolha o modo de acesso", ["Acesso público", "Entrar com login"], index=0)
            if modo == "Entrar com login":
                usuario = st.text_input("Usuário")
                senha = st.text_input("Senha", type="password")
                if st.button("Entrar", use_container_width=True):
                    dados = run_query("SELECT * FROM usuarios WHERE username = %s", (usuario,), fetch="one")
                    senha_hash = dados["password"] if dados else None
                    ok = False
                    if senha_hash:
                        try:
                            ok = bcrypt.checkpw(senha.encode(), senha_hash.encode())
                        except Exception:
                            ok = False
                    if dados and ok:
                        st.session_state.logado = True
                        st.session_state.usuario = dados["username"]
                        st.session_state.nivel = dados["nivel"]
                        st.rerun()
                    else:
                        st.error("Usuário ou senha inválidos.")
            else:
                st.info("Visitantes podem consultar e exportar contratos e itens.")
                st.session_state.logado = False
                st.session_state.usuario = "Visitante"
                st.session_state.nivel = None
        else:
            st.success(f"Logado como {st.session_state.usuario}")
            st.write(f"Nível: {st.session_state.nivel}")
            if st.button("Sair", use_container_width=True):
                st.session_state.logado = False
                st.session_state.usuario = "Visitante"
                st.session_state.nivel = None
                st.rerun()

# startup
apply_custom_css()

ok_db, db_info = testar_conexao_banco()
if not ok_db:
    st.error("Não foi possível conectar ao banco PostgreSQL.")
    st.code(str(db_info))
    st.info("Verifique se DATABASE_URL está em Settings > Secrets, se a URL é do Neon/Supabase e se contém sslmode=require.")
    st.stop()

init_db()
login_sidebar()
render_header()

menu_publico = ["Contratos"]
menu_logado = menu_publico + ["Requisições"]
if pode_editar_dados():
    menu_logado = ["Dashboard"] + menu_logado
if pode_aprovar():
    menu_logado = menu_logado + ["Aprovação de Requisições"]
menu_logado = menu_logado + ["Cadastro de Contratos", "Cadastro de Itens"]
if pode_editar_dados():
    menu_logado = menu_logado + ["Editar Contratos", "Editar Itens", "Editar Catálogo"]
menu_admin = menu_logado + ["Editar Requisições", "Codificação", "Usuários"]

if is_admin():
    opcoes_menu = menu_admin
elif st.session_state.logado:
    opcoes_menu = menu_logado
else:
    opcoes_menu = menu_publico

menu = st.sidebar.selectbox("Menu", opcoes_menu)

if menu == "Dashboard":
    if not pode_editar_dados():
        st.error("Somente usuários nível 0 e 1 podem acessar o Dashboard.")
        st.stop()
    st.title("Dashboard Gerencial")
    contratos_df = carregar_contratos()
    itens_df = carregar_itens()
    req_df = carregar_requisicoes()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Contratos", len(contratos_df))
    c2.metric("Contratos Vigentes", int((contratos_df["status"] == "VIGENTE").sum()) if not contratos_df.empty else 0)
    c3.metric("Saldo Financeiro Total", brl(float(itens_df["saldo_valor"].fillna(0).sum()) if not itens_df.empty else 0.0))
    c4.metric("Requisições", len(req_df))

if menu == "Contratos":
    st.title("Consulta de Contratos e Itens")
    st.caption("Consulte contratos e itens vinculados com busca por texto ou por lista suspensa de Padrão Descritivo.")
    contratos_df = carregar_contratos()
    itens_df = carregar_itens()
    if contratos_df.empty:
        st.warning("Nenhum contrato cadastrado.")
        st.stop()
    opcoes_lista = build_dropdown_options(contratos_df, itens_df)
    st.markdown('<div class="filtro-box">', unsafe_allow_html=True)
    modo_busca = st.radio("Modo de busca", ["Busca por texto", "Busca por lista suspensa"], horizontal=True)
    if modo_busca == "Busca por texto":
        c1, c2, c3, c4 = st.columns([2, 1.2, 1, 1.4])
        busca_geral = c1.text_input("Buscar contrato, item, detalhe ou categoria")
        busca_lista = None
        numero_sei = c2.selectbox("Número SEI", ["Todos"] + sorted(contratos_df["numero_sei"].astype(str).unique().tolist()))
        filtro_status = c3.selectbox("Status", ["Todos", "VIGENTE", "VENCIDA"])
        padrao_texto = c4.text_input("Padrão Descritivo")
    else:
        c1, c2, c3 = st.columns([2.4, 1.2, 1])
        labels = [opt["label"] for opt in opcoes_lista]
        label_escolhida = c1.selectbox("Selecione o Padrão Descritivo", labels)
        busca_lista = next((opt for opt in opcoes_lista if opt["label"] == label_escolhida), None)
        numero_sei = c2.selectbox("Número SEI", ["Todos"] + sorted(contratos_df["numero_sei"].astype(str).unique().tolist()))
        filtro_status = c3.selectbox("Status", ["Todos", "VIGENTE", "VENCIDA"])
        busca_geral = ""
        padrao_texto = ""
    justificativa_pdf = st.text_area("Justificativa para constar no PDF", placeholder="Descreva a finalidade da consulta ou do atesto.")
    st.markdown('</div>', unsafe_allow_html=True)
    contratos_filtrados, itens_filtrados = aplicar_filtros_consulta(contratos_df, itens_df, busca_geral, numero_sei, filtro_status, padrao_texto)
    if modo_busca == "Busca por lista suspensa":
        contratos_filtrados, itens_filtrados = aplicar_filtro_lista(contratos_filtrados, itens_filtrados, busca_lista)
    resumo_busca = busca_geral or (busca_lista["label"] if busca_lista else "Nenhuma")
    resumo_filtros = f"Modo: {modo_busca} | Busca: {resumo_busca} | Nº SEI: {numero_sei} | Status: {filtro_status} | Padrão Descritivo: {padrao_texto or 'Nenhum'}"
    contratos_export = contratos_filtrados.copy()
    if not contratos_export.empty:
        contratos_export["itens_exportacao"] = contratos_export["cod_unico"].apply(
            lambda cod: itens_filtrados[itens_filtrados["contrato_cod_unico"] == cod][["nome_item", "nome_padrao_descritivo", "detalhes_item", "saldo_quantidade"]].to_dict("records")
        )
    texto_inexistencia = None
    if contratos_filtrados.empty and itens_filtrados.empty:
        texto_inexistencia = "Atesta-se, para os filtros informados, a inexistência de item ou contrato correspondente nesta base."
    pdf_bytes = gerar_pdf_consulta_contratos(contratos_export, resumo_filtros, texto_inexistencia, justificativa_pdf)
    st.download_button("Exportar consulta em PDF", data=pdf_bytes, file_name=f"consulta_contratos_itens_{datetime.now().strftime('%d-%m-%Y_%H-%M-%S')}.pdf", mime="application/pdf", use_container_width=True)
    st.divider()
    if contratos_filtrados.empty and itens_filtrados.empty:
        st.error("Inexistência de item ou contrato para os filtros informados.")
        st.stop()
    for _, row in contratos_filtrados.iterrows():
        itens_contrato = itens_filtrados[itens_filtrados["contrato_cod_unico"] == row["cod_unico"]].copy()
        with st.expander(f"{row['numero_sei']} - {row['titulo']} [{row['status']}]", expanded=False):
            st.markdown(card_contrato_html(row["numero_sei"], row["titulo"], data_br(row["inicio_vigencia"]), data_br(row["fim_vigencia"]), row["status"]), unsafe_allow_html=True)
            if itens_contrato.empty:
                st.warning("Nenhum item correspondente localizado neste contrato para os filtros aplicados.")
            else:
                st.markdown("#### Itens localizados")
                for _, item in itens_contrato.iterrows():
                    with st.container(border=True):
                        c1, c2 = st.columns([1.9, 1])
                        with c1:
                            st.write(f"**Item:** {item['nome_item']}")
                            st.write(f"**Padrão Descritivo:** {item['nome_padrao_descritivo']}")
                            st.write(f"**Detalhes:** {item['detalhes_item']}")
                        with c2:
                            st.write(f"**Quantidade Inicial:** {item['quantidade']}")
                            st.write(f"**Saldo Atual:** {item['saldo_quantidade']}")
                            st.write(f"**Saldo Financeiro:** {brl(item['saldo_valor'])}")

if menu == "Requisições":
    if not pode_requisitar():
        st.error("Faça login para acessar o módulo de Requisições.")
        st.stop()
    st.title("Requisições")
    st.caption("Localize itens com busca por texto ou lista suspensa.")
    itens_df = carregar_itens()
    if itens_df.empty:
        st.warning("Nenhum item cadastrado.")
        st.stop()
    st.markdown('<div class="filtro-box">', unsafe_allow_html=True)
    modo_localizacao = st.radio("Modo de localização", ["Busca por texto", "Busca por lista suspensa"], horizontal=True, key="modo_requisicoes")
    c1, c2, c3 = st.columns([1.2, 1, 1])
    sei_filtro = c1.selectbox("Número SEI", ["Todos"] + sorted([x for x in itens_df["numero_sei"].dropna().astype(str).unique().tolist()]))
    status_contrato = c2.selectbox("Status do Contrato", ["Todos", "VIGENTE", "VENCIDA"])
    somente_disponiveis = c3.checkbox("Mostrar apenas itens com saldo disponível", value=True)
    if modo_localizacao == "Busca por texto":
        a1, a2 = st.columns(2)
        padrao_filtro = a1.text_input("Padrão Descritivo")
        texto = a2.text_input("Item, detalhe ou categoria")
        selecao_item_lista = None
    else:
        padrao_opcoes = [{"label": "Todos os Padrões Descritivos", "valor": ""}]
        padroes_req = (
            itens_df[["nome_padrao_descritivo"]]
            .dropna()
            .drop_duplicates()
            .sort_values("nome_padrao_descritivo")
        )
        for _, row in padroes_req.iterrows():
            nome = str(row["nome_padrao_descritivo"] or "").strip()
            if nome:
                padrao_opcoes.append({"label": nome, "valor": nome})
        label_padrao = st.selectbox("Selecione o Padrão Descritivo", [x["label"] for x in padrao_opcoes])
        selecao_item_lista = next((x for x in padrao_opcoes if x["label"] == label_padrao), None)
        padrao_filtro = ""
        texto = ""
    st.markdown('</div>', unsafe_allow_html=True)
    itens_filtrados = itens_df.copy()
    if sei_filtro != "Todos":
        itens_filtrados = itens_filtrados[itens_filtrados["numero_sei"].astype(str) == sei_filtro]
    if status_contrato != "Todos":
        itens_filtrados = itens_filtrados[itens_filtrados["status"] == status_contrato]
    if padrao_filtro:
        itens_filtrados = itens_filtrados[
            itens_filtrados["nome_padrao_descritivo"]
            .fillna("")
            .astype(str)
            .apply(lambda x: match_inteligente(padrao_filtro, x) or normalizar_texto(padrao_filtro) in normalizar_texto(x))
        ]
    if texto:
        itens_filtrados = itens_filtrados[itens_filtrados.apply(lambda row: (
            match_inteligente(texto, row["nome_item"]) or
            match_inteligente(texto, row["detalhes_item"]) or
            match_inteligente(texto, row["nome_padrao_descritivo"]) or
            match_inteligente(texto, row["nome_classe"]) or
            match_inteligente(texto, row["nome_categoria"])
        ), axis=1)]
    if modo_localizacao == "Busca por lista suspensa" and selecao_item_lista and selecao_item_lista["valor"]:
        itens_filtrados = itens_filtrados[
            itens_filtrados["nome_padrao_descritivo"]
            .fillna("")
            .astype(str)
            .str.strip()
            .eq(str(selecao_item_lista["valor"]).strip())
        ]
    if somente_disponiveis:
        itens_filtrados = itens_filtrados[itens_filtrados["saldo_quantidade"] > 0]
    if itens_filtrados.empty:
        st.warning("Nenhum item localizado para os filtros informados.")
        st.stop()
    for _, row in itens_filtrados.iterrows():
        with st.expander(f"{row['numero_sei']} • {row['nome_item']} • {row['status']}", expanded=False):
            st.markdown(status_badge_html(row["status"]), unsafe_allow_html=True)
            c1, c2 = st.columns([1.5, 1])
            with c1:
                st.write(f"**Contrato correspondente:** {row['titulo']}")
                st.write(f"**Padrão Descritivo:** {row['nome_padrao_descritivo']}")
                st.write(f"**Classe:** {row['nome_classe']}")
                st.write(f"**Categoria:** {row['nome_categoria']}")
                st.write(f"**Detalhamento do item:** {row['detalhes_item']}")
            with c2:
                st.write(f"**Vigência:** {data_br(row['inicio_vigencia'])} até {data_br(row['fim_vigencia'])}")
                st.write(f"**Saldo atual:** {row['saldo_quantidade']}")
                st.write(f"**Valor unitário:** {brl(row['valor_unitario'])}")
                st.write(f"**Saldo financeiro:** {brl(row['saldo_valor'])}")
    st.divider()
    section_box_start()
    st.subheader("Registrar requisição")
    itens_disponiveis = itens_filtrados[itens_filtrados["saldo_quantidade"] > 0].copy()
    if itens_disponiveis.empty:
        st.warning("Não há itens com saldo disponível para requisição.")
    else:
        itens_disponiveis["label_item"] = itens_disponiveis.apply(lambda x: f"{x['nome_item']} | SEI {x['numero_sei']} | Saldo {x['saldo_quantidade']}", axis=1)
        item_sel = st.selectbox("Selecione o item para requisição", itens_disponiveis.to_dict("records"), format_func=lambda x: x["label_item"])
        with st.form("form_requisicao_item"):
            st.write(f"**Contrato selecionado:** {item_sel['titulo']}")
            st.write(f"**Nº SEI:** {item_sel['numero_sei']}")
            st.markdown(status_badge_html(item_sel["status"]), unsafe_allow_html=True)
            quantidade_req = st.number_input("Quantidade solicitada", min_value=0.0, max_value=float(item_sel["saldo_quantidade"]), value=0.0, step=1.0)
            justificativa = st.text_area("Justificativa para utilizar ou não o item")
            enviar = st.form_submit_button("Registrar requisição", use_container_width=True)
            if enviar:
                if quantidade_req <= 0:
                    st.warning("Informe uma quantidade maior que zero.")
                elif not justificativa.strip():
                    st.warning("Informe a justificativa da requisição.")
                else:
                    valor_estimado = float(quantidade_req) * float(item_sel["valor_unitario"] or 0)
                    execute_query("""
                        INSERT INTO requisicoes(item_id, contrato_cod_unico, codigo_item, quantidade_solicitada, valor_estimado, justificativa, status, usuario_solicitante)
                        VALUES (%s, %s, %s, %s, %s, %s, 'PENDENTE', %s)
                    """, (int(item_sel["id"]), item_sel["contrato_cod_unico"], item_sel["codigo_item"], float(quantidade_req), valor_estimado, justificativa.strip(), st.session_state.usuario))
                    st.success("Requisição registrada com sucesso.")
                    st.rerun()
    section_box_end()

# módulos restantes resumidos, mantendo operação principal
if menu == "Aprovação de Requisições":
    if not pode_aprovar():
        st.error("Somente níveis 0 e 1 podem aprovar requisições.")
        st.stop()
    st.title("Aprovação de Requisições")
    req = carregar_requisicoes()
    pendentes = req[req["status"] == "PENDENTE"].copy()
    if pendentes.empty:
        st.info("Não há requisições pendentes.")
        st.stop()
    for _, row in pendentes.iterrows():
        with st.expander(f"{row['numero_sei']} • {row['nome_item']} • {row['quantidade_solicitada']}", expanded=False):
            observacao = st.text_area("Observação da análise", key=f"obs_{row['id']}")
            c1, c2 = st.columns(2)
            item = run_query("SELECT saldo_quantidade FROM itens WHERE id = %s", (int(row["item_id"]),), fetch="one")
            saldo_atual = float(item["saldo_quantidade"] or 0) if item else 0
            if c1.button("Aprovar", key=f"aprovar_{row['id']}", use_container_width=True):
                if float(row["quantidade_solicitada"]) > saldo_atual:
                    st.error("Não é possível aprovar. A quantidade solicitada é maior que o saldo atual.")
                else:
                    execute_query("UPDATE requisicoes SET status='APROVADA', usuario_aprovador=%s, data_aprovacao=CURRENT_TIMESTAMP, observacao_aprovacao=%s WHERE id=%s", (st.session_state.usuario, observacao.strip(), int(row["id"])))
                    recalc_item_balance(int(row["item_id"]))
                    st.success("Requisição aprovada.")
                    st.rerun()
            if c2.button("Rejeitar", key=f"rejeitar_{row['id']}", use_container_width=True):
                execute_query("UPDATE requisicoes SET status='REJEITADA', usuario_aprovador=%s, data_aprovacao=CURRENT_TIMESTAMP, observacao_aprovacao=%s WHERE id=%s", (st.session_state.usuario, observacao.strip(), int(row["id"])))
                st.success("Requisição rejeitada.")
                st.rerun()

if menu == "Cadastro de Contratos":
    if not pode_cadastrar_contrato():
        st.error("Somente usuários nível 2 ou nível 0 podem cadastrar contratos.")
        st.stop()
    st.title("Cadastro de Contratos")
    with st.form("form_contrato", clear_on_submit=True):
        cod_unico = st.text_input("COD Único")
        numero_sei = st.text_input("Número do SEI")
        titulo = st.text_input("Título")
        c1, c2 = st.columns(2)
        inicio_vigencia_txt = c1.text_input("Início da Vigência (DD-MM-YYYY)")
        fim_vigencia_txt = c2.text_input("Fim da Vigência (DD-MM-YYYY)")
        salvar = st.form_submit_button("Cadastrar contrato", use_container_width=True)
        if salvar:
            inicio_vigencia = parse_data_br(inicio_vigencia_txt)
            fim_vigencia = parse_data_br(fim_vigencia_txt)
            if not all([cod_unico.strip(), numero_sei.strip(), titulo.strip(), inicio_vigencia, fim_vigencia]):
                st.warning("Preencha todos os campos corretamente.")
            elif fim_vigencia < inicio_vigencia:
                st.error("A data final não pode ser menor que a data inicial.")
            else:
                status = normalizar_status(inicio_vigencia, fim_vigencia)
                execute_query("INSERT INTO contratos(cod_unico, numero_sei, inicio_vigencia, fim_vigencia, titulo, status) VALUES (%s, %s, %s, %s, %s, %s)", (cod_unico.strip(), numero_sei.strip(), inicio_vigencia, fim_vigencia, titulo.strip(), status))
                st.success("Contrato cadastrado com sucesso.")


    st.divider()
    st.subheader("Contratos cadastrados")
    contratos_lista = carregar_contratos()
    if contratos_lista.empty:
        st.info("Nenhum contrato cadastrado.")
    else:
        exibir = contratos_lista.copy()
        exibir["inicio_vigencia"] = exibir["inicio_vigencia"].apply(data_br)
        exibir["fim_vigencia"] = exibir["fim_vigencia"].apply(data_br)
        exibir = exibir[["cod_unico", "numero_sei", "inicio_vigencia", "fim_vigencia", "titulo", "status"]]
        exibir.columns = ["COD Único", "Número SEI", "Início", "Fim", "Título", "Status"]
        st.dataframe(exibir, use_container_width=True, hide_index=True)

if menu == "Cadastro de Itens":
    if not pode_cadastrar_item():
        st.error("Somente usuários nível 2 ou nível 0 podem cadastrar itens.")
        st.stop()
    st.title("Cadastro de Itens")
    contratos = carregar_contratos()
    catalogo = carregar_catalogo()
    if contratos.empty or catalogo.empty:
        st.warning("Cadastre contrato e codificação antes de cadastrar itens.")
        st.stop()
    opcoes_catalogo = {f"{row['codigo_item']} - {row['nome_item']}": row["codigo_item"] for _, row in catalogo.iterrows()}
    with st.form("form_item", clear_on_submit=True):
        contrato_cod = st.selectbox("Contrato", contratos["cod_unico"].tolist())
        item_escolhido = st.selectbox("Item do Catálogo", list(opcoes_catalogo.keys()))
        codigo_item = opcoes_catalogo[item_escolhido]
        detalhes = st.text_area("Detalhes do Item")
        c1, c2 = st.columns(2)
        quantidade = c1.number_input("Quantidade Inicial", min_value=0.0, value=0.0, step=1.0)
        valor_unitario = c2.number_input("Valor Unitário", min_value=0.0, value=0.0, step=0.01)
        salvar = st.form_submit_button("Cadastrar item", use_container_width=True)
        if salvar:
            valor_total = quantidade * valor_unitario
            execute_query("""
                INSERT INTO itens(contrato_cod_unico, codigo_item, detalhes_item, quantidade, valor_unitario, valor_total, saldo_quantidade, saldo_valor)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (contrato_cod, codigo_item, detalhes.strip(), quantidade, valor_unitario, valor_total, quantidade, valor_total))
            st.success("Item cadastrado com sucesso.")


    st.divider()
    st.subheader("Itens cadastrados")
    itens_lista = carregar_itens()
    if itens_lista.empty:
        st.info("Nenhum item cadastrado.")
    else:
        cols_exibir = [
            "numero_sei", "nome_item", "nome_padrao_descritivo", "nome_categoria",
            "detalhes_item", "quantidade", "saldo_quantidade", "saldo_valor", "status"
        ]
        exibir = itens_lista[[c for c in cols_exibir if c in itens_lista.columns]].copy()
        rename_map = {
            "numero_sei": "Número SEI",
            "nome_item": "Item",
            "nome_padrao_descritivo": "Padrão Descritivo",
            "nome_categoria": "Categoria",
            "detalhes_item": "Detalhes",
            "quantidade": "Quantidade Inicial",
            "saldo_quantidade": "Saldo Atual",
            "saldo_valor": "Saldo Financeiro",
            "status": "Status"
        }
        exibir = exibir.rename(columns=rename_map)
        if "Saldo Financeiro" in exibir.columns:
            exibir["Saldo Financeiro"] = exibir["Saldo Financeiro"].apply(brl)
        st.dataframe(exibir, use_container_width=True, hide_index=True)


if menu == "Editar Contratos":
    if not pode_editar_dados():
        st.error("Somente usuários nível 0 e 1 podem editar contratos.")
        st.stop()

    st.title("Editar Contratos")
    contratos = carregar_contratos()

    if contratos.empty:
        st.info("Nenhum contrato cadastrado para edição.")
        st.stop()

    contrato_sel = st.selectbox(
        "Selecione o contrato",
        contratos.to_dict("records"),
        format_func=lambda x: f"{x['numero_sei']} - {x['titulo']} - {x['status']}"
    )

    with st.form("form_editar_contrato"):
        cod_unico = st.text_input("COD Único", value=contrato_sel["cod_unico"])
        numero_sei = st.text_input("Número do SEI", value=contrato_sel["numero_sei"])
        titulo = st.text_input("Título", value=contrato_sel["titulo"])
        c1, c2 = st.columns(2)
        inicio_txt = c1.text_input("Início da Vigência (DD-MM-YYYY)", value=data_br(contrato_sel["inicio_vigencia"]))
        fim_txt = c2.text_input("Fim da Vigência (DD-MM-YYYY)", value=data_br(contrato_sel["fim_vigencia"]))
        salvar = st.form_submit_button("Salvar alterações", use_container_width=True)

        if salvar:
            inicio = parse_data_br(inicio_txt)
            fim = parse_data_br(fim_txt)

            if not all([cod_unico.strip(), numero_sei.strip(), titulo.strip(), inicio, fim]):
                st.warning("Preencha todos os campos corretamente.")
            elif fim < inicio:
                st.error("A data final não pode ser menor que a data inicial.")
            else:
                status = normalizar_status(inicio, fim)
                cod_antigo = contrato_sel["cod_unico"]

                try:
                    if cod_antigo != cod_unico.strip():
                        run_many([
                            ("""
                                UPDATE contratos
                                SET cod_unico=%s, numero_sei=%s, inicio_vigencia=%s, fim_vigencia=%s, titulo=%s, status=%s
                                WHERE id=%s
                            """, (cod_unico.strip(), numero_sei.strip(), inicio, fim, titulo.strip(), status, int(contrato_sel["id"]))),
                            ("UPDATE itens SET contrato_cod_unico=%s WHERE contrato_cod_unico=%s", (cod_unico.strip(), cod_antigo)),
                            ("UPDATE requisicoes SET contrato_cod_unico=%s WHERE contrato_cod_unico=%s", (cod_unico.strip(), cod_antigo)),
                        ])
                    else:
                        execute_query("""
                            UPDATE contratos
                            SET cod_unico=%s, numero_sei=%s, inicio_vigencia=%s, fim_vigencia=%s, titulo=%s, status=%s
                            WHERE id=%s
                        """, (cod_unico.strip(), numero_sei.strip(), inicio, fim, titulo.strip(), status, int(contrato_sel["id"])))

                    st.success("Contrato atualizado com sucesso.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Erro ao atualizar contrato: {e}")

    st.warning("A exclusão removerá o contrato e seus itens/requisições vinculados.")
    if st.button("Excluir contrato selecionado", type="primary", use_container_width=True):
        excluir_contrato(contrato_sel["cod_unico"])
        st.success("Contrato excluído com sucesso.")
        st.rerun()

    st.divider()
    st.subheader("Base de contratos")
    exibir = contratos.copy()
    exibir["inicio_vigencia"] = exibir["inicio_vigencia"].apply(data_br)
    exibir["fim_vigencia"] = exibir["fim_vigencia"].apply(data_br)
    st.dataframe(exibir, use_container_width=True, hide_index=True)


if menu == "Editar Itens":
    if not pode_editar_dados():
        st.error("Somente usuários nível 0 e 1 podem editar itens.")
        st.stop()

    st.title("Editar Itens")
    itens = carregar_itens()
    contratos = carregar_contratos()
    catalogo = carregar_catalogo()

    if itens.empty:
        st.info("Nenhum item cadastrado para edição.")
        st.stop()
    if contratos.empty or catalogo.empty:
        st.warning("É necessário haver contratos e catálogo cadastrados.")
        st.stop()

    item_sel = st.selectbox(
        "Selecione o item",
        itens.to_dict("records"),
        format_func=lambda x: f"SEI {x['numero_sei']} | {x['nome_item']} | {str(x['detalhes_item'])[:60]}"
    )

    opcoes_contratos = contratos["cod_unico"].astype(str).tolist()
    opcoes_catalogo = {f"{row['codigo_item']} - {row['nome_item']}": row["codigo_item"] for _, row in catalogo.iterrows()}
    labels_catalogo = list(opcoes_catalogo.keys())
    label_atual = next((k for k, v in opcoes_catalogo.items() if str(v) == str(item_sel["codigo_item"])), labels_catalogo[0])

    with st.form("form_editar_item"):
        contrato_cod = st.selectbox(
            "Contrato",
            opcoes_contratos,
            index=opcoes_contratos.index(str(item_sel["contrato_cod_unico"])) if str(item_sel["contrato_cod_unico"]) in opcoes_contratos else 0
        )
        item_catalogo = st.selectbox(
            "Item do Catálogo",
            labels_catalogo,
            index=labels_catalogo.index(label_atual) if label_atual in labels_catalogo else 0
        )
        detalhes = st.text_area("Detalhes do Item", value=str(item_sel["detalhes_item"] or ""))
        c1, c2 = st.columns(2)
        quantidade = c1.number_input("Quantidade Inicial", min_value=0.0, value=float(item_sel["quantidade"] or 0), step=1.0)
        valor_unitario = c2.number_input("Valor Unitário", min_value=0.0, value=float(item_sel["valor_unitario"] or 0), step=0.01)
        salvar = st.form_submit_button("Salvar alterações", use_container_width=True)

        if salvar:
            codigo_item = opcoes_catalogo[item_catalogo]
            aprovado = run_query("""
                SELECT COALESCE(SUM(quantidade_solicitada), 0) AS total_aprovado
                FROM requisicoes
                WHERE item_id = %s AND status = 'APROVADA'
            """, (int(item_sel["id"]),), fetch="one")
            aprovado = float((aprovado or {}).get("total_aprovado", 0) or 0)

            if quantidade < aprovado:
                st.error(f"A quantidade inicial não pode ser menor que o total já aprovado ({aprovado}).")
            else:
                saldo_quantidade = quantidade - aprovado
                valor_total = quantidade * valor_unitario
                saldo_valor = saldo_quantidade * valor_unitario

                run_many([
                    ("""
                        UPDATE itens
                        SET contrato_cod_unico=%s, codigo_item=%s, detalhes_item=%s,
                            quantidade=%s, valor_unitario=%s, valor_total=%s,
                            saldo_quantidade=%s, saldo_valor=%s
                        WHERE id=%s
                    """, (
                        contrato_cod, codigo_item, detalhes.strip(),
                        quantidade, valor_unitario, valor_total,
                        saldo_quantidade, saldo_valor, int(item_sel["id"])
                    )),
                    ("UPDATE requisicoes SET contrato_cod_unico=%s, codigo_item=%s WHERE item_id=%s",
                     (contrato_cod, codigo_item, int(item_sel["id"])))
                ])
                st.success("Item atualizado com sucesso.")
                st.rerun()

    st.warning("A exclusão removerá o item e suas requisições vinculadas.")
    if st.button("Excluir item selecionado", type="primary", use_container_width=True):
        excluir_item(item_sel["id"])
        st.success("Item excluído com sucesso.")
        st.rerun()

    st.divider()
    st.subheader("Base de itens")
    st.dataframe(itens, use_container_width=True, hide_index=True)


if menu == "Editar Catálogo":
    if not pode_editar_dados():
        st.error("Somente usuários nível 0 e 1 podem editar catálogo.")
        st.stop()

    st.title("Editar Catálogo")
    catalogo = carregar_catalogo()
    padroes = carregar_padroes()

    if catalogo.empty:
        st.info("Nenhum item do catálogo cadastrado. Cadastre primeiro em Codificação > Catálogo.")
        st.stop()
    if padroes.empty:
        st.info("Nenhum padrão descritivo cadastrado.")
        st.stop()

    item_sel = st.selectbox(
        "Selecione o item do catálogo",
        catalogo.to_dict("records"),
        format_func=lambda x: f"{x['codigo_item']} - {x['nome_item']} | {x['nome_padrao_descritivo']}"
    )

    mapa_padroes = {f"{row['codigo_padrao_descritivo']} - {row['nome_padrao_descritivo']}": row["id"] for _, row in padroes.iterrows()}
    labels_padroes = list(mapa_padroes.keys())
    label_padrao_atual = next((k for k, v in mapa_padroes.items() if int(v) == int(item_sel["padrao_descritivo_id"])), labels_padroes[0])

    with st.form("form_editar_catalogo"):
        codigo_item = st.text_input("Código do Item", value=str(item_sel["codigo_item"]))
        nome_item = st.text_input("Nome do Item", value=str(item_sel["nome_item"]))
        padrao_sel = st.selectbox(
            "Padrão Descritivo",
            labels_padroes,
            index=labels_padroes.index(label_padrao_atual) if label_padrao_atual in labels_padroes else 0
        )
        salvar = st.form_submit_button("Salvar alterações", use_container_width=True)

        if salvar:
            codigo_antigo = item_sel["codigo_item"]
            novo_padrao_id = mapa_padroes[padrao_sel]

            try:
                ops = [("""
                    UPDATE catalogo
                    SET codigo_item=%s, nome_item=%s, padrao_descritivo_id=%s
                    WHERE id=%s
                """, (codigo_item.strip(), nome_item.strip(), int(novo_padrao_id), int(item_sel["id"])))]

                if str(codigo_antigo) != codigo_item.strip():
                    ops.extend([
                        ("UPDATE itens SET codigo_item=%s WHERE codigo_item=%s", (codigo_item.strip(), codigo_antigo)),
                        ("UPDATE requisicoes SET codigo_item=%s WHERE codigo_item=%s", (codigo_item.strip(), codigo_antigo)),
                    ])

                run_many(ops)
                st.success("Catálogo atualizado com sucesso.")
                st.rerun()
            except Exception as e:
                st.error(f"Erro ao atualizar catálogo: {e}")

    st.warning("A exclusão pode falhar caso o item esteja vinculado a itens/requisições.")
    if st.button("Excluir item do catálogo selecionado", type="primary", use_container_width=True):
        try:
            excluir_catalogo(item_sel["codigo_item"])
            st.success("Item do catálogo excluído com sucesso.")
            st.rerun()
        except Exception as e:
            st.error(f"Não foi possível excluir. Verifique vínculos existentes. Detalhe: {e}")

    st.divider()
    st.subheader("Itens do catálogo")
    st.dataframe(catalogo, use_container_width=True, hide_index=True)


if menu == "Editar Requisições":
    if not is_admin():
        st.error("Somente usuários nível 0 podem editar e excluir requisições.")
        st.stop()

    st.title("Editar Requisições")
    req = carregar_requisicoes()

    if req.empty:
        st.info("Nenhuma requisição registrada.")
        st.stop()

    req_sel = st.selectbox(
        "Selecione a requisição",
        req.to_dict("records"),
        format_func=lambda x: f"ID {x['id']} | {x['numero_sei']} | {x['nome_item']} | {x['status']}"
    )

    item_row = run_query("""
        SELECT id, saldo_quantidade, valor_unitario
        FROM itens
        WHERE id = %s
    """, (int(req_sel["item_id"]),), fetch="one")

    valor_unit_item = float(item_row["valor_unitario"] or 0) if item_row else 0.0
    qtd_atual = float(req_sel["quantidade_solicitada"] or 0)

    with st.form("form_editar_requisicao"):
        nova_quantidade = st.number_input("Quantidade solicitada", min_value=0.0, value=qtd_atual, step=1.0)
        nova_justificativa = st.text_area("Justificativa", value=str(req_sel["justificativa"] or ""))
        opcoes_status = ["PENDENTE", "APROVADA", "REJEITADA"]
        status_atual = str(req_sel["status"] or "PENDENTE").upper()
        novo_status = st.selectbox("Status", opcoes_status, index=opcoes_status.index(status_atual) if status_atual in opcoes_status else 0)
        nova_observacao = st.text_area("Observação da análise", value=str(req_sel["observacao_aprovacao"] or ""))
        salvar = st.form_submit_button("Salvar alterações", use_container_width=True)

        if salvar:
            if nova_quantidade <= 0:
                st.warning("Informe uma quantidade maior que zero.")
            elif not nova_justificativa.strip():
                st.warning("Informe a justificativa.")
            else:
                valor_estimado = float(nova_quantidade) * valor_unit_item
                if novo_status in ["APROVADA", "REJEITADA"]:
                    execute_query("""
                        UPDATE requisicoes
                        SET quantidade_solicitada=%s, valor_estimado=%s, justificativa=%s,
                            status=%s, usuario_aprovador=%s, data_aprovacao=CURRENT_TIMESTAMP,
                            observacao_aprovacao=%s
                        WHERE id=%s
                    """, (
                        float(nova_quantidade), valor_estimado, nova_justificativa.strip(),
                        novo_status, st.session_state.usuario, nova_observacao.strip(), int(req_sel["id"])
                    ))
                else:
                    execute_query("""
                        UPDATE requisicoes
                        SET quantidade_solicitada=%s, valor_estimado=%s, justificativa=%s,
                            status=%s, usuario_aprovador=NULL, data_aprovacao=NULL,
                            observacao_aprovacao=%s
                        WHERE id=%s
                    """, (
                        float(nova_quantidade), valor_estimado, nova_justificativa.strip(),
                        novo_status, nova_observacao.strip(), int(req_sel["id"])
                    ))

                recalc_item_balance(int(req_sel["item_id"]))
                st.success("Requisição atualizada com sucesso.")
                st.rerun()

    if st.button("Excluir requisição selecionada", type="primary", use_container_width=True):
        execute_query("DELETE FROM requisicoes WHERE id = %s", (int(req_sel["id"]),))
        recalc_item_balance(int(req_sel["item_id"]))
        st.success("Requisição excluída com sucesso.")
        st.rerun()

    st.divider()
    st.subheader("Base de requisições")
    st.dataframe(req, use_container_width=True, hide_index=True)


if menu == "Codificação":
    if not pode_cadastrar_codificacao():
        st.error("Somente usuários nível 0 podem acessar a codificação.")
        st.stop()
    st.title("Codificação")
    tab1, tab2, tab3, tab4 = st.tabs(["Categorias", "Classes", "Padrões Descritivos", "Catálogo"])
    with tab1:
        with st.form("form_categoria", clear_on_submit=True):
            codigo_categoria = st.text_input("Código da Categoria")
            nome_categoria = st.text_input("Nome da Categoria")
            if st.form_submit_button("Salvar categoria", use_container_width=True):
                execute_query("INSERT INTO categorias(codigo_categoria, nome_categoria) VALUES (%s, %s)", (codigo_categoria.strip(), nome_categoria.strip()))
                st.success("Categoria cadastrada com sucesso.")
    with tab2:
        categorias = carregar_categorias()
        if not categorias.empty:
            with st.form("form_classe", clear_on_submit=True):
                codigo_classe = st.text_input("Código da Classe")
                nome_classe = st.text_input("Nome da Classe")
                categoria_sel = st.selectbox("Categoria", categorias.to_dict("records"), format_func=lambda x: f"{x['codigo_categoria']} - {x['nome_categoria']}")
                if st.form_submit_button("Salvar classe", use_container_width=True):
                    execute_query("INSERT INTO classes(codigo_classe, nome_classe, categoria_id) VALUES (%s, %s, %s)", (codigo_classe.strip(), nome_classe.strip(), int(categoria_sel["id"])))
                    st.success("Classe cadastrada com sucesso.")
    with tab3:
        classes = carregar_classes()
        if not classes.empty:
            with st.form("form_padrao", clear_on_submit=True):
                codigo_padrao = st.text_input("Código do Padrão Descritivo")
                nome_padrao = st.text_input("Nome do Padrão Descritivo")
                classe_sel = st.selectbox("Classe", classes.to_dict("records"), format_func=lambda x: f"{x['codigo_classe']} - {x['nome_classe']}")
                if st.form_submit_button("Salvar padrão", use_container_width=True):
                    execute_query("INSERT INTO padroes_descritivos(codigo_padrao_descritivo, nome_padrao_descritivo, classe_id) VALUES (%s, %s, %s)", (codigo_padrao.strip(), nome_padrao.strip(), int(classe_sel["id"])))
                    st.success("Padrão cadastrado com sucesso.")
    with tab4:
        padroes = carregar_padroes()
        if not padroes.empty:
            with st.form("form_catalogo", clear_on_submit=True):
                codigo_item = st.text_input("Código do Item")
                nome_item = st.text_input("Nome do Item")
                padrao_sel = st.selectbox("Padrão Descritivo", padroes.to_dict("records"), format_func=lambda x: f"{x['codigo_padrao_descritivo']} - {x['nome_padrao_descritivo']}")
                if st.form_submit_button("Salvar item do catálogo", use_container_width=True):
                    execute_query("INSERT INTO catalogo(codigo_item, nome_item, padrao_descritivo_id) VALUES (%s, %s, %s)", (codigo_item.strip(), nome_item.strip(), int(padrao_sel["id"])))
                    st.success("Item do catálogo cadastrado com sucesso.")

if menu == "Usuários":
    if not is_admin():
        st.error("Somente usuários nível 0 podem gerenciar usuários.")
        st.stop()
    st.title("Usuários")
    with st.form("form_usuario", clear_on_submit=True):
        username = st.text_input("Usuário")
        senha = st.text_input("Senha", type="password")
        nivel = st.selectbox("Nível", [0, 1, 2])
        if st.form_submit_button("Cadastrar usuário", use_container_width=True):
            senha_hash = bcrypt.hashpw(senha.encode(), bcrypt.gensalt()).decode()
            execute_query("INSERT INTO usuarios(username, password, nivel) VALUES (%s, %s, %s)", (username.strip(), senha_hash, int(nivel)))
            st.success("Usuário cadastrado com sucesso.")
