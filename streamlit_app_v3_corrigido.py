import streamlit as st
import os
import psycopg
from psycopg.rows import dict_row
import pandas as pd
import bcrypt
from datetime import date, datetime
from io import BytesIO
import unicodedata
import re
from difflib import SequenceMatcher
import base64
import uuid
from pathlib import Path

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, HRFlowable

APP_TITLE = "Sistema de Gestão de ARPs, Requisições e Catálogo"
LOGO_PATH = "/mnt/data/logo-centra-de-compras.svg"

COR_AZUL = "#164194"
COR_AMARELO = "#F7B600"
COR_VERMELHO = "#E63312"
COR_VERDE = "#107527"
COR_TEXTO = "#1D1D1B"
COR_FUNDO = "#F6F8FC"
COR_CARD = "#FFFFFF"
COR_BORDA = "#D9E1F2"

st.set_page_config(page_title=APP_TITLE, layout="wide")


# =========================================================
# HELPERS
# =========================================================
def brl(valor):
    try:
        valor = float(valor or 0)
    except Exception:
        valor = 0.0
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")




def normalizar_hash_bcrypt(valor):
    """
    Normaliza o hash salvo no PostgreSQL/Neon para bytes.
    Necessário porque BYTEA pode retornar como bytes, memoryview ou str,
    especialmente quando houve migração entre SQLite/PostgreSQL.
    """
    if valor is None:
        return None

    if isinstance(valor, bytes):
        return valor

    if isinstance(valor, bytearray):
        return bytes(valor)

    if isinstance(valor, memoryview):
        return valor.tobytes()

    if isinstance(valor, str):
        txt = valor.strip()

        # Caso venha como "\\x..." hexadecimal de BYTEA.
        if txt.startswith("\\x"):
            try:
                return bytes.fromhex(txt[2:])
            except Exception:
                return txt.encode("utf-8")

        # Caso venha como "b'$2b$...'"
        if (txt.startswith("b'") and txt.endswith("'")) or (txt.startswith('b"') and txt.endswith('"')):
            txt = txt[2:-1]

        return txt.encode("utf-8")

    try:
        return bytes(valor)
    except Exception:
        return str(valor).encode("utf-8")


def data_br(data):
    if not data:
        return ""
    try:
        if isinstance(data, str):
            txt = data.strip()
            for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(txt[:19], fmt).strftime("%d-%m-%Y")
                except Exception:
                    continue
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
    """
    Aceita:
    - DDMMAAAA, exemplo: 31122026
    - DD-MM-AAAA, exemplo: 31-12-2026
    - DD/MM/AAAA, exemplo: 31/12/2026
    """
    txt = str(texto or "").strip()
    if not txt:
        return None

    formatos = ["%d%m%Y", "%d-%m-%Y", "%d/%m/%Y"]

    for fmt in formatos:
        try:
            return datetime.strptime(txt, fmt).date()
        except Exception:
            pass

    return None


def gerar_cod_unico():
    return f"ARP-{datetime.now().strftime('%Y%m%d')}-{uuid.uuid4().hex[:8].upper()}"


def validar_codigo_sei(codigo):
    """
    Formato correto do SEI:
    00000.000000/AAAA-00
    Exemplo: 00002.004441/2024-46
    """
    return re.fullmatch(r"\d{5}\.\d{6}/\d{4}-\d{2}", str(codigo or "").strip()) is not None


def normalizar_status(inicio, fim):
    """
    Atualiza status de forma dinâmica:
    - VENCIDA: fim da vigência anterior à data atual
    - PRÓXIMO AO VENCIMENTO: fim da vigência entre hoje e os próximos 30 dias
    - VIGENTE: demais casos
    """
    try:
        if isinstance(fim, str):
            txt = fim.strip()
            for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d%m%Y", "%Y-%m-%d %H:%M:%S"):
                try:
                    fim_dt = datetime.strptime(txt[:19], fmt).date()
                    break
                except Exception:
                    fim_dt = None
            if fim_dt is None:
                fim_dt = pd.to_datetime(fim, dayfirst=True).date()
        else:
            fim_dt = pd.to_datetime(fim).date()
    except Exception:
        return "VIGENTE"

    hoje = date.today()
    dias_restantes = (fim_dt - hoje).days

    if dias_restantes < 0:
        return "VENCIDA"
    elif dias_restantes <= 30:
        return "PRÓXIMO AO VENCIMENTO"
    else:
        return "VIGENTE"


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
    """
    Busca tolerante a grafia:
    - remove acentos e pontuação
    - compara por inclusão direta
    - exige que os termos principais da consulta apareçam no texto
    - usa similaridade como apoio
    """
    consulta_n = normalizar_texto(consulta)
    texto_n = normalizar_texto(texto)

    if not consulta_n:
        return True

    if consulta_n in texto_n:
        return True

    termos = [t for t in consulta_n.split() if len(t) > 1]

    if termos:
        # Exige todos os termos principais no texto consolidado.
        if all(t in texto_n for t in termos):
            return True

        # Permite pequena tolerância quando a busca tem 3 ou mais termos.
        hits = sum(1 for t in termos if t in texto_n)
        if len(termos) >= 3 and hits >= len(termos) - 1:
            return True

    if similaridade(consulta_n, texto_n) >= 0.70:
        return True

    palavras_texto = texto_n.split()
    for termo in termos:
        if any(similaridade(termo, palavra) >= 0.84 for palavra in palavras_texto):
            continue
        break
    else:
        if termos:
            return True

    return False


def status_badge_html(status):
    status = str(status or "").upper()
    if status == "VIGENTE":
        classe = "status-vigente"
    elif status == "VENCIDA":
        classe = "status-vencida"
    elif status == "PRÓXIMO AO VENCIMENTO":
        classe = "status-proximo"
    else:
        classe = "status-pendente"
    return f'<span class="status-pill {classe}">{status}</span>'


def status_badge_df(status):
    return str(status or "").upper()


def get_logo_data_uri():
    try:
        content = Path(LOGO_PATH).read_text(encoding="utf-8")
        encoded = base64.b64encode(content.encode("utf-8")).decode("utf-8")
        return f"data:image/svg+xml;base64,{encoded}"
    except Exception:
        return ""


def _pg_sql(sql):
    """Traduz SQL estilo SQLite usado no app para PostgreSQL/Neon."""
    sql = str(sql)

    # DDL SQLite -> PostgreSQL
    sql = sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
    sql = sql.replace("BLOB", "BYTEA")
    sql = re.sub(r"\bREAL\b", "DOUBLE PRECISION", sql)

    # O status é calculado dinamicamente; sem CHECK para permitir 'PRÓXIMO AO VENCIMENTO'.
    sql = sql.replace(
        "status TEXT NOT NULL",
        "status TEXT NOT NULL"
    )

    # INSERT OR IGNORE do SQLite -> PostgreSQL
    sql = re.sub(
        r"INSERT\s+OR\s+IGNORE\s+INTO\s+(.+?)\s+VALUES\s*\((.*?)\)",
        r"INSERT INTO \1 VALUES (\2) ON CONFLICT DO NOTHING",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )

    # placeholders SQLite (?) -> PostgreSQL (%s)
    sql = sql.replace("?", "%s")
    return sql


class PgCursorCompat:
    def __init__(self, cursor):
        self._cursor = cursor

    def execute(self, sql, params=None):
        self._cursor.execute(_pg_sql(sql), params or None)
        return self

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    @property
    def description(self):
        return self._cursor.description

    def close(self):
        self._cursor.close()


class PgConnCompat:
    def __init__(self, raw):
        self.raw = raw

    def execute(self, sql, params=None):
        return self.raw.execute(_pg_sql(sql), params or None)

    def cursor(self):
        return PgCursorCompat(self.raw.cursor())

    def commit(self):
        try:
            self.raw.commit()
        except Exception:
            pass

    def rollback(self):
        try:
            self.raw.rollback()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type:
            self.rollback()
        else:
            self.commit()
        return False


def conectado():
    database_url = None
    try:
        database_url = st.secrets.get("DATABASE_URL")
    except Exception:
        database_url = None

    database_url = database_url or os.environ.get("DATABASE_URL")

    if not database_url:
        st.error("DATABASE_URL não configurada. Cadastre a variável em Secrets do Streamlit Cloud.")
        st.stop()

    raw = psycopg.connect(database_url, row_factory=dict_row, autocommit=True)
    return PgConnCompat(raw)


def read_sql(sql, _conn=None, params=None, **kwargs):
    """Substitui pd.read_sql para operar de forma controlada com psycopg/Neon."""
    cur = conn.execute(sql, params=params)
    rows = cur.fetchall()
    if not rows:
        cols = [d.name if hasattr(d, "name") else d[0] for d in (cur.description or [])]
        return pd.DataFrame(columns=cols)
    return pd.DataFrame(rows)


def get_columns(conn, table_name):
    cur = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table_name,)
    )
    return [row["column_name"] for row in cur.fetchall()]


def ensure_column(conn, table_name, column_name, column_def):
    cols = get_columns(conn, table_name)
    if column_name not in cols:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")
        conn.commit()


def remover_checks_antigos_status():
    """
    Remove CHECK antigo do campo status, caso exista no Neon.
    Necessário porque agora existe o status 'PRÓXIMO AO VENCIMENTO'.
    """
    try:
        cur = conn.execute(
            """
            SELECT conname
            FROM pg_constraint
            WHERE conrelid = 'contratos'::regclass
              AND contype = 'c'
              AND pg_get_constraintdef(oid) ILIKE %s
            """,
            ("%status%",)
        )
        for row in cur.fetchall():
            conn.execute(f"ALTER TABLE contratos DROP CONSTRAINT IF EXISTS {row['conname']}")
        conn.commit()
    except Exception:
        conn.rollback()


def recalc_item_balance(item_id):
    row = conn.execute("""
        SELECT id, quantidade, valor_unitario
        FROM itens
        WHERE id = ?
    """, (int(item_id),)).fetchone()

    if row is None:
        return

    qtd_total = float(row["quantidade"] or 0)
    valor_unit = float(row["valor_unitario"] or 0)

    aprovado = conn.execute("""
        SELECT COALESCE(SUM(quantidade_solicitada), 0)
        FROM requisicoes
        WHERE item_id = ?
          AND status = 'APROVADA'
    """, (int(item_id),)).fetchone()[0]

    aprovado = float(aprovado or 0)
    saldo_qtd = max(qtd_total - aprovado, 0)
    saldo_valor = saldo_qtd * valor_unit

    conn.execute("""
        UPDATE itens
        SET saldo_quantidade = ?, saldo_valor = ?, valor_total = ?
        WHERE id = ?
    """, (saldo_qtd, saldo_valor, qtd_total * valor_unit, int(item_id)))
    conn.commit()

def recalc_all_balances():
    ids = read_sql("SELECT id FROM itens", conn)
    for _, row in ids.iterrows():
        recalc_item_balance(int(row["id"]))



def excluir_contrato(cod_unico):
    """
    Exclui uma ARP e todos os itens/requisições vinculados.
    Mantém a consistência do banco SQLite.
    """
    itens_vinculados = read_sql(
        "SELECT id FROM itens WHERE contrato_cod_unico = ?",
        conn,
        params=(cod_unico,)
    )

    for _, item in itens_vinculados.iterrows():
        conn.execute(
            "DELETE FROM requisicoes WHERE item_id = ?",
            (int(item["id"]),)
        )

    conn.execute(
        "DELETE FROM itens WHERE contrato_cod_unico = ?",
        (cod_unico,)
    )

    conn.execute(
        "DELETE FROM contratos WHERE cod_unico = ?",
        (cod_unico,)
    )

    conn.commit()


def excluir_item(item_id):
    """
    Exclui um item operacional e suas requisições vinculadas.
    """
    conn.execute(
        "DELETE FROM requisicoes WHERE item_id = ?",
        (int(item_id),)
    )
    conn.execute(
        "DELETE FROM itens WHERE id = ?",
        (int(item_id),)
    )
    conn.commit()


def excluir_catalogo(codigo_item):
    """
    Exclui um item do catálogo e todos os itens/requisições vinculados a ele.
    """
    itens_vinculados = read_sql(
        "SELECT id FROM itens WHERE codigo_item = ?",
        conn,
        params=(codigo_item,)
    )

    for _, item in itens_vinculados.iterrows():
        conn.execute(
            "DELETE FROM requisicoes WHERE item_id = ?",
            (int(item["id"]),)
        )

    conn.execute(
        "DELETE FROM itens WHERE codigo_item = ?",
        (codigo_item,)
    )
    conn.execute(
        "DELETE FROM catalogo WHERE codigo_item = ?",
        (codigo_item,)
    )
    conn.commit()

def excluir_categoria(categoria_id):
    """
    Exclui categoria e todos os vínculos descendentes:
    classes -> padrões -> catálogo -> itens -> requisições.
    """
    padroes = read_sql("""
        SELECT pd.id, cat.codigo_item
        FROM classes cl
        LEFT JOIN padroes_descritivos pd ON pd.classe_id = cl.id
        LEFT JOIN catalogo cat ON cat.padrao_descritivo_id = pd.id
        WHERE cl.categoria_id = ?
    """, conn, params=(int(categoria_id),))

    for _, row in padroes.dropna(subset=["codigo_item"]).iterrows():
        excluir_catalogo(row["codigo_item"])

    conn.execute("""
        DELETE FROM padroes_descritivos
        WHERE classe_id IN (
            SELECT id FROM classes WHERE categoria_id = ?
        )
    """, (int(categoria_id),))
    conn.execute("DELETE FROM classes WHERE categoria_id = ?", (int(categoria_id),))
    conn.execute("DELETE FROM categorias WHERE id = ?", (int(categoria_id),))
    conn.commit()


def excluir_classe(classe_id):
    """
    Exclui classe e todos os vínculos descendentes:
    padrões -> catálogo -> itens -> requisições.
    """
    catalogo_vinculado = read_sql("""
        SELECT cat.codigo_item
        FROM padroes_descritivos pd
        LEFT JOIN catalogo cat ON cat.padrao_descritivo_id = pd.id
        WHERE pd.classe_id = ?
    """, conn, params=(int(classe_id),))

    for _, row in catalogo_vinculado.dropna(subset=["codigo_item"]).iterrows():
        excluir_catalogo(row["codigo_item"])

    conn.execute("DELETE FROM padroes_descritivos WHERE classe_id = ?", (int(classe_id),))
    conn.execute("DELETE FROM classes WHERE id = ?", (int(classe_id),))
    conn.commit()


def excluir_padrao_descritivo(padrao_id):
    """
    Exclui padrão descritivo e todos os vínculos descendentes:
    catálogo -> itens -> requisições.
    """
    catalogo_vinculado = read_sql("""
        SELECT codigo_item
        FROM catalogo
        WHERE padrao_descritivo_id = ?
    """, conn, params=(int(padrao_id),))

    for _, row in catalogo_vinculado.iterrows():
        excluir_catalogo(row["codigo_item"])

    conn.execute("DELETE FROM padroes_descritivos WHERE id = ?", (int(padrao_id),))
    conn.commit()



# =========================================================
# PDF
# =========================================================
def _pdf_add_logo(elements, styles):
    # Compatível com todos os PDFs do sistema.
    # Alguns relatórios usam PdfHeader, outros usam HistHeader.
    estilo_header = (
        styles["PdfHeader"]
        if "PdfHeader" in styles.byName
        else styles["HistHeader"]
        if "HistHeader" in styles.byName
        else styles["Title"]
    )
    elements.append(Paragraph("<b>Central de Compras</b>", estilo_header))


def gerar_pdf_consulta_ARPs(df, filtros_texto, texto_inexistencia=None, justificativa="", usuario="Usuário não identificado"):
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=1.5 * cm,
        rightMargin=1.5 * cm,
        topMargin=1.2 * cm,
        bottomMargin=1.2 * cm
    )

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
    elementos.append(Paragraph("Consulta Pública de ARPs e Itens", styles["PdfTitle"]))
    elementos.append(Paragraph(f"Filtros aplicados: {filtros_texto}", styles["PdfSmall"]))
    if justificativa.strip():
        elementos.append(Paragraph(f"Justificativa: {justificativa}", styles["PdfSmall"]))
    elementos.append(Paragraph(f"Usuário responsável pela emissão: {usuario or 'Usuário não identificado'}", styles["PdfSmall"]))
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
            elementos.append(Paragraph(
                f"<b>{row['numero_sei']} | {row['titulo']}</b> | Status: {row['status']}",
                styles["PdfSection"]
            ))
            elementos.append(Paragraph(
                f"Vigência: {data_br(row['inicio_vigencia'])} até {data_br(row['fim_vigencia'])}",
                styles["PdfBody"]
            ))

            itens = row.get("itens_exportacao", [])
            if itens:
                elementos.append(Paragraph("<b>Itens localizados:</b>", styles["PdfBody"]))
                for item in itens:
                    elementos.append(Paragraph(
                        f"• <b>{item['nome_item']}</b> | Padrão Descritivo: {item['nome_padrao_descritivo']} | "
                        f"Detalhes: {item['detalhes_item']}",
                        styles["PdfItem"]
                    ))
            else:
                elementos.append(Paragraph("• Nenhum item vinculado.", styles["PdfItem"]))

            elementos.append(Spacer(1, 0.18 * cm))

    doc.build(elementos)
    buffer.seek(0)
    return buffer.getvalue()




def registrar_historico_consulta_arps(contratos_df, itens_df, filtros, busca_inteligente=""):
    """
    Registra cada consulta realizada no módulo ARPs, com ou sem resultado.
    O histórico guarda:
    - data/hora
    - filtros utilizados
    - busca inteligente
    - resultado ENCONTRADO / NÃO ENCONTRADO
    - quantidade de ARPs e itens localizados
    - resumo dos itens encontrados
    """
    historico = st.session_state.get("historico_consultas_arps", [])

    qtd_arps = 0 if contratos_df is None else len(contratos_df)
    qtd_itens = 0 if itens_df is None else len(itens_df)
    resultado = "ENCONTRADO" if (qtd_arps > 0 or qtd_itens > 0) else "NÃO ENCONTRADO"

    resumo_itens = []
    if itens_df is not None and not itens_df.empty:
        for _, item in itens_df.head(50).iterrows():
            quantidade = float(item.get("quantidade", 0) or 0)
            valor_unitario = float(item.get("valor_unitario", 0) or 0)
            resumo_itens.append({
                "numero_sei": str(item.get("numero_sei", "")),
                "titulo": str(item.get("titulo", "")),
                "status": str(item.get("status", "")),
                "codigo_item": str(item.get("codigo_item", "")),
                "nome_item": str(item.get("nome_item", "")),
                "nome_padrao_descritivo": str(item.get("nome_padrao_descritivo", "")),
                "nome_classe": str(item.get("nome_classe", "")),
                "nome_categoria": str(item.get("nome_categoria", "")),
                "detalhes_item": str(item.get("detalhes_item", "")),
                "quantidade_inicial": quantidade,
                "valor_unitario": valor_unitario,
                "valor_total_inicial": quantidade * valor_unitario,
            })

    registro = {
        "id": uuid.uuid4().hex,
        "data_pesquisa": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
        "busca_inteligente": str(busca_inteligente or "Nenhuma"),
        "filtros": filtros,
        "resultado": resultado,
        "qtd_arps": qtd_arps,
        "qtd_itens": qtd_itens,
        "itens": resumo_itens,
    }

    historico.append(registro)
    st.session_state.historico_consultas_arps = historico[-200:]


def gerar_pdf_historico_consultas_arps(consultas_selecionadas, referencia, usuario):
    """
    Gera PDF com o histórico selecionado de consultas realizadas no módulo ARPs.
    Inclui filtros, busca inteligente e resultado ENCONTRADO / NÃO ENCONTRADO.
    """
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=1.5 * cm,
        rightMargin=1.5 * cm,
        topMargin=1.2 * cm,
        bottomMargin=1.2 * cm
    )

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="HistHeader", fontSize=18, leading=22, textColor=colors.HexColor(COR_AZUL), spaceAfter=8))
    styles.add(ParagraphStyle(name="HistSmall", fontSize=9, leading=12, textColor=colors.HexColor("#4b5563"), spaceAfter=4))
    styles.add(ParagraphStyle(name="HistTitle", fontSize=15, leading=18, textColor=colors.HexColor(COR_AZUL), spaceAfter=8))
    styles.add(ParagraphStyle(name="HistSection", fontSize=11, leading=14, textColor=colors.HexColor(COR_TEXTO), spaceAfter=5))
    styles.add(ParagraphStyle(name="HistBody", fontSize=9, leading=12, textColor=colors.HexColor(COR_TEXTO), spaceAfter=3))
    styles.add(ParagraphStyle(name="HistItem", fontSize=9, leading=12, leftIndent=12, textColor=colors.HexColor(COR_TEXTO), spaceAfter=2))

    elementos = []
    agora = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

    _pdf_add_logo(elementos, styles)
    elementos.append(Paragraph("<b>GOVERNO DO ESTADO</b>", styles["HistHeader"]))
    elementos.append(Paragraph("Histórico de Consultas de ARPs e Itens", styles["HistTitle"]))
    elementos.append(Paragraph(f"Referência (Processo SEI): {referencia}", styles["HistSmall"]))
    elementos.append(Paragraph(f"Usuário responsável pela emissão: {usuario or 'Usuário não identificado'}", styles["HistSmall"]))
    elementos.append(Paragraph(f"Emitido em: {agora}", styles["HistSmall"]))
    elementos.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor(COR_AZUL), spaceBefore=6, spaceAfter=8))

    if not consultas_selecionadas:
        elementos.append(Paragraph("<b>Resultado:</b> Nenhuma consulta selecionada.", styles["HistBody"]))
    else:
        elementos.append(Paragraph(f"<b>Total de consultas selecionadas:</b> {len(consultas_selecionadas)}", styles["HistBody"]))
        elementos.append(Spacer(1, 0.2 * cm))

        for consulta in consultas_selecionadas:
            elementos.append(Paragraph(
                f"<b>Consulta realizada em:</b> {consulta.get('data_pesquisa', '')} | "
                f"<b>Resultado:</b> {consulta.get('resultado', '')}",
                styles["HistSection"]
            ))
            elementos.append(Paragraph(f"<b>Busca inteligente:</b> {consulta.get('busca_inteligente', 'Nenhuma')}", styles["HistBody"]))
            elementos.append(Paragraph(f"<b>Filtros utilizados:</b> {consulta.get('filtros', '')}", styles["HistBody"]))
            elementos.append(Paragraph(
                f"<b>ARPs localizadas:</b> {consulta.get('qtd_arps', 0)} | "
                f"<b>Itens localizados:</b> {consulta.get('qtd_itens', 0)}",
                styles["HistBody"]
            ))

            itens = consulta.get("itens", [])
            if itens:
                elementos.append(Paragraph("<b>Itens associados à consulta:</b>", styles["HistBody"]))
                for item in itens:
                    elementos.append(Paragraph(
                        f"• <b>{item.get('nome_item', '')}</b> | SEI: {item.get('numero_sei', '')} | "
                        f"Status: {item.get('status', '')}",
                        styles["HistItem"]
                    ))
                    elementos.append(Paragraph(
                        f"• Padrão Descritivo: {item.get('nome_padrao_descritivo', '')} | "
                        f"Classe: {item.get('nome_classe', '')} | Categoria: {item.get('nome_categoria', '')}",
                        styles["HistItem"]
                    ))
                    elementos.append(Paragraph(
                        f"• Quantidade Inicial: {item.get('quantidade_inicial', 0)} | "
                        f"Valor Total Inicial: {brl(item.get('valor_total_inicial', 0))}",
                        styles["HistItem"]
                    ))
            else:
                elementos.append(Paragraph("• Nenhum item localizado nesta consulta.", styles["HistItem"]))

            elementos.append(Spacer(1, 0.18 * cm))

    doc.build(elementos)
    buffer.seek(0)
    return buffer.getvalue()


def gerar_pdf_historico_itens(itens_selecionados, referencia, usuario):
    """
    Gera PDF com histórico selecionado de itens pesquisados.
    Não expõe Quantidade Atual nem Valor Disponível.
    """
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=1.5 * cm,
        rightMargin=1.5 * cm,
        topMargin=1.2 * cm,
        bottomMargin=1.2 * cm
    )

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="HistHeader", fontSize=18, leading=22, textColor=colors.HexColor(COR_AZUL), spaceAfter=8))
    styles.add(ParagraphStyle(name="HistSmall", fontSize=9, leading=12, textColor=colors.HexColor("#4b5563"), spaceAfter=4))
    styles.add(ParagraphStyle(name="HistTitle", fontSize=15, leading=18, textColor=colors.HexColor(COR_AZUL), spaceAfter=8))
    styles.add(ParagraphStyle(name="HistSection", fontSize=11, leading=14, textColor=colors.HexColor(COR_TEXTO), spaceAfter=5))
    styles.add(ParagraphStyle(name="HistBody", fontSize=9, leading=12, textColor=colors.HexColor(COR_TEXTO), spaceAfter=3))
    styles.add(ParagraphStyle(name="HistItem", fontSize=9, leading=12, leftIndent=12, textColor=colors.HexColor(COR_TEXTO), spaceAfter=2))

    elementos = []
    agora = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

    _pdf_add_logo(elementos, styles)
    elementos.append(Paragraph("<b>GOVERNO DO ESTADO</b>", styles["HistHeader"]))
    elementos.append(Paragraph("Histórico Selecionado de Itens Pesquisados", styles["HistTitle"]))
    elementos.append(Paragraph(f"Referência (Processo SEI): {referencia}", styles["HistSmall"]))
    elementos.append(Paragraph(f"Usuário responsável pela emissão: {usuario or 'Usuário não identificado'}", styles["HistSmall"]))
    elementos.append(Paragraph(f"Emitido em: {agora}", styles["HistSmall"]))
    elementos.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor(COR_AZUL), spaceBefore=6, spaceAfter=8))

    if not itens_selecionados:
        elementos.append(Paragraph("<b>Resultado:</b> Nenhum item selecionado.", styles["HistBody"]))
    else:
        elementos.append(Paragraph(f"<b>Total de itens selecionados:</b> {len(itens_selecionados)}", styles["HistBody"]))
        elementos.append(Spacer(1, 0.2 * cm))

        for item in itens_selecionados:
            elementos.append(Paragraph(
                f"<b>{item.get('numero_sei', '')} | {item.get('titulo', '')}</b> | Status: {item.get('status', '')}",
                styles["HistSection"]
            ))
            elementos.append(Paragraph(
                f"Pesquisa registrada em: {item.get('data_pesquisa', '')}",
                styles["HistSmall"]
            ))
            elementos.append(Paragraph(
                f"Filtros utilizados: {item.get('filtros', '')}",
                styles["HistSmall"]
            ))
            elementos.append(Paragraph(
                f"• <b>{item.get('nome_item', '')}</b> | Código: {item.get('codigo_item', '')}",
                styles["HistItem"]
            ))
            elementos.append(Paragraph(
                f"• Padrão Descritivo: {item.get('nome_padrao_descritivo', '')}",
                styles["HistItem"]
            ))
            elementos.append(Paragraph(
                f"• Classe: {item.get('nome_classe', '')} | Categoria: {item.get('nome_categoria', '')}",
                styles["HistItem"]
            ))
            elementos.append(Paragraph(
                f"• Detalhes: {item.get('detalhes_item', '')}",
                styles["HistItem"]
            ))
            elementos.append(Paragraph(
                f"• Quantidade Inicial: {item.get('quantidade_inicial', 0)} | "
                f"Valor Unitário: {brl(item.get('valor_unitario', 0))} | "
                f"Valor Total Inicial: {brl(item.get('valor_total_inicial', 0))}",
                styles["HistItem"]
            ))
            elementos.append(Spacer(1, 0.18 * cm))

    doc.build(elementos)
    buffer.seek(0)
    return buffer.getvalue()

# =========================================================
# ESTILO
# =========================================================
def apply_custom_css():
    st.markdown(f"""
    <style>
    .stApp {{
        background: {COR_FUNDO};
        color: {COR_TEXTO};
    }}
    .block-container {{
        padding-top: 1.1rem;
        padding-bottom: 2rem;
    }}
    .status-pill {{
        display:inline-block;
        padding:6px 12px;
        border-radius:999px;
        font-weight:700;
        font-size:12px;
        letter-spacing:0.2px;
        border:1px solid transparent;
        margin-bottom:8px;
    }}
    .status-vigente {{
        background: rgba(16,117,39,0.12);
        color: {COR_VERDE};
        border-color: rgba(16,117,39,0.25);
    }}
    .status-vencida {{
        background: rgba(230,51,18,0.10);
        color: {COR_VERMELHO};
        border-color: rgba(230,51,18,0.25);
    }}
    .status-pendente {{
        background: rgba(247,182,0,0.18);
        color: #7a5900;
        border-color: rgba(247,182,0,0.35);
    }}
    .topo-sistema {{
        background: linear-gradient(135deg, {COR_CARD} 0%, #eef3ff 100%);
        border: 1px solid {COR_BORDA};
        border-radius: 24px;
        padding: 20px 24px;
        margin-bottom: 18px;
        box-shadow: 0 8px 24px rgba(22,65,148,0.08);
    }}
    .topo-grid {{
        display:flex;
        gap:20px;
        align-items:center;
        justify-content:space-between;
        flex-wrap:wrap;
    }}
    .topo-texto h1 {{
        margin:0;
        color:{COR_AZUL};
        font-size:28px;
        line-height:1.1;
    }}
    .topo-texto p {{
        margin:6px 0 0 0;
        color:#4b5563;
        font-size:14px;
    }}
    .logo-topo {{
        max-width:320px;
        width:100%;
        height:auto;
    }}
    .card-info {{
        background:{COR_CARD};
        border:1px solid {COR_BORDA};
        border-radius:18px;
        padding:16px;
        box-shadow:0 2px 10px rgba(0,0,0,0.04);
        margin-bottom:12px;
    }}
    .filtro-box {{
        background:{COR_CARD};
        border:1px solid {COR_BORDA};
        border-radius:18px;
        padding:12px 14px 2px 14px;
        margin-bottom:14px;
        box-shadow:0 2px 10px rgba(0,0,0,0.03);
    }}
    .section-card {{
        background:{COR_CARD};
        border:1px solid {COR_BORDA};
        border-radius:20px;
        padding:18px;
        box-shadow:0 8px 22px rgba(22,65,148,0.05);
        margin-bottom:14px;
    }}
    div[data-testid="stDownloadButton"] > button {{
        background: linear-gradient(135deg, {COR_AZUL} 0%, {COR_VERDE} 100%);
        color: white;
        border: none;
        border-radius: 12px;
        font-weight: 700;
    }}
    div[data-testid="stButton"] > button {{
        border-radius: 12px;
        font-weight: 600;
    }}
    div[data-baseweb="select"] > div, .stTextInput input, .stTextArea textarea {{
        border-radius: 12px !important;
    }}
    .texto-suporte {{
        color:#596579;
        font-size:13px;
    }}
    </style>
    """, unsafe_allow_html=True)


def render_header():
    logo_uri = get_logo_data_uri()
    logo_html = f'<img src="{logo_uri}" class="logo-topo" />' if logo_uri else ""
    st.markdown(f"""
    <div class="topo-sistema">
        <div class="topo-grid">
            <div class="topo-texto">
                <h1>{APP_TITLE}</h1>
                <p>Consulta pública, requisições e gestão operacional da Central de Compras.</p>
            </div>
            <div>{logo_html}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def section_box_start():
    st.markdown('<div class="section-card">', unsafe_allow_html=True)


def section_box_end():
    st.markdown('</div>', unsafe_allow_html=True)


# =========================================================
# BANCO DE DADOS
# =========================================================
conn = conectado()
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS usuarios(
    id SERIAL PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    password BYTEA NOT NULL,
    nivel INTEGER NOT NULL CHECK (nivel IN (0, 1, 2))
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS usuario_modulos(
    id SERIAL PRIMARY KEY,
    username TEXT NOT NULL,
    modulo TEXT NOT NULL,
    permitido INTEGER NOT NULL DEFAULT 1,
    UNIQUE(username, modulo)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS contratos(
    id SERIAL PRIMARY KEY,
    cod_unico TEXT UNIQUE NOT NULL,
    numero_sei TEXT NOT NULL,
    inicio_vigencia TEXT NOT NULL,
    fim_vigencia TEXT NOT NULL,
    titulo TEXT NOT NULL,
    status TEXT NOT NULL,
    criado_em TEXT DEFAULT CURRENT_TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS categorias(
    id SERIAL PRIMARY KEY,
    codigo_categoria TEXT UNIQUE NOT NULL,
    nome_categoria TEXT NOT NULL,
    criado_em TEXT DEFAULT CURRENT_TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS classes(
    id SERIAL PRIMARY KEY,
    codigo_classe TEXT UNIQUE NOT NULL,
    nome_classe TEXT NOT NULL,
    categoria_id INTEGER NOT NULL,
    criado_em TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (categoria_id) REFERENCES categorias(id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS padroes_descritivos(
    id SERIAL PRIMARY KEY,
    codigo_padrao_descritivo TEXT UNIQUE NOT NULL,
    nome_padrao_descritivo TEXT NOT NULL,
    classe_id INTEGER NOT NULL,
    criado_em TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (classe_id) REFERENCES classes(id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS catalogo(
    id SERIAL PRIMARY KEY,
    codigo_item TEXT UNIQUE NOT NULL,
    nome_item TEXT NOT NULL,
    padrao_descritivo_id INTEGER NOT NULL,
    criado_em TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (padrao_descritivo_id) REFERENCES padroes_descritivos(id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS itens(
    id SERIAL PRIMARY KEY,
    contrato_cod_unico TEXT,
    codigo_item TEXT,
    detalhes_item TEXT,
    quantidade REAL DEFAULT 0,
    valor_unitario REAL DEFAULT 0,
    valor_total REAL DEFAULT 0,
    saldo_quantidade REAL DEFAULT 0,
    saldo_valor REAL DEFAULT 0,
    criado_em TEXT DEFAULT CURRENT_TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS requisicoes(
    id SERIAL PRIMARY KEY,
    item_id INTEGER NOT NULL,
    contrato_cod_unico TEXT NOT NULL,
    codigo_item TEXT NOT NULL,
    quantidade_solicitada REAL NOT NULL,
    valor_estimado REAL NOT NULL DEFAULT 0,
    justificativa TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDENTE',
    usuario_solicitante TEXT NOT NULL,
    data_solicitacao TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    usuario_aprovador TEXT,
    data_aprovacao TEXT,
    observacao_aprovacao TEXT
)
""")

conn.commit()
remover_checks_antigos_status()

cols_itens = get_columns(conn, "itens")
if "codigo_item" not in cols_itens and "cod_item" in cols_itens:
    ensure_column(conn, "itens", "codigo_item", "TEXT")
    conn.execute("""
        UPDATE itens
        SET codigo_item = cod_item
        WHERE (codigo_item IS NULL OR codigo_item = '')
          AND cod_item IS NOT NULL
    """)
    conn.commit()

ensure_column(conn, "itens", "contrato_cod_unico", "TEXT")
ensure_column(conn, "itens", "saldo_quantidade", "REAL DEFAULT 0")
ensure_column(conn, "itens", "saldo_valor", "REAL DEFAULT 0")

conn.execute("""
    UPDATE itens
    SET saldo_quantidade = COALESCE(NULLIF(saldo_quantidade, 0), quantidade)
    WHERE saldo_quantidade IS NULL OR (saldo_quantidade = 0 AND quantidade > 0)
""")
conn.execute("""
    UPDATE itens
    SET saldo_valor = COALESCE(NULLIF(saldo_valor, 0), quantidade * valor_unitario)
    WHERE saldo_valor IS NULL OR (saldo_valor = 0 AND quantidade > 0)
""")
conn.commit()

cursor.execute("SELECT id FROM usuarios WHERE username = ?", ("AndersonMPMelo",))
if cursor.fetchone() is None:
    senha = bcrypt.hashpw("Tomatinho".encode("utf-8"), bcrypt.gensalt())
    cursor.execute(
        "INSERT INTO usuarios(username, password, nivel) VALUES (?, ?, ?)",
        ("AndersonMPMelo", senha, 0),
    )
    conn.commit()

if "logado" not in st.session_state:
    st.session_state.logado = False
if "usuario" not in st.session_state:
    st.session_state.usuario = "Visitante"
if "nivel" not in st.session_state:
    st.session_state.nivel = None
if "historico_pesquisa_itens" not in st.session_state:
    st.session_state.historico_pesquisa_itens = []
if "historico_consultas_arps" not in st.session_state:
    st.session_state.historico_consultas_arps = []


# =========================================================
# PERMISSÕES
# =========================================================
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


MODULOS_SISTEMA = [
    "Dashboard",
    "ARPs",
    "Requisições",
    "Aprovação de Requisições",
    "Cadastro de ARPs",
    "Cadastro de Itens",
    "Editar ARPs",
    "Editar Itens",
    "Editar Catálogo",
    "Editar Requisições",
    "Codificação",
    "Usuários",
]


def modulos_padrao_por_nivel(nivel):
    if nivel == 0:
        return MODULOS_SISTEMA.copy()
    if nivel == 1:
        return [
            "Dashboard",
            "ARPs",
            "Requisições",
            "Aprovação de Requisições",
            "Editar ARPs",
            "Editar Itens",
            "Editar Catálogo",
        ]
    if nivel == 2:
        return [
            "ARPs",
            "Requisições",
            "Cadastro de ARPs",
            "Cadastro de Itens",
        ]
    return ["ARPs"]


def garantir_permissoes_usuario(username, nivel):
    if not username:
        return

    existentes = read_sql(
        "SELECT modulo FROM usuario_modulos WHERE username = ?",
        conn,
        params=(username,)
    )
    existentes_set = set(existentes["modulo"].tolist()) if not existentes.empty else set()
    padrao = set(modulos_padrao_por_nivel(int(nivel)))

    for modulo in MODULOS_SISTEMA:
        if modulo not in existentes_set:
            permitido = 1 if modulo in padrao else 0
            conn.execute(
                "INSERT INTO usuario_modulos(username, modulo, permitido) VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
                (username, modulo, permitido)
            )
    conn.commit()


def usuario_tem_modulo(username, modulo):
    if modulo == "ARPs" and not st.session_state.logado:
        return True

    if not st.session_state.logado:
        return False

    if st.session_state.nivel == 0:
        return True

    garantir_permissoes_usuario(username, st.session_state.nivel)

    row = conn.execute(
        "SELECT permitido FROM usuario_modulos WHERE username = ? AND modulo = ?",
        (username, modulo)
    ).fetchone()

    if row is None:
        return modulo in modulos_padrao_por_nivel(st.session_state.nivel)

    return int(row["permitido"]) == 1


def filtrar_modulos_permitidos(modulos):
    if not st.session_state.logado:
        return [m for m in modulos if m == "ARPs"]

    if st.session_state.nivel == 0:
        return modulos

    return [m for m in modulos if usuario_tem_modulo(st.session_state.usuario, m)]


def excluir_usuario(username):
    if username == st.session_state.usuario:
        raise ValueError("Não é possível excluir o próprio usuário logado.")
    if username == "AndersonMPMelo":
        raise ValueError("Não é possível excluir o administrador padrão.")

    conn.execute("DELETE FROM usuario_modulos WHERE username = ?", (username,))
    conn.execute("DELETE FROM usuarios WHERE username = ?", (username,))
    conn.commit()



# Garante permissões explícitas para o administrador padrão após a definição das funções.
try:
    garantir_permissoes_usuario("AndersonMPMelo", 0)
except Exception:
    pass


def login_sidebar():
    with st.sidebar:
        st.markdown("## Acesso")
        if not st.session_state.logado:
            modo = st.radio("Escolha o modo de acesso", ["Acesso público", "Entrar com login"], index=0)

            if modo == "Entrar com login":
                usuario = st.text_input("Usuário")
                senha = st.text_input("Senha", type="password")
                if st.button("Entrar", use_container_width=True):
                    cursor.execute("SELECT * FROM usuarios WHERE username = ?", (usuario,))
                    dados = cursor.fetchone()
                    senha_banco = normalizar_hash_bcrypt(dados["password"]) if dados else None
                    if dados and senha_banco and bcrypt.checkpw(senha.encode("utf-8"), senha_banco):
                        st.session_state.logado = True
                        st.session_state.usuario = dados["username"]
                        st.session_state.nivel = dados["nivel"]
                        garantir_permissoes_usuario(dados["username"], dados["nivel"])
                        st.rerun()
                    else:
                        st.error("Usuário ou senha inválidos.")
            else:
                st.info("Visitantes podem consultar ARPs e itens. Para exportar PDF, faça login.")
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


# =========================================================
# CONSULTAS
# =========================================================
def carregar_contratos():
    df = read_sql("""
        SELECT id, cod_unico, numero_sei, inicio_vigencia, fim_vigencia, titulo, status
        FROM contratos
        ORDER BY numero_sei, titulo
    """, conn)
    if not df.empty:
        df["status"] = df.apply(lambda x: normalizar_status(x["inicio_vigencia"], x["fim_vigencia"]), axis=1)

        # Atualiza o banco para manter consistência nas demais telas.
        for _, row in df.iterrows():
            conn.execute(
                "UPDATE contratos SET status = ? WHERE id = ?",
                (row["status"], int(row["id"]))
            )
        conn.commit()

    return df


def carregar_catalogo():
    return read_sql("""
        SELECT
            cat.id,
            cat.codigo_item,
            cat.nome_item,
            cat.padrao_descritivo_id,
            pd.codigo_padrao_descritivo,
            pd.nome_padrao_descritivo,
            cl.codigo_classe,
            cl.nome_classe,
            cg.codigo_categoria,
            cg.nome_categoria
        FROM catalogo cat
        JOIN padroes_descritivos pd ON pd.id = cat.padrao_descritivo_id
        JOIN classes cl ON cl.id = pd.classe_id
        JOIN categorias cg ON cg.id = cl.categoria_id
        ORDER BY cg.nome_categoria, cl.nome_classe, pd.nome_padrao_descritivo, cat.nome_item
    """, conn)


def carregar_itens():
    cols = get_columns(conn, "itens")
    col_item = "codigo_item" if "codigo_item" in cols else ("cod_item" if "cod_item" in cols else None)
    col_contrato = "contrato_cod_unico" if "contrato_cod_unico" in cols else None

    if not col_item:
        return pd.DataFrame()

    contrato_select = f"i.{col_contrato} AS contrato_cod_unico" if col_contrato else "'' AS contrato_cod_unico"
    contrato_join_ref = f"i.{col_contrato}" if col_contrato else "''"

    query = f"""
        SELECT
            i.id,
            {contrato_select},
            i.{col_item} AS codigo_item,
            cat.nome_item,
            pd.codigo_padrao_descritivo,
            pd.nome_padrao_descritivo,
            cl.codigo_classe,
            cl.nome_classe,
            cg.codigo_categoria,
            cg.nome_categoria,
            i.detalhes_item,
            i.quantidade,
            i.valor_unitario,
            i.valor_total,
            i.saldo_quantidade,
            i.saldo_valor,
            ct.numero_sei,
            ct.titulo,
            ct.inicio_vigencia,
            ct.fim_vigencia,
            ct.status
        FROM itens i
        LEFT JOIN catalogo cat ON cat.codigo_item = i.{col_item}
        LEFT JOIN padroes_descritivos pd ON pd.id = cat.padrao_descritivo_id
        LEFT JOIN classes cl ON cl.id = pd.classe_id
        LEFT JOIN categorias cg ON cg.id = cl.categoria_id
        LEFT JOIN contratos ct ON ct.cod_unico = {contrato_join_ref}
        ORDER BY ct.numero_sei, cat.nome_item, i.id
    """
    df = read_sql(query, conn)
    if not df.empty:
        df["status"] = df.apply(lambda x: normalizar_status(x["inicio_vigencia"], x["fim_vigencia"]), axis=1)
    return df


def carregar_requisicoes():
    return read_sql("""
        SELECT
            r.id,
            r.item_id,
            r.contrato_cod_unico,
            r.codigo_item,
            r.quantidade_solicitada,
            r.valor_estimado,
            r.justificativa,
            r.status,
            r.usuario_solicitante,
            r.data_solicitacao,
            r.usuario_aprovador,
            r.data_aprovacao,
            r.observacao_aprovacao,
            ct.numero_sei,
            ct.titulo,
            cat.nome_item,
            pd.nome_padrao_descritivo
        FROM requisicoes r
        LEFT JOIN contratos ct ON ct.cod_unico = r.contrato_cod_unico
        LEFT JOIN catalogo cat ON cat.codigo_item = r.codigo_item
        LEFT JOIN padroes_descritivos pd ON pd.id = cat.padrao_descritivo_id
        ORDER BY r.id DESC
    """, conn)


def carregar_categorias():
    return read_sql("""
        SELECT id, codigo_categoria, nome_categoria
        FROM categorias
        ORDER BY codigo_categoria, nome_categoria
    """, conn)


def carregar_classes():
    return read_sql("""
        SELECT cl.id, cl.codigo_classe, cl.nome_classe, cl.categoria_id,
               cg.codigo_categoria, cg.nome_categoria
        FROM classes cl
        JOIN categorias cg ON cg.id = cl.categoria_id
        ORDER BY cg.nome_categoria, cl.nome_classe
    """, conn)


def carregar_padroes():
    return read_sql("""
        SELECT pd.id, pd.codigo_padrao_descritivo, pd.nome_padrao_descritivo, pd.classe_id,
               cl.codigo_classe, cl.nome_classe, cg.codigo_categoria, cg.nome_categoria
        FROM padroes_descritivos pd
        JOIN classes cl ON cl.id = pd.classe_id
        JOIN categorias cg ON cg.id = cl.categoria_id
        ORDER BY cg.nome_categoria, cl.nome_classe, pd.nome_padrao_descritivo
    """, conn)


def aplicar_filtros_consulta(contratos_df, itens_df, busca_geral="", numero_sei=None, filtro_status=None, padrao_texto=None):
    contratos_filtrados = contratos_df.copy()
    itens_filtrados = itens_df.copy()

    if numero_sei is None:
        numero_sei = []
    if filtro_status is None:
        filtro_status = []
    if padrao_texto is None:
        padrao_texto = []

    if isinstance(numero_sei, str):
        numero_sei = [] if numero_sei in ["", "Todos"] else [numero_sei]
    if isinstance(filtro_status, str):
        filtro_status = [] if filtro_status in ["", "Todos"] else [filtro_status]
    if isinstance(padrao_texto, str):
        padrao_texto = [] if padrao_texto in ["", "Todos"] else [padrao_texto]

    if numero_sei:
        contratos_filtrados = contratos_filtrados[contratos_filtrados["numero_sei"].astype(str).isin([str(x) for x in numero_sei])]
        itens_filtrados = itens_filtrados[itens_filtrados["numero_sei"].astype(str).isin([str(x) for x in numero_sei])]

    if filtro_status:
        contratos_filtrados = contratos_filtrados[contratos_filtrados["status"].isin(filtro_status)]
        itens_filtrados = itens_filtrados[itens_filtrados["status"].isin(filtro_status)]

    if padrao_texto:
        itens_filtrados = itens_filtrados[
            itens_filtrados["nome_padrao_descritivo"].fillna("").apply(
                lambda x: any(match_inteligente(p, x) for p in padrao_texto)
            )
        ]

    if busca_geral:
        mask_contrato = contratos_filtrados.apply(
            lambda row: match_inteligente(
                busca_geral,
                f"{row.get('titulo', '')} {row.get('numero_sei', '')} {row.get('cod_unico', '')} {row.get('status', '')}"
            ),
            axis=1
        )
        contratos_por_texto = contratos_filtrados[mask_contrato]

        if not itens_filtrados.empty:
            mask_itens = itens_filtrados.apply(
                lambda row: match_inteligente(
                    busca_geral,
                    " ".join([
                        str(row.get("nome_item", "")),
                        str(row.get("detalhes_item", "")),
                        str(row.get("nome_padrao_descritivo", "")),
                        str(row.get("codigo_padrao_descritivo", "")),
                        str(row.get("nome_classe", "")),
                        str(row.get("codigo_classe", "")),
                        str(row.get("nome_categoria", "")),
                        str(row.get("codigo_categoria", "")),
                        str(row.get("numero_sei", "")),
                        str(row.get("titulo", "")),
                    ])
                ),
                axis=1
            )
            itens_por_texto = itens_filtrados[mask_itens]
        else:
            itens_por_texto = itens_filtrados

        cods_contrato = set(contratos_por_texto["cod_unico"].tolist()) | set(itens_por_texto["contrato_cod_unico"].dropna().tolist())

        contratos_filtrados = contratos_filtrados[contratos_filtrados["cod_unico"].isin(cods_contrato)]

        if not itens_por_texto.empty:
            itens_filtrados = itens_filtrados[itens_filtrados["id"].isin(itens_por_texto["id"].tolist())]
        else:
            itens_filtrados = itens_filtrados[itens_filtrados["contrato_cod_unico"].isin(cods_contrato)]

    return contratos_filtrados, itens_filtrados


def card_contrato_html(numero_sei, titulo, inicio, fim, status):
    if status == "VIGENTE":
        cor = COR_VERDE
    elif status == "VENCIDA":
        cor = COR_VERMELHO
    elif status == "PRÓXIMO AO VENCIMENTO":
        cor = COR_AMARELO
    else:
        cor = COR_AZUL
    return f"""
    <div style="
        border:1px solid {COR_BORDA};
        border-radius:20px;
        padding:18px;
        background:linear-gradient(135deg,#ffffff 0%,#f7faff 100%);
        box-shadow:0 8px 22px rgba(22,65,148,0.06);
        margin-bottom:8px;
    ">
        <div style="display:flex;justify-content:space-between;gap:16px;align-items:center;flex-wrap:wrap;">
            <div>
                <div style="font-size:14px;color:#475569;"><b>Nº SEI:</b> {numero_sei}</div>
                <div style="font-size:20px;color:{COR_AZUL};font-weight:800;margin-top:6px;">{titulo}</div>
                <div style="font-size:13px;color:#64748b;margin-top:8px;">
                    Vigência: {inicio} até {fim}
                </div>
            </div>
            <div style="
                background:{cor};
                color:white;
                padding:8px 14px;
                border-radius:999px;
                font-size:12px;
                font-weight:700;
                white-space:nowrap;
            ">
                {status}
            </div>
        </div>
    </div>
    """




def importar_catalogo_em_massa(arquivo):
    """
    Importa CSV para a estrutura:
    categorias -> classes -> padroes_descritivos -> catalogo.

    Colunas esperadas:
    codigo_categoria, nome_categoria,
    codigo_classe, nome_classe,
    codigo_padrao_descritivo, nome_padrao_descritivo,
    codigo_item, nome_item

    Colunas extras são ignoradas.
    """
    try:
        df = pd.read_csv(arquivo, dtype=str, sep=None, engine="python")
    except Exception:
        arquivo.seek(0)
        df = pd.read_csv(arquivo, dtype=str)

    df.columns = [str(c).strip().lower() for c in df.columns]
    df = df.fillna("")

    colunas_obrigatorias = [
        "codigo_categoria", "nome_categoria",
        "codigo_classe", "nome_classe",
        "codigo_padrao_descritivo", "nome_padrao_descritivo",
        "codigo_item", "nome_item"
    ]

    faltantes = [c for c in colunas_obrigatorias if c not in df.columns]
    if faltantes:
        return {
            "ok": False,
            "erro": f"Colunas obrigatórias ausentes: {', '.join(faltantes)}",
            "linhas": 0,
            "categorias": 0,
            "classes": 0,
            "padroes": 0,
            "catalogo": 0,
        }

    df = df[colunas_obrigatorias].copy()
    for col in colunas_obrigatorias:
        df[col] = df[col].astype(str).str.strip()

    df = df[
        (df["codigo_categoria"] != "") &
        (df["nome_categoria"] != "") &
        (df["codigo_classe"] != "") &
        (df["nome_classe"] != "") &
        (df["codigo_padrao_descritivo"] != "") &
        (df["nome_padrao_descritivo"] != "") &
        (df["codigo_item"] != "") &
        (df["nome_item"] != "")
    ].drop_duplicates()

    if df.empty:
        return {
            "ok": False,
            "erro": "O arquivo não possui linhas válidas para importação.",
            "linhas": 0,
            "categorias": 0,
            "classes": 0,
            "padroes": 0,
            "catalogo": 0,
        }

    total_cat = total_cl = total_pd = total_it = 0

    try:
        with conn:
            for _, row in df.iterrows():
                codigo_categoria = row["codigo_categoria"]
                nome_categoria = row["nome_categoria"]
                codigo_classe = row["codigo_classe"]
                nome_classe = row["nome_classe"]
                codigo_padrao = row["codigo_padrao_descritivo"]
                nome_padrao = row["nome_padrao_descritivo"]
                codigo_item = row["codigo_item"]
                nome_item = row["nome_item"]

                # Categoria
                existente = conn.execute(
                    "SELECT id FROM categorias WHERE codigo_categoria = ?",
                    (codigo_categoria,)
                ).fetchone()

                if existente is None:
                    conn.execute(
                        "INSERT INTO categorias(codigo_categoria, nome_categoria) VALUES (?, ?)",
                        (codigo_categoria, nome_categoria)
                    )
                    total_cat += 1
                else:
                    conn.execute(
                        "UPDATE categorias SET nome_categoria = ? WHERE codigo_categoria = ?",
                        (nome_categoria, codigo_categoria)
                    )

                categoria_id = conn.execute(
                    "SELECT id FROM categorias WHERE codigo_categoria = ?",
                    (codigo_categoria,)
                ).fetchone()["id"]

                # Classe
                existente = conn.execute(
                    "SELECT id FROM classes WHERE codigo_classe = ?",
                    (codigo_classe,)
                ).fetchone()

                if existente is None:
                    conn.execute(
                        "INSERT INTO classes(codigo_classe, nome_classe, categoria_id) VALUES (?, ?, ?)",
                        (codigo_classe, nome_classe, int(categoria_id))
                    )
                    total_cl += 1
                else:
                    conn.execute(
                        "UPDATE classes SET nome_classe = ?, categoria_id = ? WHERE codigo_classe = ?",
                        (nome_classe, int(categoria_id), codigo_classe)
                    )

                classe_id = conn.execute(
                    "SELECT id FROM classes WHERE codigo_classe = ?",
                    (codigo_classe,)
                ).fetchone()["id"]

                # Padrão Descritivo
                existente = conn.execute(
                    "SELECT id FROM padroes_descritivos WHERE codigo_padrao_descritivo = ?",
                    (codigo_padrao,)
                ).fetchone()

                if existente is None:
                    conn.execute(
                        """
                        INSERT INTO padroes_descritivos(
                            codigo_padrao_descritivo,
                            nome_padrao_descritivo,
                            classe_id
                        )
                        VALUES (?, ?, ?)
                        """,
                        (codigo_padrao, nome_padrao, int(classe_id))
                    )
                    total_pd += 1
                else:
                    conn.execute(
                        """
                        UPDATE padroes_descritivos
                        SET nome_padrao_descritivo = ?, classe_id = ?
                        WHERE codigo_padrao_descritivo = ?
                        """,
                        (nome_padrao, int(classe_id), codigo_padrao)
                    )

                padrao_id = conn.execute(
                    "SELECT id FROM padroes_descritivos WHERE codigo_padrao_descritivo = ?",
                    (codigo_padrao,)
                ).fetchone()["id"]

                # Catálogo
                existente = conn.execute(
                    "SELECT id FROM catalogo WHERE codigo_item = ?",
                    (codigo_item,)
                ).fetchone()

                if existente is None:
                    conn.execute(
                        "INSERT INTO catalogo(codigo_item, nome_item, padrao_descritivo_id) VALUES (?, ?, ?)",
                        (codigo_item, nome_item, int(padrao_id))
                    )
                    total_it += 1
                else:
                    conn.execute(
                        "UPDATE catalogo SET nome_item = ?, padrao_descritivo_id = ? WHERE codigo_item = ?",
                        (nome_item, int(padrao_id), codigo_item)
                    )

        return {
            "ok": True,
            "erro": "",
            "linhas": len(df),
            "categorias": total_cat,
            "classes": total_cl,
            "padroes": total_pd,
            "catalogo": total_it,
        }

    except Exception as e:
        return {
            "ok": False,
            "erro": str(e),
            "linhas": 0,
            "categorias": 0,
            "classes": 0,
            "padroes": 0,
            "catalogo": 0,
        }

# =========================================================
# APP
# =========================================================
apply_custom_css()
login_sidebar()
render_header()

menu_publico = ["ARPs"]
menu_base_logado = [
    "Dashboard",
    "ARPs",
    "Requisições",
    "Aprovação de Requisições",
    "Cadastro de ARPs",
    "Cadastro de Itens",
    "Editar ARPs",
    "Editar Itens",
    "Editar Catálogo",
    "Editar Requisições",
    "Codificação",
    "Usuários",
]

if is_admin():
    opcoes_menu = menu_base_logado
elif st.session_state.logado:
    opcoes_menu = filtrar_modulos_permitidos(menu_base_logado)
else:
    opcoes_menu = menu_publico

if not opcoes_menu:
    st.sidebar.error("Seu usuário não possui módulos liberados.")
    st.stop()

menu = st.sidebar.selectbox("Menu", opcoes_menu)


# =========================================================
# DASHBOARD
# =========================================================
if menu == "Dashboard":
    if not pode_editar_dados():
        st.error("Somente usuários nível 0 e 1 podem acessar o Dashboard.")
        st.stop()

    st.title("Dashboard Gerencial")
    st.caption("Visão consolidada de ARPs, quantidades e requisições por solicitante.")
    recalc_all_balances()

    contratos_df = carregar_contratos()
    itens_df = carregar_itens()
    req_df = carregar_requisicoes()

    total_ARPs = len(contratos_df)
    ARPs_vigentes = int((contratos_df["status"] == "VIGENTE").sum()) if not contratos_df.empty else 0
    saldo_financeiro_total = float(itens_df["saldo_valor"].fillna(0).sum()) if not itens_df.empty else 0.0
    requisicoes_total = len(req_df)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("ARPs", total_ARPs)
    c2.metric("ARPs Vigentes", ARPs_vigentes)
    c3.metric("Valor Total Disponível", brl(saldo_financeiro_total))
    c4.metric("Requisições", requisicoes_total)

    st.divider()

    col_a, col_b = st.columns(2)

    with col_a:
        section_box_start()
        st.subheader("ARPs e Quantidades")
        if itens_df.empty:
            st.info("Nenhum item cadastrado para compor o saldo dos ARPs.")
        else:
            ARPs_saldo = itens_df.groupby(["numero_sei", "titulo", "status"], dropna=False, as_index=False).agg(
                saldo_quantidade=("saldo_quantidade", "sum"),
                saldo_valor=("saldo_valor", "sum"),
                quantidade_inicial=("quantidade", "sum")
            ).sort_values(["status", "numero_sei", "titulo"])

            ARPs_saldo["Status"] = ARPs_saldo["status"].apply(status_badge_df)
            ARPs_saldo["Valor Disponível"] = ARPs_saldo["saldo_valor"].apply(brl)

            exibir = ARPs_saldo.rename(columns={
                "numero_sei": "Número do SEI",
                "titulo": "Contrato",
                "quantidade_inicial": "Quantidade Inicial",
                "saldo_quantidade": "Quantidade Atual"
            })[["Número do SEI", "Contrato", "Status", "Quantidade Inicial", "Quantidade Atual", "Valor Disponível"]]

            st.dataframe(exibir, use_container_width=True, hide_index=True)

            grafico_saldo = ARPs_saldo[["titulo", "saldo_valor"]].copy()
            grafico_saldo = grafico_saldo.sort_values("saldo_valor", ascending=False).head(10)
            grafico_saldo = grafico_saldo.set_index("titulo")
            if not grafico_saldo.empty:
                st.markdown("**Top 10 ARPs por saldo financeiro**")
                st.bar_chart(grafico_saldo)
        section_box_end()

    with col_b:
        section_box_start()
        st.subheader("Requisições por Solicitante")
        if req_df.empty:
            st.info("Nenhuma requisição cadastrada.")
        else:
            req_solicitante = req_df.groupby(["usuario_solicitante", "status"], dropna=False, as_index=False).size()
            tabela_req = req_solicitante.pivot_table(
                index="usuario_solicitante",
                columns="status",
                values="size",
                aggfunc="sum",
                fill_value=0
            ).reset_index()

            for col in ["APROVADA", "PENDENTE", "REJEITADA"]:
                if col not in tabela_req.columns:
                    tabela_req[col] = 0

            tabela_req["Total"] = tabela_req[["APROVADA", "PENDENTE", "REJEITADA"]].sum(axis=1)
            tabela_req = tabela_req.sort_values("Total", ascending=False)

            exibir_req = tabela_req.rename(columns={
                "usuario_solicitante": "Solicitante",
                "APROVADA": "Aprovadas",
                "PENDENTE": "Pendentes",
                "REJEITADA": "Rejeitadas"
            })[["Solicitante", "Aprovadas", "Pendentes", "Rejeitadas", "Total"]]

            st.dataframe(exibir_req, use_container_width=True, hide_index=True)

            grafico_req = tabela_req[["usuario_solicitante", "Total"]].copy().set_index("usuario_solicitante")
            if not grafico_req.empty:
                st.markdown("**Volume de requisições por solicitante**")
                st.bar_chart(grafico_req)

            req_valor = req_df.groupby("usuario_solicitante", as_index=False)["valor_estimado"].sum().sort_values("valor_estimado", ascending=False)
            if not req_valor.empty:
                req_valor["valor_estimado"] = req_valor["valor_estimado"].fillna(0)
                st.markdown("**Valor estimado por solicitante**")
                st.dataframe(
                    req_valor.rename(columns={
                        "usuario_solicitante": "Solicitante",
                        "valor_estimado": "Valor Estimado"
                    }).assign(**{"Valor Estimado": req_valor["valor_estimado"].apply(brl)}),
                    use_container_width=True,
                    hide_index=True
                )
        section_box_end()

    section_box_start()
    st.subheader("Resumo de status das requisições")
    if req_df.empty:
        st.info("Nenhuma requisição cadastrada.")
    else:
        status_counts = req_df.groupby("status", as_index=False).size().sort_values("size", ascending=False)
        c1, c2, c3 = st.columns(3)
        pend = int(status_counts.loc[status_counts["status"] == "PENDENTE", "size"].sum())
        apr = int(status_counts.loc[status_counts["status"] == "APROVADA", "size"].sum())
        rej = int(status_counts.loc[status_counts["status"] == "REJEITADA", "size"].sum())
        c1.metric("Pendentes", pend)
        c2.metric("Aprovadas", apr)
        c3.metric("Rejeitadas", rej)
        graf = status_counts.set_index("status")[["size"]]
        st.bar_chart(graf)
    section_box_end()

# =========================================================
# CONTRATOS
# =========================================================
if menu == "ARPs":
    st.title("Consulta de ARPs e Itens")
    st.caption("Consulte ARPs e itens vinculados, com busca inteligente por grafia semelhante.")

    contratos_df = carregar_contratos()
    itens_df = carregar_itens()

    if contratos_df.empty:
        st.warning("Nenhum ARP cadastrada.")
        st.stop()

    st.markdown('<div class="filtro-box">', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns([1.7, 1.7, 1.2, 1.4])

    busca_livre = c1.text_input(
        "Busca inteligente",
        placeholder="Digite item, classe, categoria, detalhe ou palavra-chave"
    )

    opcoes_busca = []
    if not itens_df.empty:
        itens_busca = []
        for _, item_opt in itens_df.iterrows():
            partes = [
                str(item_opt.get("nome_item", "")),
                str(item_opt.get("nome_padrao_descritivo", "")),
                str(item_opt.get("nome_classe", "")),
                str(item_opt.get("nome_categoria", "")),
            ]
            label = " | ".join([p for p in partes if p and p != "nan"])
            if label:
                itens_busca.append(label)
        opcoes_busca = sorted(set(itens_busca))

    busca_lista = c2.multiselect("Selecionar item/classe/categoria", opcoes_busca)

    numero_sei = c3.multiselect(
        "Número SEI",
        sorted(contratos_df["numero_sei"].astype(str).unique().tolist())
    )

    filtro_status = c4.multiselect(
        "Status",
        ["VIGENTE", "PRÓXIMO AO VENCIMENTO", "VENCIDA"],
        default=[]
    )

    padroes_opcoes = sorted([
        str(x) for x in itens_df["nome_padrao_descritivo"].dropna().unique().tolist()
        if str(x).strip()
    ])
    padrao_texto = st.multiselect("Padrão Descritivo", padroes_opcoes)

    justificativa_pdf = st.text_area("Referência (Processo SEI)", placeholder="Para tornar esta pesquisa válida, indicar expressamente Processo SEI aberto para adesão à Ata.")
    st.markdown('</div>', unsafe_allow_html=True)

    termos_busca = []
    if busca_livre.strip():
        termos_busca.append(busca_livre.strip())
    if busca_lista:
        termos_busca.extend(busca_lista)

    busca_geral_filtro = " ".join(termos_busca).strip()

    contratos_filtrados, itens_filtrados = aplicar_filtros_consulta(
        contratos_df, itens_df, busca_geral_filtro, numero_sei, filtro_status, padrao_texto
    )

    resumo_filtros = (
        f"Busca: {busca_geral_filtro or 'Nenhuma'} | "
        f"Nº SEI: {', '.join(numero_sei) if numero_sei else 'Todos'} | "
        f"Status: {', '.join(filtro_status) if filtro_status else 'Todos'} | "
        f"Padrão Descritivo: {', '.join(padrao_texto) if padrao_texto else 'Todos'}"
    )

    contratos_export = contratos_filtrados.copy()
    contratos_export["itens_exportacao"] = contratos_export["cod_unico"].apply(
        lambda cod: itens_filtrados[itens_filtrados["contrato_cod_unico"] == cod][[
            "nome_item", "nome_padrao_descritivo", "detalhes_item"
        ]].to_dict("records")
    )

    texto_inexistencia = None
    if contratos_filtrados.empty and itens_filtrados.empty:
        texto_inexistencia = "Informo para os deviaos fins que, apôs diligências realzadas nesta Gerência de Atas - SEAD-PIGAB/SLCIGPPCL, NÃO CONSTA no Sistema de Registro de Preços, ATAS VIGENTES E GERENCIADAS PELA PRÓPRIA SEAD-PI. referentes especificamente ao ITEM pesquisado."
   
    referencia_pdf = str(justificativa_pdf or "").strip()

    if not st.session_state.logado:
        st.info("Faça login para exportar a consulta em PDF.")
    elif not filtro_status:
        st.warning("Para exportar o PDF, selecione obrigatoriamente ao menos um Status válido.")
    elif not referencia_pdf:
        st.warning("Para exportar o PDF, informe obrigatoriamente a Referência (Processo SEI).")
    elif not validar_codigo_sei(referencia_pdf):
        st.error("A Referência (Processo SEI) deve estar no formato 00000.000000/AAAA-00. Exemplo: 00002.004441/2024-46.")
    else:
        usuario_pdf = st.session_state.get("usuario", "Usuário não identificado")
        pdf_bytes = gerar_pdf_consulta_ARPs(
            contratos_export,
            resumo_filtros,
            texto_inexistencia,
            referencia_pdf,
            usuario_pdf
        )
        st.download_button(
            "Exportar consulta em PDF",
            data=pdf_bytes,
            file_name=f"consulta_arps_itens_{datetime.now().strftime('%d-%m-%Y_%H-%M-%S')}.pdf",
            mime="application/pdf",
            use_container_width=True
        )

    st.divider()
    st.subheader("Registro da consulta atual")

    resultado_consulta_atual = "ENCONTRADO" if (not contratos_filtrados.empty or not itens_filtrados.empty) else "NÃO ENCONTRADO"

    col_reg1, col_reg2, col_reg3 = st.columns([1, 1, 2])
    col_reg1.metric("Resultado", resultado_consulta_atual)
    col_reg2.metric("Itens localizados", len(itens_filtrados))
    col_reg3.write(f"**Filtros utilizados:** {resumo_filtros}")

    if st.button("Registrar", use_container_width=True):
        registrar_historico_consulta_arps(
            contratos_filtrados,
            itens_filtrados,
            resumo_filtros,
            busca_geral_filtro
        )
        st.success("Consulta registrada no histórico.")
        st.rerun()

    st.divider()
    st.subheader("Histórico de consultas realizadas")

    historico_consultas = st.session_state.get("historico_consultas_arps", [])

    if not historico_consultas:
        st.info("Nenhuma consulta registrada nesta sessão ainda.")
    else:
        hist_df = pd.DataFrame([
            {
                "Data da Pesquisa": h.get("data_pesquisa", ""),
                "Busca Inteligente": h.get("busca_inteligente", "Nenhuma"),
                "Filtros Utilizados": h.get("filtros", ""),
                "Resultado": h.get("resultado", ""),
                "ARPs Localizadas": h.get("qtd_arps", 0),
                "Itens Localizados": h.get("qtd_itens", 0),
            }
            for h in historico_consultas
        ])

        st.dataframe(hist_df, use_container_width=True, hide_index=True)

        opcoes_hist = {
            f"{h.get('data_pesquisa', '')} | {h.get('resultado', '')} | {h.get('busca_inteligente', 'Nenhuma')} | Itens: {h.get('qtd_itens', 0)}": h
            for h in historico_consultas
        }

        selecionados_hist_labels = st.multiselect(
            "Selecionar consultas do histórico para exportar em PDF",
            list(opcoes_hist.keys()),
            default=[]
        )

        referencia_historico = st.text_input(
            "Referência (Processo SEI) para PDF do histórico",
            placeholder="00002.004441/2024-46",
            key="referencia_historico_pdf"
        )

        c_hist1, c_hist2 = st.columns(2)

        if c_hist1.button("Limpar histórico da sessão", use_container_width=True):
            st.session_state.historico_consultas_arps = []
            st.session_state.historico_pesquisa_itens = []
            st.success("Histórico limpo.")
            st.rerun()

        consultas_hist_selecionadas = [opcoes_hist[label] for label in selecionados_hist_labels] if selecionados_hist_labels else []

        if not st.session_state.logado:
            st.info("Faça login para exportar o histórico em PDF.")
        elif not referencia_historico:
            st.info("Informe a Referência (Processo SEI) para habilitar o PDF do histórico.")
        elif not validar_codigo_sei(referencia_historico):
            st.error("A Referência (Processo SEI) deve estar no formato 00000.000000/AAAA-00. Exemplo: 00002.004441/2024-46.")
        elif not consultas_hist_selecionadas:
            st.warning("Selecione ao menos uma consulta do histórico.")
        else:
            pdf_hist = gerar_pdf_historico_consultas_arps(
                consultas_hist_selecionadas,
                referencia_historico.strip(),
                st.session_state.get("usuario", "Usuário não identificado")
            )
            c_hist2.download_button(
                "Exportar histórico selecionado em PDF",
                data=pdf_hist,
                file_name=f"historico_consultas_arps_{datetime.now().strftime('%d-%m-%Y_%H-%M-%S')}.pdf",
                mime="application/pdf",
                use_container_width=True
            )

    st.divider()

    if contratos_filtrados.empty and itens_filtrados.empty:
        st.error("Inexistência de item ou contrato para os filtros informados.")
        st.info("A consulta pode ser exportada em PDF para atestar a inexistência.")
        st.stop()

    for _, row in contratos_filtrados.iterrows():
        itens_contrato = itens_filtrados[itens_filtrados["contrato_cod_unico"] == row["cod_unico"]].copy()
        titulo_expander = f"{row['numero_sei']} - {row['titulo']} [{row['status']}]"

        with st.expander(titulo_expander, expanded=False):
            st.markdown(
                card_contrato_html(
                    row["numero_sei"], row["titulo"], data_br(row["inicio_vigencia"]),
                    data_br(row["fim_vigencia"]), row["status"]
                ),
                unsafe_allow_html=True
            )

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
                            quantidade_inicial = float(item.get("quantidade", 0) or 0)
                            valor_unitario_inicial = float(item.get("valor_unitario", 0) or 0)
                            valor_total_inicial = quantidade_inicial * valor_unitario_inicial
                            st.write(f"**Quantidade Inicial:** {quantidade_inicial}")
                            st.write(f"**Valor Total Inicial:** {brl(valor_total_inicial)}")
       
# =========================================================
# REQUISIÇÕES
# =========================================================
if menu == "Requisições":
    if not pode_requisitar():
        st.error("Faça login para acessar o módulo de Requisições.")
        st.stop()

    st.title("Requisições")
    st.caption("Localize itens com mais precisão, registre requisições e acompanhe o andamento.")

    itens_df = carregar_itens()

    if itens_df.empty:
        st.warning("Nenhum item cadastrado.")
        st.stop()

    st.markdown('<div class="filtro-box">', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    sei_filtro = c1.multiselect(
        "Número SEI",
        sorted([x for x in itens_df["numero_sei"].dropna().astype(str).unique().tolist()])
    )

    status_contrato = c2.multiselect(
        "Status da ARP",
        ["VIGENTE", "PRÓXIMO AO VENCIMENTO", "VENCIDA"],
        default=[]
    )

    padroes_req_opcoes = sorted([
        str(x) for x in itens_df["nome_padrao_descritivo"].dropna().unique().tolist()
        if str(x).strip()
    ])
    padrao_filtro = c3.multiselect("Padrão Descritivo", padroes_req_opcoes, key="padrao_req_select")

    opcoes_req_busca = ["Todos"]
    if not itens_df.empty:
        labels_req = []
        for _, item_opt in itens_df.iterrows():
            partes = [
                str(item_opt.get("nome_item", "")),
                str(item_opt.get("nome_padrao_descritivo", "")),
                str(item_opt.get("nome_classe", "")),
                str(item_opt.get("nome_categoria", "")),
            ]
            label = " | ".join([p for p in partes if p and p != "nan"])
            if label:
                labels_req.append(label)
        opcoes_req_busca += sorted(set(labels_req))

    texto_lista = c4.multiselect("Selecionar item/classe/categoria", opcoes_req_busca, key="req_busca_select")
    texto_livre = st.text_input("Busca inteligente em Requisições", placeholder="Digite item, classe, categoria, detalhe ou palavra-chave")
    somente_disponiveis = st.checkbox("Mostrar apenas itens com quantidade disponível", value=True)
    st.markdown('</div>', unsafe_allow_html=True)

    itens_filtrados = itens_df.copy()

    if sei_filtro:
        itens_filtrados = itens_filtrados[itens_filtrados["numero_sei"].astype(str).isin(sei_filtro)]
    if status_contrato:
        itens_filtrados = itens_filtrados[itens_filtrados["status"].isin(status_contrato)]
    if padrao_filtro:
        itens_filtrados = itens_filtrados[
            itens_filtrados["nome_padrao_descritivo"].fillna("").apply(
                lambda x: any(match_inteligente(p, x) for p in padrao_filtro)
            )
        ]
    termos_req = []
    if texto_lista:
        termos_req.extend(texto_lista)
    if texto_livre.strip():
        termos_req.append(texto_livre.strip())

    texto_busca_req = " ".join(termos_req).strip()

    if texto_busca_req:
        itens_filtrados = itens_filtrados[
            itens_filtrados.apply(
                lambda row: match_inteligente(
                    texto_busca_req,
                    " ".join([
                        str(row.get("nome_item", "")),
                        str(row.get("detalhes_item", "")),
                        str(row.get("nome_padrao_descritivo", "")),
                        str(row.get("nome_classe", "")),
                        str(row.get("nome_categoria", "")),
                    ])
                ),
                axis=1
            )
        ]
    if somente_disponiveis:
        itens_filtrados = itens_filtrados[itens_filtrados["saldo_quantidade"] > 0]

    if itens_filtrados.empty:
        st.warning("Nenhum item localizado para os filtros informados.")
        st.stop()

    st.markdown('<div class="card-info">', unsafe_allow_html=True)
    st.markdown(f"<div class='texto-suporte'>Itens localizados: <b>{len(itens_filtrados)}</b></div>", unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    for _, row in itens_filtrados.iterrows():
        titulo_expander = f"{row['numero_sei']} • {row['nome_item']} • {row['status']}"
        with st.expander(titulo_expander, expanded=False):
            st.markdown(status_badge_html(row["status"]), unsafe_allow_html=True)
            c1, c2 = st.columns([1.5, 1])
            with c1:
                st.write(f"**ARP correspondente:** {row['titulo']}")
                st.write(f"**Padrão Descritivo:** {row['nome_padrao_descritivo']}")
                st.write(f"**Classe:** {row['nome_classe']}")
                st.write(f"**Categoria:** {row['nome_categoria']}")
                st.write(f"**Detalhamento do item:** {row['detalhes_item']}")
            with c2:
                st.write(f"**Vigência:** {data_br(row['inicio_vigencia'])} até {data_br(row['fim_vigencia'])}")
                st.write(f"**Quantidade atual:** {row['saldo_quantidade']}")
                st.write(f"**Valor unitário:** {brl(row['valor_unitario'])}")
                st.write(f"**Valor disponível:** {brl(row['saldo_valor'])}")

    st.divider()
    section_box_start()
    st.subheader("Registrar requisição")

    itens_disponiveis = itens_filtrados[itens_filtrados["saldo_quantidade"] > 0].copy()
    if itens_disponiveis.empty:
        st.warning("Não há itens com quantidade disponível para requisição.")
    else:
        itens_disponiveis["label_item"] = itens_disponiveis.apply(
            lambda x: f"{x['nome_item']} | SEI {x['numero_sei']} | Quantidade {x['saldo_quantidade']}",
            axis=1
        )
        item_sel = st.selectbox(
            "Selecione o item para requisição",
            itens_disponiveis.to_dict("records"),
            format_func=lambda x: x["label_item"]
        )

        with st.form("form_requisicao_item"):
            st.write(f"**ARP selecionada:** {item_sel['titulo']}")
            st.write(f"**Nº SEI:** {item_sel['numero_sei']}")
            st.markdown(status_badge_html(item_sel["status"]), unsafe_allow_html=True)
            quantidade_req = st.number_input(
                "Quantidade solicitada",
                min_value=0.0,
                max_value=float(item_sel["saldo_quantidade"]),
                value=0.0,
                step=1.0
            )
            justificativa = st.text_area("Justificativa para utilizar ou não o item")
            enviar = st.form_submit_button("Registrar requisição", use_container_width=True)

            if enviar:
                if quantidade_req <= 0:
                    st.warning("Informe uma quantidade maior que zero.")
                elif not justificativa.strip():
                    st.warning("Informe a justificativa da requisição.")
                else:
                    valor_estimado = float(quantidade_req) * float(item_sel["valor_unitario"] or 0)
                    conn.execute("""
                        INSERT INTO requisicoes(
                            item_id, contrato_cod_unico, codigo_item,
                            quantidade_solicitada, valor_estimado, justificativa,
                            status, usuario_solicitante
                        )
                        VALUES (?, ?, ?, ?, ?, ?, 'PENDENTE', ?)
                    """, (
                        int(item_sel["id"]),
                        item_sel["contrato_cod_unico"],
                        item_sel["codigo_item"],
                        float(quantidade_req),
                        valor_estimado,
                        justificativa.strip(),
                        st.session_state.usuario
                    ))
                    conn.commit()
                    st.success("Requisição registrada com sucesso.")
                    st.rerun()
    section_box_end()

    st.divider()
    section_box_start()
    st.subheader("Minhas requisições")
    req = carregar_requisicoes()
    if st.session_state.nivel not in [0, 1]:
        req = req[req["usuario_solicitante"] == st.session_state.usuario]

    if req.empty:
        st.info("Nenhuma requisição registrada.")
    else:
        exibir = req[[
            "numero_sei", "nome_item", "nome_padrao_descritivo",
            "quantidade_solicitada", "valor_estimado", "status",
            "usuario_solicitante", "data_solicitacao", "usuario_aprovador", "data_aprovacao"
        ]].copy()
        exibir.columns = [
            "Número do SEI", "Nome do Item", "Padrão Descritivo",
            "Quantidade Solicitada", "Valor Estimado", "Status",
            "Solicitante", "Data da Solicitação", "Aprovador", "Data da Aprovação"
        ]
        exibir["Valor Estimado"] = exibir["Valor Estimado"].apply(brl)
        exibir["Status"] = exibir["Status"].apply(status_badge_df)
        exibir["Data da Solicitação"] = exibir["Data da Solicitação"].apply(datahora_br)
        exibir["Data da Aprovação"] = exibir["Data da Aprovação"].apply(datahora_br)
        st.dataframe(exibir, use_container_width=True, hide_index=True)
    section_box_end()


# =========================================================
# EDITAR REQUISIÇÕES
# =========================================================
if menu == "Editar Requisições":
    if not is_admin():
        st.error("Somente usuários nível 0 podem editar e excluir requisições.")
        st.stop()

    st.title("Editar Requisições")
    req = carregar_requisicoes()

    if req.empty:
        st.info("Nenhuma requisição registrada.")
        st.stop()

    section_box_start()
    req_sel = st.selectbox(
        "Selecione a requisição",
        req.to_dict("records"),
        format_func=lambda x: f"{x['numero_sei']} - {x['nome_item']} - {x['status']} - ID {x['id']}"
    )

    item_row = conn.execute("""
        SELECT id, saldo_quantidade, valor_unitario
        FROM itens
        WHERE id = ?
    """, (int(req_sel["item_id"]),)).fetchone()

    saldo_atual_item = float(item_row["saldo_quantidade"] or 0) if item_row else 0.0
    valor_unit_item = float(item_row["valor_unitario"] or 0) if item_row else 0.0
    qtd_atual = float(req_sel["quantidade_solicitada"] or 0)

    st.markdown(status_badge_html(req_sel["status"]), unsafe_allow_html=True)
    st.write(f"**Solicitante:** {req_sel['usuario_solicitante']}")
    st.write(f"**Data da solicitação:** {datahora_br(req_sel['data_solicitacao'])}")
    st.write(f"**Contrato:** {req_sel['titulo']}")
    st.write(f"**Padrão Descritivo:** {req_sel['nome_padrao_descritivo']}")

    with st.form("form_editar_requisicao"):
        nova_quantidade = st.number_input(
            "Quantidade solicitada",
            min_value=0.0,
            value=qtd_atual,
            step=1.0
        )
        nova_justificativa = st.text_area("Justificativa", value=req_sel["justificativa"] or "")
        status_atual = str(req_sel["status"]).upper()
        opcoes_status = ["PENDENTE", "APROVADA", "REJEITADA"]
        idx_status = opcoes_status.index(status_atual) if status_atual in opcoes_status else 0
        novo_status = st.selectbox("Status", opcoes_status, index=idx_status)
        nova_observacao = st.text_area("Observação da análise", value=req_sel["observacao_aprovacao"] or "")
        salvar = st.form_submit_button("Salvar alterações", use_container_width=True)

        if salvar:
            if nova_quantidade <= 0:
                st.warning("Informe uma quantidade maior que zero.")
            elif not nova_justificativa.strip():
                st.warning("Informe a justificativa.")
            else:
                disponivel_para_aprovar = saldo_atual_item + (qtd_atual if status_atual == "APROVADA" else 0)

                if novo_status == "APROVADA" and nova_quantidade > disponivel_para_aprovar:
                    st.error("Não é possível salvar como aprovada. A quantidade supera o quantidade disponível do item.")
                else:
                    valor_estimado = nova_quantidade * valor_unit_item

                    if novo_status in ["APROVADA", "REJEITADA"]:
                        conn.execute("""
                            UPDATE requisicoes
                            SET quantidade_solicitada = ?,
                                valor_estimado = ?,
                                justificativa = ?,
                                status = ?,
                                usuario_aprovador = ?,
                                data_aprovacao = CURRENT_TIMESTAMP,
                                observacao_aprovacao = ?
                            WHERE id = ?
                        """, (
                            float(nova_quantidade),
                            float(valor_estimado),
                            nova_justificativa.strip(),
                            novo_status,
                            st.session_state.usuario,
                            nova_observacao.strip(),
                            int(req_sel["id"])
                        ))
                    else:
                        conn.execute("""
                            UPDATE requisicoes
                            SET quantidade_solicitada = ?,
                                valor_estimado = ?,
                                justificativa = ?,
                                status = ?,
                                usuario_aprovador = NULL,
                                data_aprovacao = NULL,
                                observacao_aprovacao = ?
                            WHERE id = ?
                        """, (
                            float(nova_quantidade),
                            float(valor_estimado),
                            nova_justificativa.strip(),
                            novo_status,
                            nova_observacao.strip(),
                            int(req_sel["id"])
                        ))

                    conn.commit()
                    # Quantidade atualizada no Dashboard: recalc_item_balance(int(req_sel["item_id"]))
                    st.success("Requisição atualizada com sucesso.")
                    st.rerun()

    st.warning("A exclusão removerá permanentemente a requisição selecionada.")
    if st.button("Excluir requisição selecionada", type="primary", use_container_width=True):
        conn.execute("DELETE FROM requisicoes WHERE id = ?", (int(req_sel["id"]),))
        conn.commit()
        # Quantidade atualizada no Dashboard: recalc_item_balance(int(req_sel["item_id"]))
        st.success("Requisição excluída com sucesso.")
        st.rerun()
    section_box_end()

# =========================================================
# APROVAÇÃO DE REQUISIÇÕES
# =========================================================
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
            st.markdown(status_badge_html("PENDENTE"), unsafe_allow_html=True)
            st.write(f"**Solicitante:** {row['usuario_solicitante']}")
            st.write(f"**Data da solicitação:** {datahora_br(row['data_solicitacao'])}")
            st.write(f"**Contrato:** {row['titulo']}")
            st.write(f"**Padrão Descritivo:** {row['nome_padrao_descritivo']}")
            st.write(f"**Quantidade solicitada:** {row['quantidade_solicitada']}")
            st.write(f"**Valor estimado:** {brl(row['valor_estimado'])}")
            st.write(f"**Justificativa:** {row['justificativa']}")

            item = conn.execute("SELECT saldo_quantidade FROM itens WHERE id = ?", (int(row["item_id"]),)).fetchone()
            saldo_atual = float(item["saldo_quantidade"] or 0) if item else 0
            st.info(f"Quantidade atual do item no contrato: {saldo_atual}")

            observacao = st.text_area("Observação da análise", key=f"obs_{row['id']}")
            c1, c2 = st.columns(2)

            if c1.button("Aprovar", key=f"aprovar_{row['id']}", use_container_width=True):
                if float(row["quantidade_solicitada"]) > saldo_atual:
                    st.error("Não é possível aprovar. A quantidade solicitada é maior que o saldo atual.")
                else:
                    conn.execute("""
                        UPDATE requisicoes
                        SET status = 'APROVADA',
                            usuario_aprovador = ?,
                            data_aprovacao = CURRENT_TIMESTAMP,
                            observacao_aprovacao = ?
                        WHERE id = ?
                    """, (st.session_state.usuario, observacao.strip(), int(row["id"])))
                    conn.commit()
                    # Quantidade atualizada no Dashboard: recalc_item_balance(int(row["item_id"]))
                    st.success("Requisição aprovada e balancete atualizado.")
                    st.rerun()

            if c2.button("Rejeitar", key=f"rejeitar_{row['id']}", use_container_width=True):
                conn.execute("""
                    UPDATE requisicoes
                    SET status = 'REJEITADA',
                        usuario_aprovador = ?,
                        data_aprovacao = CURRENT_TIMESTAMP,
                        observacao_aprovacao = ?
                    WHERE id = ?
                """, (st.session_state.usuario, observacao.strip(), int(row["id"])))
                conn.commit()
                st.success("Requisição rejeitada.")
                st.rerun()

# =========================================================
# CADASTRO DE CONTRATOS
# =========================================================
if menu == "Cadastro de ARPs":
    if not pode_cadastrar_contrato():
        st.error("Somente usuários nível 2 ou nível 0 podem cadastrar ARPs.")
        st.stop()

    st.title("Cadastro de ARPs")
    section_box_start()
    with st.form("form_contrato", clear_on_submit=True):
        cod_unico = gerar_cod_unico()
        st.text_input("COD Único gerado automaticamente", value=cod_unico, disabled=True)
        numero_sei = st.text_input("Número do SEI", placeholder="00002.004441/2024-46")
        titulo = st.text_input("Título")
        c1, c2 = st.columns(2)
        inicio_vigencia_txt = c1.text_input("Início da Vigência (DDMMAAAA ou DD-MM-AAAA)", placeholder="31122026")
        fim_vigencia_txt = c2.text_input("Fim da Vigência (DDMMAAAA ou DD-MM-AAAA)", placeholder="31122027")
        salvar = st.form_submit_button("Cadastrar ARP", use_container_width=True)

        if salvar:
            inicio_vigencia = parse_data_br(inicio_vigencia_txt)
            fim_vigencia = parse_data_br(fim_vigencia_txt)
            if not all([cod_unico.strip(), numero_sei.strip(), titulo.strip(), inicio_vigencia, fim_vigencia]):
                st.warning("Preencha todos os campos e informe as datas no padrão DDMMAAAA ou DD-MM-AAAA.")
            elif not validar_codigo_sei(numero_sei):
                st.error("Informe o Número SEI no formato 00000.000000/AAAA-00. Exemplo: 00002.004441/2024-46.")
            elif fim_vigencia < inicio_vigencia:
                st.error("A data final não pode ser menor que a data inicial.")
            else:
                status = normalizar_status(inicio_vigencia, fim_vigencia)
                try:
                    cursor.execute("""
                        INSERT INTO contratos(cod_unico, numero_sei, inicio_vigencia, fim_vigencia, titulo, status)
                        VALUES (?,?,?,?,?,?)
                    """, (
                        cod_unico.strip(),
                        numero_sei.strip(),
                        inicio_vigencia.strftime("%Y-%m-%d"),
                        fim_vigencia.strftime("%Y-%m-%d"),
                        titulo.strip(),
                        status
                    ))
                    conn.commit()
                    st.success("Contrato cadastrado com sucesso.")
                except psycopg.IntegrityError:
                    st.error("Já existe ARP com este COD Único.")
    section_box_end()

    ARPs = carregar_contratos()
    if not ARPs.empty:
        section_box_start()
        st.subheader("ARPs cadastradas")
        exibir = ARPs.copy()
        exibir["inicio_vigencia"] = exibir["inicio_vigencia"].apply(data_br)
        exibir["fim_vigencia"] = exibir["fim_vigencia"].apply(data_br)
        exibir["status"] = exibir["status"].apply(status_badge_df)
        exibir = exibir[["cod_unico", "numero_sei", "inicio_vigencia", "fim_vigencia", "titulo", "status"]]
        exibir.columns = ["COD Único", "Número do SEI", "Início", "Fim", "Título", "Status"]
        st.dataframe(exibir, use_container_width=True, hide_index=True)
        section_box_end()

# =========================================================
# CADASTRO DE ITENS
# =========================================================
if menu == "Cadastro de Itens":
    if not pode_cadastrar_item():
        st.error("Somente usuários nível 2 ou nível 0 podem cadastrar itens.")
        st.stop()

    st.title("Cadastro de Itens")
    ARPs = carregar_contratos()
    catalogo = carregar_catalogo()

    if ARPs.empty:
        st.warning("Cadastre uma ARP antes de cadastrar itens.")
        st.stop()
    if catalogo.empty:
        st.warning("Cadastre a Codificação antes de cadastrar itens.")
        st.stop()

    section_box_start()
    opcoes_catalogo = {f"{row['codigo_item']} - {row['nome_item']}": row["codigo_item"] for _, row in catalogo.iterrows()}
    with st.form("form_item", clear_on_submit=True):
        contrato_cod = st.selectbox("Contrato", ARPs["cod_unico"].tolist())
        item_escolhido = st.selectbox("Item do Catálogo", list(opcoes_catalogo.keys()))
        codigo_item = opcoes_catalogo[item_escolhido]

        info_item = catalogo[catalogo["codigo_item"] == codigo_item].iloc[0]
        st.caption(
            f"Categoria: {info_item['nome_categoria']} | Classe: {info_item['nome_classe']} | "
            f"Padrão Descritivo: {info_item['nome_padrao_descritivo']}"
        )

        detalhes = st.text_area("Detalhes do Item")
        c1, c2 = st.columns(2)
        quantidade = c1.number_input("Quantidade Inicial", min_value=0.0, value=0.0, step=1.0)
        valor_unitario = c2.number_input("Valor Unitário", min_value=0.0, value=0.0, step=0.01)
        valor_total = quantidade * valor_unitario
        st.info(f"Quantidade inicial do item: {quantidade} | Valor total inicial: {brl(valor_total)}")

        salvar = st.form_submit_button("Cadastrar item", use_container_width=True)
        if salvar:
            if not detalhes.strip():
                st.warning("Informe os detalhes do item.")
            else:
                cursor.execute("""
                    INSERT INTO itens(
                        contrato_cod_unico, codigo_item, detalhes_item,
                        quantidade, valor_unitario, valor_total,
                        saldo_quantidade, saldo_valor
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    contrato_cod, codigo_item, detalhes.strip(),
                    quantidade, valor_unitario, valor_total,
                    quantidade, valor_total
                ))
                conn.commit()
                st.success("Item cadastrado com sucesso.")
    section_box_end()

    itens = carregar_itens()
    if not itens.empty:
        section_box_start()
        st.subheader("Itens cadastrados")
        exibir = itens[[
            "numero_sei", "nome_item", "nome_padrao_descritivo", "detalhes_item",
            "quantidade", "saldo_quantidade", "saldo_valor", "status"
        ]].copy()
        exibir.columns = ["Número do SEI", "Nome do Item", "Padrão Descritivo", "Detalhes", "Quantidade Inicial", "Quantidade Atual", "Valor Disponível", "Status"]
        exibir["Valor Disponível"] = exibir["Valor Disponível"].apply(brl)
        exibir["Status"] = exibir["Status"].apply(status_badge_df)
        st.dataframe(exibir, use_container_width=True, hide_index=True)
        section_box_end()

# =========================================================
# EDITAR CONTRATOS
# =========================================================
if menu == "Editar ARPs":
    if not pode_editar_dados():
        st.error("Somente usuários nível 0 e 1 podem editar ARPs.")
        st.stop()

    st.title("Editar ARPs")
    ARPs = carregar_contratos()

    if ARPs.empty:
        st.info("Nenhum ARP cadastrada.")
        st.stop()

    section_box_start()
    contrato_sel = st.selectbox(
        "Selecione a ARP",
        ARPs.to_dict("records"),
        format_func=lambda x: f"{x['numero_sei']} - {x['titulo']}"
    )

    with st.form("form_editar_contrato"):
        cod_unico = st.text_input("COD Único", value=contrato_sel["cod_unico"], disabled=True)
        numero_sei = st.text_input("Número do SEI", value=contrato_sel["numero_sei"], help="Formato: 00000.000000/AAAA-00")
        titulo = st.text_input("Título", value=contrato_sel["titulo"])
        c1, c2 = st.columns(2)
        inicio_txt = c1.text_input("Início da Vigência (DDMMAAAA ou DD-MM-AAAA)", value=data_br(contrato_sel["inicio_vigencia"]))
        fim_txt = c2.text_input("Fim da Vigência (DDMMAAAA ou DD-MM-AAAA)", value=data_br(contrato_sel["fim_vigencia"]))
        salvar = st.form_submit_button("Salvar alterações", use_container_width=True)

        if salvar:
            inicio = parse_data_br(inicio_txt)
            fim = parse_data_br(fim_txt)
            if not all([cod_unico.strip(), numero_sei.strip(), titulo.strip(), inicio, fim]):
                st.warning("Preencha todos os campos corretamente.")
            elif not validar_codigo_sei(numero_sei):
                st.error("Informe o Número SEI no formato 00000.000000/AAAA-00. Exemplo: 00002.004441/2024-46.")
            elif fim < inicio:
                st.error("A data final não pode ser menor que a data inicial.")
            else:
                status = normalizar_status(inicio, fim)
                try:
                    cod_antigo = contrato_sel["cod_unico"]
                    cursor.execute("""
                        UPDATE contratos
                        SET cod_unico=?, numero_sei=?, inicio_vigencia=?, fim_vigencia=?, titulo=?, status=?
                        WHERE id=?
                    """, (
                        cod_unico.strip(), numero_sei.strip(), inicio.strftime("%Y-%m-%d"),
                        fim.strftime("%Y-%m-%d"), titulo.strip(), status, int(contrato_sel["id"])
                    ))
                    if cod_antigo != cod_unico.strip():
                        cursor.execute("UPDATE itens SET contrato_cod_unico=? WHERE contrato_cod_unico=?", (cod_unico.strip(), cod_antigo))
                        cursor.execute("UPDATE requisicoes SET contrato_cod_unico=? WHERE contrato_cod_unico=?", (cod_unico.strip(), cod_antigo))
                    conn.commit()
                    st.success("Contrato atualizado com sucesso.")
                    st.rerun()
                except psycopg.IntegrityError:
                    st.error("Já existe outro ARP com este COD Único.")
    st.warning("A exclusão do contrato removerá também os itens e requisições vinculados.")
    if st.button("Excluir ARP selecionada", type="primary", use_container_width=True):
        excluir_contrato(contrato_sel["cod_unico"])
        st.success("Contrato excluído com sucesso.")
        st.rerun()
    section_box_end()

# =========================================================
# EDITAR ITENS
# =========================================================
if menu == "Editar Itens":
    if not pode_editar_dados():
        st.error("Somente usuários nível 0 e 1 podem editar itens.")
        st.stop()

    st.title("Editar Itens")
    itens = carregar_itens()
    ARPs = carregar_contratos()
    catalogo = carregar_catalogo()

    if itens.empty:
        st.info("Nenhum item cadastrado.")
        st.stop()

    section_box_start()
    item_sel = st.selectbox(
        "Selecione o item",
        itens.to_dict("records"),
        format_func=lambda x: f"{x['numero_sei']} - {x['nome_item']} - {str(x['detalhes_item'])[:50]}"
    )

    opcoes_contratos = ARPs["cod_unico"].tolist()
    opcoes_catalogo = {f"{row['codigo_item']} - {row['nome_item']}": row["codigo_item"] for _, row in catalogo.iterrows()}
    labels_catalogo = list(opcoes_catalogo.keys())
    label_atual = next((k for k, v in opcoes_catalogo.items() if v == item_sel["codigo_item"]), labels_catalogo[0])

    with st.form("form_editar_item"):
        contrato_cod = st.selectbox(
            "Contrato", opcoes_contratos,
            index=max(0, opcoes_contratos.index(item_sel["contrato_cod_unico"])) if item_sel["contrato_cod_unico"] in opcoes_contratos else 0
        )
        item_catalogo = st.selectbox(
            "Item do Catálogo", labels_catalogo,
            index=max(0, labels_catalogo.index(label_atual)) if label_atual in labels_catalogo else 0
        )
        detalhes = st.text_area("Detalhes do Item", value=item_sel["detalhes_item"])
        c1, c2 = st.columns(2)
        quantidade = c1.number_input("Quantidade Inicial", min_value=0.0, value=float(item_sel["quantidade"] or 0), step=1.0)
        valor_unitario = c2.number_input("Valor Unitário", min_value=0.0, value=float(item_sel["valor_unitario"] or 0), step=0.01)
        salvar = st.form_submit_button("Salvar alterações", use_container_width=True)

        if salvar:
            codigo_item = opcoes_catalogo[item_catalogo]
            aprovado = conn.execute("""
                SELECT COALESCE(SUM(quantidade_solicitada), 0)
                FROM requisicoes
                WHERE item_id = ? AND status = 'APROVADA'
            """, (int(item_sel["id"]),)).fetchone()[0]
            aprovado = float(aprovado or 0)

            if quantidade < aprovado:
                st.error(f"A quantidade inicial não pode ser menor que o total já aprovado ({aprovado}).")
            else:
                saldo_quantidade = quantidade - aprovado
                valor_total = quantidade * valor_unitario
                saldo_valor = saldo_quantidade * valor_unitario

                conn.execute("""
                    UPDATE itens
                    SET contrato_cod_unico=?, codigo_item=?, detalhes_item=?,
                        quantidade=?, valor_unitario=?, valor_total=?,
                        saldo_quantidade=?, saldo_valor=?
                    WHERE id=?
                """, (
                    contrato_cod, codigo_item, detalhes.strip(),
                    quantidade, valor_unitario, valor_total,
                    saldo_quantidade, saldo_valor, int(item_sel["id"])
                ))
                conn.execute("""
                    UPDATE requisicoes
                    SET contrato_cod_unico=?, codigo_item=?
                    WHERE item_id=?
                """, (contrato_cod, codigo_item, int(item_sel["id"])))
                conn.commit()
                st.success("Item atualizado com sucesso.")
                st.rerun()

    if st.button("Excluir item selecionado", type="primary", use_container_width=True):
        excluir_item(item_sel["id"])
        st.success("Item excluído com sucesso.")
        st.rerun()
    section_box_end()

# =========================================================
# EDITAR CATÁLOGO
# =========================================================
if menu == "Editar Catálogo":
    if not pode_editar_dados():
        st.error("Somente usuários nível 0 e 1 podem editar catálogo.")
        st.stop()

    st.title("Editar Catálogo")
    catalogo = carregar_catalogo()
    padroes = carregar_padroes()

    if catalogo.empty:
        st.info("Nenhum item do catálogo cadastrado.")
        st.stop()

    section_box_start()
    item_sel = st.selectbox(
        "Selecione o item do catálogo",
        catalogo.to_dict("records"),
        format_func=lambda x: f"{x['codigo_item']} - {x['nome_item']}"
    )

    mapa_padroes = {f"{row['codigo_padrao_descritivo']} - {row['nome_padrao_descritivo']}": row["id"] for _, row in padroes.iterrows()}
    labels_padroes = list(mapa_padroes.keys())
    label_padrao_atual = next((k for k, v in mapa_padroes.items() if v == item_sel["padrao_descritivo_id"]), labels_padroes[0])

    with st.form("form_editar_catalogo"):
        codigo_item = st.text_input("Código do Item", value=item_sel["codigo_item"])
        nome_item = st.text_input("Nome do Item", value=item_sel["nome_item"])
        padrao_sel = st.selectbox(
            "Padrão Descritivo", labels_padroes,
            index=max(0, labels_padroes.index(label_padrao_atual)) if label_padrao_atual in labels_padroes else 0
        )
        salvar = st.form_submit_button("Salvar alterações", use_container_width=True)

        if salvar:
            try:
                codigo_antigo = item_sel["codigo_item"]
                novo_padrao_id = mapa_padroes[padrao_sel]
                conn.execute("""
                    UPDATE catalogo
                    SET codigo_item=?, nome_item=?, padrao_descritivo_id=?
                    WHERE id=?
                """, (codigo_item.strip(), nome_item.strip(), novo_padrao_id, int(item_sel["id"])))
                if codigo_antigo != codigo_item.strip():
                    conn.execute("UPDATE itens SET codigo_item=? WHERE codigo_item=?", (codigo_item.strip(), codigo_antigo))
                    conn.execute("UPDATE requisicoes SET codigo_item=? WHERE codigo_item=?", (codigo_item.strip(), codigo_antigo))
                conn.commit()
                st.success("Catálogo atualizado com sucesso.")
                st.rerun()
            except psycopg.IntegrityError:
                st.error("Já existe outro item no catálogo com este código.")

    st.warning("A exclusão do item do catálogo removerá também os itens operacionais e requisições vinculados.")
    if st.button("Excluir item do catálogo selecionado", type="primary", use_container_width=True):
        excluir_catalogo(item_sel["codigo_item"])
        st.success("Item do catálogo excluído com sucesso.")
        st.rerun()
    section_box_end()

# =========================================================
# CODIFICAÇÃO
# =========================================================
if menu == "Codificação":
    if not pode_cadastrar_codificacao():
        st.error("Somente o usuário nível 0 pode cadastrar e editar informações da Codificação.")
        st.stop()

    st.title("Codificação")
    abas = st.tabs(["Categorias", "Classes", "Padrão Descritivo", "Catálogo", "Visualização"])

    with abas[0]:
        section_box_start()
        st.subheader("Tabela de Categorias")

        with st.form("form_categoria", clear_on_submit=True):
            codigo_categoria = st.text_input("Código da Categoria")
            nome_categoria = st.text_input("Nome da Categoria")
            salvar = st.form_submit_button("Cadastrar categoria", use_container_width=True)
            if salvar:
                if not codigo_categoria.strip() or not nome_categoria.strip():
                    st.warning("Preencha o código e o nome da categoria.")
                else:
                    try:
                        conn.execute(
                            "INSERT INTO categorias(codigo_categoria, nome_categoria) VALUES (?, ?)",
                            (codigo_categoria.strip(), nome_categoria.strip())
                        )
                        conn.commit()
                        st.success("Categoria cadastrada com sucesso.")
                        st.rerun()
                    except psycopg.IntegrityError:
                        st.error("Já existe uma categoria com este código.")

        categorias = carregar_categorias()
        if not categorias.empty:
            st.divider()
            st.subheader("Editar ou excluir Categoria")

            categoria_sel = st.selectbox(
                "Selecione a Categoria",
                categorias.to_dict("records"),
                format_func=lambda x: f"{x['codigo_categoria']} - {x['nome_categoria']}",
                key="categoria_edicao_select"
            )

            with st.form("form_editar_categoria"):
                novo_codigo_categoria = st.text_input(
                    "Código da Categoria",
                    value=categoria_sel["codigo_categoria"],
                    key="editar_codigo_categoria"
                )
                novo_nome_categoria = st.text_input(
                    "Nome da Categoria",
                    value=categoria_sel["nome_categoria"],
                    key="editar_nome_categoria"
                )
                salvar_edicao_categoria = st.form_submit_button("Salvar alterações da categoria", use_container_width=True)

                if salvar_edicao_categoria:
                    if not novo_codigo_categoria.strip() or not novo_nome_categoria.strip():
                        st.warning("Preencha o código e o nome da categoria.")
                    else:
                        try:
                            conn.execute("""
                                UPDATE categorias
                                SET codigo_categoria = ?, nome_categoria = ?
                                WHERE id = ?
                            """, (
                                novo_codigo_categoria.strip(),
                                novo_nome_categoria.strip(),
                                int(categoria_sel["id"])
                            ))
                            conn.commit()
                            st.success("Categoria atualizada com sucesso.")
                            st.rerun()
                        except psycopg.IntegrityError:
                            st.error("Já existe outra categoria com este código.")

            st.warning("A exclusão da categoria removerá também classes, padrões, catálogo, itens e requisições vinculadas.")
            confirmar_categoria = st.checkbox(
                "Confirmo que desejo excluir esta categoria e seus vínculos",
                key="confirmar_excluir_categoria"
            )
            if st.button("Excluir categoria selecionada", type="primary", use_container_width=True, disabled=not confirmar_categoria):
                excluir_categoria(int(categoria_sel["id"]))
                st.success("Categoria excluída com sucesso.")
                st.rerun()

            st.divider()
            st.subheader("Categorias cadastradas")
            st.dataframe(
                categorias[["codigo_categoria", "nome_categoria"]],
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("Nenhuma categoria cadastrada.")
        section_box_end()

    with abas[1]:
        section_box_start()
        st.subheader("Tabela de Classes")
        categorias = carregar_categorias()

        if categorias.empty:
            st.warning("Cadastre ao menos uma categoria antes de cadastrar classes.")
        else:
            mapa_categorias = {
                f"{row['codigo_categoria']} - {row['nome_categoria']}": row["id"]
                for _, row in categorias.iterrows()
            }

            with st.form("form_classe", clear_on_submit=True):
                categoria_sel_cadastro = st.selectbox("Categoria", list(mapa_categorias.keys()), key="categoria_classe_cadastro")
                codigo_classe = st.text_input("Código da Classe")
                nome_classe = st.text_input("Nome da Classe")
                salvar = st.form_submit_button("Cadastrar classe", use_container_width=True)
                if salvar:
                    if not codigo_classe.strip() or not nome_classe.strip():
                        st.warning("Preencha o código e o nome da classe.")
                    else:
                        try:
                            conn.execute("""
                                INSERT INTO classes(codigo_classe, nome_classe, categoria_id)
                                VALUES (?, ?, ?)
                            """, (
                                codigo_classe.strip(),
                                nome_classe.strip(),
                                int(mapa_categorias[categoria_sel_cadastro])
                            ))
                            conn.commit()
                            st.success("Classe cadastrada com sucesso.")
                            st.rerun()
                        except psycopg.IntegrityError:
                            st.error("Já existe uma classe com este código.")

        classes = carregar_classes()
        if not classes.empty:
            st.divider()
            st.subheader("Editar ou excluir Classe")

            classe_sel = st.selectbox(
                "Selecione a Classe",
                classes.to_dict("records"),
                format_func=lambda x: f"{x['codigo_classe']} - {x['nome_classe']} | {x['nome_categoria']}",
                key="classe_edicao_select"
            )

            mapa_categorias_edicao = {
                f"{row['codigo_categoria']} - {row['nome_categoria']}": row["id"]
                for _, row in categorias.iterrows()
            }
            labels_categorias = list(mapa_categorias_edicao.keys())
            label_categoria_atual = next(
                (label for label, cat_id in mapa_categorias_edicao.items() if int(cat_id) == int(classe_sel["categoria_id"])),
                labels_categorias[0]
            )

            with st.form("form_editar_classe"):
                categoria_edicao = st.selectbox(
                    "Categoria",
                    labels_categorias,
                    index=labels_categorias.index(label_categoria_atual),
                    key="categoria_classe_edicao"
                )
                novo_codigo_classe = st.text_input("Código da Classe", value=classe_sel["codigo_classe"])
                novo_nome_classe = st.text_input("Nome da Classe", value=classe_sel["nome_classe"])
                salvar_edicao_classe = st.form_submit_button("Salvar alterações da classe", use_container_width=True)

                if salvar_edicao_classe:
                    if not novo_codigo_classe.strip() or not novo_nome_classe.strip():
                        st.warning("Preencha o código e o nome da classe.")
                    else:
                        try:
                            conn.execute("""
                                UPDATE classes
                                SET codigo_classe = ?, nome_classe = ?, categoria_id = ?
                                WHERE id = ?
                            """, (
                                novo_codigo_classe.strip(),
                                novo_nome_classe.strip(),
                                int(mapa_categorias_edicao[categoria_edicao]),
                                int(classe_sel["id"])
                            ))
                            conn.commit()
                            st.success("Classe atualizada com sucesso.")
                            st.rerun()
                        except psycopg.IntegrityError:
                            st.error("Já existe outra classe com este código.")

            st.warning("A exclusão da classe removerá também padrões, catálogo, itens e requisições vinculadas.")
            confirmar_classe = st.checkbox(
                "Confirmo que desejo excluir esta classe e seus vínculos",
                key="confirmar_excluir_classe"
            )
            if st.button("Excluir classe selecionada", type="primary", use_container_width=True, disabled=not confirmar_classe):
                excluir_classe(int(classe_sel["id"]))
                st.success("Classe excluída com sucesso.")
                st.rerun()

            st.divider()
            st.subheader("Classes cadastradas")
            st.dataframe(
                classes[["codigo_categoria", "nome_categoria", "codigo_classe", "nome_classe"]],
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("Nenhuma classe cadastrada.")
        section_box_end()

    with abas[2]:
        section_box_start()
        st.subheader("Tabela de Padrão Descritivo")
        classes = carregar_classes()

        if classes.empty:
            st.warning("Cadastre ao menos uma classe antes de cadastrar o padrão descritivo.")
        else:
            mapa_classes = {
                f"{row['codigo_classe']} - {row['nome_classe']}": row["id"]
                for _, row in classes.iterrows()
            }

            with st.form("form_padrao", clear_on_submit=True):
                classe_sel_cadastro = st.selectbox("Classe", list(mapa_classes.keys()), key="classe_padrao_cadastro")
                codigo_padrao = st.text_input("Código do Padrão Descritivo")
                nome_padrao = st.text_input("Nome do Padrão Descritivo")
                salvar = st.form_submit_button("Cadastrar padrão descritivo", use_container_width=True)
                if salvar:
                    if not codigo_padrao.strip() or not nome_padrao.strip():
                        st.warning("Preencha o código e o nome do padrão descritivo.")
                    else:
                        try:
                            conn.execute("""
                                INSERT INTO padroes_descritivos(codigo_padrao_descritivo, nome_padrao_descritivo, classe_id)
                                VALUES (?, ?, ?)
                            """, (
                                codigo_padrao.strip(),
                                nome_padrao.strip(),
                                int(mapa_classes[classe_sel_cadastro])
                            ))
                            conn.commit()
                            st.success("Padrão descritivo cadastrado com sucesso.")
                            st.rerun()
                        except psycopg.IntegrityError:
                            st.error("Já existe um padrão descritivo com este código.")

        padroes = carregar_padroes()
        if not padroes.empty:
            st.divider()
            st.subheader("Editar ou excluir Padrão Descritivo")

            padrao_sel = st.selectbox(
                "Selecione o Padrão Descritivo",
                padroes.to_dict("records"),
                format_func=lambda x: f"{x['codigo_padrao_descritivo']} - {x['nome_padrao_descritivo']} | {x['nome_classe']}",
                key="padrao_edicao_select"
            )

            mapa_classes_edicao = {
                f"{row['codigo_classe']} - {row['nome_classe']}": row["id"]
                for _, row in classes.iterrows()
            }
            labels_classes = list(mapa_classes_edicao.keys())
            label_classe_atual = next(
                (label for label, classe_id in mapa_classes_edicao.items() if int(classe_id) == int(padrao_sel["classe_id"])),
                labels_classes[0]
            )

            with st.form("form_editar_padrao"):
                classe_edicao = st.selectbox(
                    "Classe",
                    labels_classes,
                    index=labels_classes.index(label_classe_atual),
                    key="classe_padrao_edicao"
                )
                novo_codigo_padrao = st.text_input("Código do Padrão Descritivo", value=padrao_sel["codigo_padrao_descritivo"])
                novo_nome_padrao = st.text_input("Nome do Padrão Descritivo", value=padrao_sel["nome_padrao_descritivo"])
                salvar_edicao_padrao = st.form_submit_button("Salvar alterações do padrão descritivo", use_container_width=True)

                if salvar_edicao_padrao:
                    if not novo_codigo_padrao.strip() or not novo_nome_padrao.strip():
                        st.warning("Preencha o código e o nome do padrão descritivo.")
                    else:
                        try:
                            conn.execute("""
                                UPDATE padroes_descritivos
                                SET codigo_padrao_descritivo = ?, nome_padrao_descritivo = ?, classe_id = ?
                                WHERE id = ?
                            """, (
                                novo_codigo_padrao.strip(),
                                novo_nome_padrao.strip(),
                                int(mapa_classes_edicao[classe_edicao]),
                                int(padrao_sel["id"])
                            ))
                            conn.commit()
                            st.success("Padrão descritivo atualizado com sucesso.")
                            st.rerun()
                        except psycopg.IntegrityError:
                            st.error("Já existe outro padrão descritivo com este código.")

            st.warning("A exclusão do padrão removerá também catálogo, itens e requisições vinculadas.")
            confirmar_padrao = st.checkbox(
                "Confirmo que desejo excluir este padrão descritivo e seus vínculos",
                key="confirmar_excluir_padrao"
            )
            if st.button("Excluir padrão descritivo selecionado", type="primary", use_container_width=True, disabled=not confirmar_padrao):
                excluir_padrao_descritivo(int(padrao_sel["id"]))
                st.success("Padrão descritivo excluído com sucesso.")
                st.rerun()

            st.divider()
            st.subheader("Padrões cadastrados")
            st.dataframe(
                padroes[[
                    "codigo_categoria", "nome_categoria",
                    "codigo_classe", "nome_classe",
                    "codigo_padrao_descritivo", "nome_padrao_descritivo"
                ]],
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("Nenhum padrão descritivo cadastrado.")
        section_box_end()

    with abas[3]:
        section_box_start()
        st.subheader("Tabela de Catálogo")

        st.markdown("#### Importação em massa do Catálogo")
        st.caption("Envie um CSV com as colunas: codigo_categoria, nome_categoria, codigo_classe, nome_classe, codigo_padrao_descritivo, nome_padrao_descritivo, codigo_item, nome_item.")

        arquivo_catalogo = st.file_uploader(
            "Importar CSV do Catálogo",
            type=["csv"],
            key="upload_catalogo_massa"
        )

        if arquivo_catalogo is not None:
            preview_df = pd.read_csv(arquivo_catalogo, dtype=str, sep=None, engine="python")
            arquivo_catalogo.seek(0)
            st.write("Prévia do arquivo:")
            st.dataframe(preview_df.head(20), use_container_width=True, hide_index=True)

            if st.button("Importar catálogo em massa", use_container_width=True):
                resultado = importar_catalogo_em_massa(arquivo_catalogo)

                if resultado["ok"]:
                    st.success(
                        f"Importação concluída. Linhas processadas: {resultado['linhas']} | "
                        f"Categorias novas: {resultado['categorias']} | "
                        f"Classes novas: {resultado['classes']} | "
                        f"Padrões novos: {resultado['padroes']} | "
                        f"Itens novos no catálogo: {resultado['catalogo']}"
                    )
                    st.rerun()
                else:
                    st.error(f"Falha na importação: {resultado['erro']}")

            st.divider()

        padroes = carregar_padroes()

        if padroes.empty:
            st.warning("Cadastre ao menos um padrão descritivo antes de cadastrar o catálogo.")
        else:
            mapa_padroes = {
                f"{row['codigo_padrao_descritivo']} - {row['nome_padrao_descritivo']}": row["id"]
                for _, row in padroes.iterrows()
            }

            with st.form("form_catalogo", clear_on_submit=True):
                padrao_sel_cadastro = st.selectbox("Padrão Descritivo", list(mapa_padroes.keys()), key="padrao_catalogo_cadastro")
                codigo_item = st.text_input("Código do Item")
                nome_item = st.text_input("Nome do Item")
                salvar = st.form_submit_button("Cadastrar item no catálogo", use_container_width=True)
                if salvar:
                    if not codigo_item.strip() or not nome_item.strip():
                        st.warning("Preencha o código e o nome do item.")
                    else:
                        try:
                            conn.execute("""
                                INSERT INTO catalogo(codigo_item, nome_item, padrao_descritivo_id)
                                VALUES (?, ?, ?)
                            """, (
                                codigo_item.strip(),
                                nome_item.strip(),
                                int(mapa_padroes[padrao_sel_cadastro])
                            ))
                            conn.commit()
                            st.success("Item do catálogo cadastrado com sucesso.")
                            st.rerun()
                        except psycopg.IntegrityError:
                            st.error("Já existe um item com este código.")

        catalogo = carregar_catalogo()
        if not catalogo.empty:
            st.divider()
            st.subheader("Catálogo cadastrado")
            st.dataframe(
                catalogo[[
                    "codigo_categoria", "nome_categoria",
                    "codigo_classe", "nome_classe",
                    "codigo_padrao_descritivo", "nome_padrao_descritivo",
                    "codigo_item", "nome_item"
                ]],
                use_container_width=True,
                hide_index=True
            )
            st.info("Para editar ou excluir itens do Catálogo, use o módulo 'Editar Catálogo'.")
        else:
            st.info("Nenhum item do catálogo cadastrado.")
        section_box_end()

    with abas[4]:
        section_box_start()
        st.subheader("Visualização consolidada da Codificação")
        categorias = carregar_categorias()
        classes = carregar_classes()
        padroes = carregar_padroes()
        catalogo = carregar_catalogo()

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Categorias", len(categorias))
        c2.metric("Classes", len(classes))
        c3.metric("Padrões Descritivos", len(padroes))
        c4.metric("Itens do Catálogo", len(catalogo))

        if not catalogo.empty:
            termo = st.text_input("Buscar na Codificação")
            df = catalogo.copy()
            if termo:
                df = df[df.apply(
                    lambda row: (
                        match_inteligente(termo, row["codigo_categoria"]) or
                        match_inteligente(termo, row["nome_categoria"]) or
                        match_inteligente(termo, row["codigo_classe"]) or
                        match_inteligente(termo, row["nome_classe"]) or
                        match_inteligente(termo, row["codigo_padrao_descritivo"]) or
                        match_inteligente(termo, row["nome_padrao_descritivo"]) or
                        match_inteligente(termo, row["codigo_item"]) or
                        match_inteligente(termo, row["nome_item"])
                    ), axis=1)]
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("Nenhum item do catálogo cadastrado.")
        section_box_end()



# =========================================================
# USUÁRIOS
# =========================================================
if menu == "Usuários":
    if not usuario_tem_modulo(st.session_state.usuario, "Usuários"):
        st.error("Você não possui permissão para acessar este módulo.")
        st.stop()

    if not is_admin():
        st.error("Somente o administrador nível 0 pode gerenciar usuários.")
        st.stop()

    st.title("Cadastro e Permissões de Usuários")

    section_box_start()
    st.subheader("Criar novo usuário")
    with st.form("form_usuario", clear_on_submit=True):
        user = st.text_input("Usuário")
        senha = st.text_input("Senha", type="password")
        nivel = st.selectbox("Nível", [0, 1, 2])
        salvar = st.form_submit_button("Criar usuário", use_container_width=True)

        if salvar:
            if not user.strip() or not senha.strip():
                st.warning("Informe usuário e senha.")
            else:
                try:
                    senha_hash = bcrypt.hashpw(senha.encode("utf-8"), bcrypt.gensalt())
                    conn.execute(
                        "INSERT INTO usuarios(username, password, nivel) VALUES (?, ?, ?)",
                        (user.strip(), senha_hash, int(nivel))
                    )
                    conn.commit()
                    garantir_permissoes_usuario(user.strip(), int(nivel))
                    st.success("Usuário criado com sucesso.")
                    st.rerun()
                except psycopg.IntegrityError:
                    st.error("Já existe um usuário com este nome.")
    section_box_end()

    usuarios = read_sql("SELECT id, username, nivel FROM usuarios ORDER BY nivel, username", conn)

    section_box_start()
    st.subheader("Usuários cadastrados")
    st.dataframe(
        usuarios[["username", "nivel"]].rename(columns={"username": "Usuário", "nivel": "Nível"}),
        use_container_width=True,
        hide_index=True
    )
    section_box_end()

    if not usuarios.empty:
        section_box_start()
        st.subheader("Editar nível, permissões e exclusão")

        usuario_sel = st.selectbox(
            "Selecione o usuário",
            usuarios.to_dict("records"),
            format_func=lambda x: f"{x['username']} | Nível {x['nivel']}",
            key="usuario_gerenciar_select"
        )

        garantir_permissoes_usuario(usuario_sel["username"], int(usuario_sel["nivel"]))

        st.markdown("#### Dados do usuário")
        novo_nivel = st.selectbox(
            "Nível do usuário",
            [0, 1, 2],
            index=[0, 1, 2].index(int(usuario_sel["nivel"])),
            key="editar_nivel_usuario",
            disabled=(usuario_sel["username"] == "AndersonMPMelo")
        )

        if st.button("Salvar nível do usuário", use_container_width=True, disabled=(usuario_sel["username"] == "AndersonMPMelo")):
            conn.execute(
                "UPDATE usuarios SET nivel = ? WHERE username = ?",
                (int(novo_nivel), usuario_sel["username"])
            )
            conn.commit()
            garantir_permissoes_usuario(usuario_sel["username"], int(novo_nivel))
            st.success("Nível atualizado com sucesso.")
            st.rerun()

        st.divider()
        st.markdown("#### Permissões por módulo")

        permissoes_df = read_sql(
            "SELECT modulo, permitido FROM usuario_modulos WHERE username = ?",
            conn,
            params=(usuario_sel["username"],)
        )

        permissoes_map = {
            row["modulo"]: bool(row["permitido"])
            for _, row in permissoes_df.iterrows()
        }

        novas_permissoes = {}
        col1, col2 = st.columns(2)

        for idx, modulo in enumerate(MODULOS_SISTEMA):
            valor_padrao = permissoes_map.get(
                modulo,
                modulo in modulos_padrao_por_nivel(int(usuario_sel["nivel"]))
            )

            disabled = usuario_sel["username"] == "AndersonMPMelo"

            if idx % 2 == 0:
                with col1:
                    novas_permissoes[modulo] = st.checkbox(
                        modulo,
                        value=valor_padrao,
                        key=f"perm_{usuario_sel['username']}_{modulo}",
                        disabled=disabled
                    )
            else:
                with col2:
                    novas_permissoes[modulo] = st.checkbox(
                        modulo,
                        value=valor_padrao,
                        key=f"perm_{usuario_sel['username']}_{modulo}",
                        disabled=disabled
                    )

        if st.button("Salvar permissões por módulo", use_container_width=True, disabled=(usuario_sel["username"] == "AndersonMPMelo")):
            for modulo, permitido in novas_permissoes.items():
                conn.execute("""
                    INSERT INTO usuario_modulos(username, modulo, permitido)
                    VALUES (?, ?, ?)
                    ON CONFLICT(username, modulo)
                    DO UPDATE SET permitido = excluded.permitido
                """, (
                    usuario_sel["username"],
                    modulo,
                    1 if permitido else 0
                ))

            conn.commit()
            st.success("Permissões atualizadas com sucesso.")
            st.rerun()

        st.divider()
        st.markdown("#### Excluir usuário")
        st.warning("A exclusão remove o usuário e suas permissões. As requisições históricas permanecerão registradas com o nome do solicitante.")

        bloqueia_exclusao = (
            usuario_sel["username"] == st.session_state.usuario
            or usuario_sel["username"] == "AndersonMPMelo"
        )

        confirmar_exclusao = st.checkbox(
            f"Confirmo que desejo excluir o usuário {usuario_sel['username']}",
            key="confirmar_excluir_usuario",
            disabled=bloqueia_exclusao
        )

        if st.button(
            "Excluir usuário selecionado",
            type="primary",
            use_container_width=True,
            disabled=(not confirmar_exclusao or bloqueia_exclusao)
        ):
            try:
                excluir_usuario(usuario_sel["username"])
                st.success("Usuário excluído com sucesso.")
                st.rerun()
            except ValueError as e:
                st.error(str(e))

        section_box_end()
