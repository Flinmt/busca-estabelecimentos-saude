import streamlit as st
import json
from google.oauth2 import service_account
from pandas_gbq import read_gbq

# Configurações
st.set_page_config(page_title="Busca CNES", layout="wide")

# Conexão com o BigQuery
@st.cache_resource
def get_credentials():
    if "GCP_CREDENTIALS" in st.secrets:
        creds_dict = json.loads(st.secrets["GCP_CREDENTIALS"])
        return service_account.Credentials.from_service_account_info(creds_dict)
    else:
        st.error("Credenciais GCP não encontradas em st.secrets.")
        st.stop()

# Exemplo de uso
def carregar_dados(query):
    credentials = get_credentials()
    df = read_gbq(query, credentials=credentials, project_id=credentials.project_id)
    return df

# Consulta paginada com filtros
@st.cache_data(ttl=600)
def get_data(estado=None, municipio=None, pagina=1, limite=100):
    offset = (pagina - 1) * limite

    where_clauses = []
    if estado:
        where_clauses.append(f"estado = '{estado}'")
    if municipio:
        where_clauses.append(f"municipio = '{municipio}'")

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    query = f"""
        SELECT * 
        FROM `bigquery3cx.estabelecimentos_saude.estabelecimentos_saude`
        {where_sql}
        ORDER BY estado
        LIMIT {limite}
        OFFSET {offset}
    """
    return read_gbq(query, credentials=get_credentials())

# Contagem total de linhas para paginação
@st.cache_data(ttl=600)
def get_total_rows(estado=None, municipio=None):
    where_clauses = []
    if estado:
        where_clauses.append(f"estado = '{estado}'")
    if municipio:
        where_clauses.append(f"municipio = '{municipio}'")

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    query = f"""
        SELECT COUNT(*) as total 
        FROM `bigquery3cx.estabelecimentos_saude.estabelecimentos_saude`
        {where_sql}
    """
    df = read_gbq(query, credentials=get_credentials())
    return int(df["total"].iloc[0])

# API CNES
def buscar_por_cnes(cnes):
    url = f"https://apidadosabertos.saude.gov.br/cnes/estabelecimentos/{cnes}"
    try:
        response = requests.get(url, timeout=10)
        return response.json() if response.status_code == 200 else None
    except requests.RequestException:
        return None

# Interface
st.title("🔍 Busca de Estabelecimentos de Saúde")
tab1, tab2 = st.tabs(["Banco de Dados", "API CNES"])

# --- ABA 1: Banco de Dados ---
with tab1:
    st.header("Filtrar no Banco de Dados")

    # Carrega estado e município para dropdowns
    df_estados_municipios = read_gbq(
        """
        SELECT DISTINCT estado, municipio
        FROM `bigquery3cx.estabelecimentos_saude.estabelecimentos_saude`
        WHERE estado IS NOT NULL AND municipio IS NOT NULL
        """,
        credentials=get_credentials()
    )

    col1, col2 = st.columns(2)

    with col1:
        estado = st.selectbox(
            "Estado",
            options=[""] + sorted(df_estados_municipios['estado'].dropna().unique()),
            index=0
        )

    with col2:
        municipios = df_estados_municipios[df_estados_municipios['estado'] == estado]['municipio'].dropna().unique() if estado else []
        municipio = st.selectbox(
            "Município",
            options=[""] + sorted(municipios) if estado else [],
            disabled=not estado,
            index=0
        )

    total_linhas = get_total_rows(estado, municipio)
    linhas_por_pagina = 100
    total_paginas = (total_linhas // linhas_por_pagina) + int(total_linhas % linhas_por_pagina > 0)

    pagina = st.number_input("Página", min_value=1, max_value=max(total_paginas, 1), step=1, value=1)

    df_paginado = get_data(estado, municipio, pagina=pagina, limite=linhas_por_pagina)

    st.write(f"Exibindo {((pagina - 1) * linhas_por_pagina + 1)} a {min(pagina * linhas_por_pagina, total_linhas)} de {total_linhas} registros")
    st.dataframe(df_paginado, use_container_width=True)

# --- ABA 2: API CNES ---
with tab2:
    st.header("Consultar CNES Direto na API")
    cnes = st.text_input("Digite o código CNES:", placeholder="Ex: 1234567", key="cnes_input_unique")

    if cnes:
        if cnes.isdigit():
            with st.spinner("Buscando na API do Ministério da Saúde..."):
                dados = buscar_por_cnes(cnes)

            if dados:
                st.success("Dados encontrados!")
                col_a, col_b, col_c = st.columns(3)

                with col_a:
                    st.subheader("Informações Principais")
                    st.write(f"**Nome:** {dados.get('nome_fantasia')}")
                    st.write(f"**CNES:** {dados.get('codigo_cnes')}")
                    st.write(f"**CNPJ:** {dados.get('numero_cnpj')}")

                with col_b:
                    st.subheader("Contato")
                    st.write(f"**Bairro:** {dados.get('bairro_estabelecimento')}")
                    st.write(f"**Endereço:** {dados.get('endereco_estabelecimento')}")
                    st.write(f"**Número:** {dados.get('numero_estabelecimento')}")
                    st.write(f"**Telefone:** {dados.get('numero_telefone_estabelecimento')}")
                    st.write(f"**Email:** {dados.get('endereco_email_estabelecimento')}")

                with col_c:
                    st.subheader("Mais Informações")
                    st.write(f"**Possui Centro Cirúrgico:** {'Sim' if dados.get('estabelecimento_possui_centro_cirurgico') == 1 else 'Não'}")
                    st.write(f"**Possui Centro Obstretico:** {'Sim' if dados.get('estabelecimento_possui_centro_obstetrico') == 1 else 'Não'}")
                    st.write(f"**Possui Centro Neonatal:** {'Sim' if dados.get('estabelecimento_possui_centro_neonatal') == 1 else 'Não'}")
                    st.write(f"**Possui Atendimento Hospitalar:** {'Sim' if dados.get('estabelecimento_possui_atendimento_hospitalar') == 1 else 'Não'}")
                    st.write(f"**Possui Serviço de Apoio:** {'Sim' if dados.get('estabelecimento_possui_servico_apoio') == 1 else 'Não'}")
                    st.write(f"**Possui Atendimento Ambulatorial:** {'Sim' if dados.get('estabelecimento_possui_atendimento_ambulatorial') == 1 else 'Não'}")
                    st.write(f"**Atualizado em:** {dados.get('data_atualizacao')}")
            else:
                st.error("CNES não encontrado ou erro na API.")
        else:
            st.warning("Digite apenas números no campo CNES.")
