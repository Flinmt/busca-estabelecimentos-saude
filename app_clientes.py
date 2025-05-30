import streamlit as st
import pandas as pd
import requests
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Configurações
load_dotenv()
st.set_page_config(page_title="Busca CNES", layout="wide")

# Conexão com o Neon
@st.cache_resource
def get_db():
    return create_engine(os.getenv("NEON_DB_URL"))

# Função auxiliar para buscar dados do banco
@st.cache_data(ttl=60)
def get_data():
    return pd.read_sql("SELECT * FROM estabelecimentos_saude", get_db())

# API CNES (Dados públicos do Ministério da Saúde)
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
    df = get_data()

    col1, col2 = st.columns(2)

    with col1:
        estado = st.selectbox(
            "Estado",
            options=[""] + sorted(df['estado'].dropna().unique()),
            index=0
        )

    with col2:
        municipios = df[df['estado'] == estado]['municipio'].dropna().unique() if estado else []
        municipio = st.selectbox(
            "Município",
            options=[""] + sorted(municipios) if estado else [],
            disabled=not estado,
            index=0
        )

    # Filtragem
    df_filtrado = df.copy()
    if estado:
        df_filtrado = df_filtrado[df_filtrado['estado'] == estado]
        if municipio:
            df_filtrado = df_filtrado[df_filtrado['municipio'] == municipio]

    # Paginação
    linhas_por_pagina = 100
    total_linhas = len(df_filtrado)
    total_paginas = (total_linhas // linhas_por_pagina) + int(total_linhas % linhas_por_pagina > 0)

    pagina = st.number_input("Página", min_value=1, max_value=max(total_paginas, 1), step=1, value=1)

    inicio = (pagina - 1) * linhas_por_pagina
    fim = inicio + linhas_por_pagina
    df_paginado = df_filtrado.iloc[inicio:fim]

    st.write(f"Exibindo {inicio + 1} a {min(fim, total_linhas)} de {total_linhas} registros")

    st.dataframe(df_paginado, use_container_width=True)

# --- ABA 2: API CNES ---
with tab2:
    st.header("Consultar CNES Direto na API")
    cnes = st.text_input("Digite o código CNES:", placeholder="Ex: 1234567")

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

