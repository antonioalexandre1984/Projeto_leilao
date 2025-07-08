import streamlit as st
import pandas as pd
import psycopg2
from sklearn.linear_model import LinearRegression
import google.generativeai as genai
from dotenv import load_dotenv
import os

# Carregar variáveis do .env (para chaves de API, etc.).
# As variáveis definidas no docker-compose.yml terão prioridade, o que é ideal para o ambiente Docker.
load_dotenv()

# Variáveis de ambiente para conexão com o banco de dados e API Gemini
DB_HOST = os.getenv("PG_HOST") 
DB_NAME = os.getenv("PG_DATABASE")
DB_USER = os.getenv("PG_USER")
DB_PASS = os.getenv("PG_PASSWORD")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Inicializa o estado da sessão para armazenar a resposta do Gemini
if 'gemini_response' not in st.session_state: # Linha corrigida aqui
    st.session_state.gemini_response = ""
if 'selected_lote_data' not in st.session_state:
    st.session_state.selected_lote_data = None

# Configurar conexão com banco de dados
@st.cache_data
def carregar_dados():
    """
    Carrega dados das tabelas 'leilo', 'parque_leiloes_oficial' e 'loop' do banco de dados PostgreSQL.
    Mapeia as colunas para um formato padronizado e combina os dados.
    Usa st.cache_data para cachear os dados e evitar recarregar desnecessariamente.
    """
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port="5432" # Porta padrão do PostgreSQL dentro da rede Docker
        )

        all_data = []

        # Função auxiliar para buscar e padronizar dados de uma tabela
        def fetch_table_data(table_name, column_map, conn):
            # Seleciona apenas as colunas que existem no mapa
            cols_to_select = ", ".join(column_map.keys())
            query = f"SELECT {cols_to_select} FROM {table_name}"
            
            try:
                df = pd.read_sql(query, conn)
                df.rename(columns=column_map, inplace=True)
                df['source_table'] = table_name # Adiciona a tabela de origem para rastreamento
                st.success(f"Dados da tabela '{table_name}' carregados com sucesso!")
                return df
            except Exception as e:
                st.warning(f"Não foi possível carregar dados da tabela '{table_name}'. Verifique se a tabela existe e as colunas estão corretas. Erro: {e}")
                return pd.DataFrame()

        # Mapeamento de colunas para a tabela 'leilo'
        leilo_columns = {
            "veiculo_ano_fabricacao": "ano_fabricacao",
            "veiculo_data_leilao": "data_leilao",
            "veiculo_tipo_combustivel": "tipo_combustivel",
            "veiculo_cor": "cor",
            "veiculo_possui_chave": "possui_chave",
            "veiculo_tipo_retomada": "tipo_retomada",
            "veiculo_tipo": "tipo_veiculo",
            "veiculo_valor_fipe": "valor_fipe",
            "veiculo_fabricante": "fabricante",
            "veiculo_imagem": "imagem",
            "veiculo_km": "km",
            "veiculo_link_lote": "link_lote",
            "veiculo_modelo": "modelo",
            "veiculo_situacao": "situacao",
            "veiculo_titulo": "titulo",
            "veiculo_patio_uf": "patio_uf",
            "veiculo_valor_lance_atual": "preco_lote", # Preço primário para regressão
        }
        df_leilo = fetch_table_data("leilo", leilo_columns, conn)
        if not df_leilo.empty:
            all_data.append(df_leilo)

        # Mapeamento de colunas para a tabela 'parque_leiloes_oficial'
        parque_leiloes_oficial_columns = {
            "Veiculo_Ano_Fabricacao": "ano_fabricacao",
            "Veiculo_Ano_Modelo": "ano_modelo",
            "Veiculo_Condicao_Motor": "condicao_motor",
            "Veiculo_Data_Leilao": "data_leilao",
            "Veiculo_Fabricante": "fabricante",
            "Veiculo_Final_Placa": "final_placa",
            "Veiculo_Imagem": "imagem",
            "Veiculo_KM": "km",
            "Veiculo_Lance_Inicial": "preco_lote", # Preço primário para regressão
            "Veiculo_Link_Lote": "link_lote",
            "Veiculo_Modelo": "modelo",
            "Veiculo_Possui_Chave": "possui_chave",
            "Veiculo_Tipo_Combustivel": "tipo_combustivel",
            "Veiculo_Tipo_Retomada": "tipo_retomada",
            "Veiculo_Titulo": "titulo",
            "Veiculo_Total_Lances": "total_lances",
            "Veiculo_Valor_Fipe": "valor_fipe",
        }
        df_parque = fetch_table_data("parque_leiloes_oficial", parque_leiloes_oficial_columns, conn)
        if not df_parque.empty:
            all_data.append(df_parque)

        # Mapeamento de colunas para a tabela 'loop'
        loop_columns = {
            "veiculo_link_lote": "link_lote",
            "veiculo_titulo": "titulo",
            "veiculo_fabricante": "fabricante",
            "veiculo_modelo": "modelo",
            "veiculo_versao": "versao",
            "veiculo_ano_fabricacao": "ano_fabricacao",
            "Veiculo_Ano_Modelo": "ano_modelo",
            "Veiculo_Valor_Fipe": "valor_fipe",
            "veiculo_blindado": "blindado",
            "veiculo_chave": "possui_chave", # Mapeia 'veiculo_chave' para 'possui_chave'
            "veiculo_funcionando": "funcionando",
            "Veiculo_Tipo_Combustivel": "tipo_combustivel",
            "veiculo_km": "km",
            "Veiculo_Total_Lances": "total_lances",
            "veiculo_numero_visualizacoes": "numero_visualizacoes",
            "Veiculo_Data_Leilao": "data_leilao",
            "veiculo_horario_leilao": "horario_leilao",
            "Veiculo_Lance_Inicial": "preco_lote", # Preço primário para regressão
            "veiculo_situacao": "situacao",
        }
        df_loop = fetch_table_data("loop", loop_columns, conn)
        if not df_loop.empty:
            all_data.append(df_loop)

        conn.close()

        if all_data:
            df_combined = pd.concat(all_data, ignore_index=True)
            
            # Harmoniza a coluna 'ano' para a regressão: prefere 'ano_fabricacao', depois 'ano_modelo'
            df_combined['ano'] = df_combined['ano_fabricacao'].fillna(df_combined['ano_modelo'])
            
            st.success("Dados carregados e combinados de todas as tabelas com sucesso!")
            return df_combined
        else:
            st.warning("Nenhum dado foi carregado de nenhuma das tabelas especificadas. Verifique as configurações do banco de dados e os nomes das tabelas/colunas.")
            return pd.DataFrame()

    except Exception as e:
        st.error(f"Erro geral ao conectar ao banco de dados ou carregar dados: {e}")
        st.warning(f"Detalhes da conexão (verificados no container 'analyzer'): Host={DB_HOST}, DB={DB_NAME}, User={DB_USER}")
        return pd.DataFrame()

# Estimar valor de mercado
def estimar_valor(df):
    """
    Estima o valor de mercado dos veículos usando Regressão Linear
    e calcula o percentual de desconto e oportunidade.
    Considerações para melhoria: Para dados reais, considere mais features (e.g., tipo_combustível)
    e modelos de ML mais complexos (e.g., Random Forest) para maior precisão.
    """
    # Certifica-se de que os tipos de dados estão corretos para o modelo
    df['ano'] = pd.to_numeric(df['ano'], errors='coerce') # 'ano' é agora harmonizado em carregar_dados
    df['km'] = pd.to_numeric(df['km'], errors='coerce')
    df['preco_lote'] = pd.to_numeric(df['preco_lote'], errors='coerce')

    # Remove linhas com valores NaN resultantes da conversão, se houver
    # Também remove linhas onde 'preco_lote' é 0 ou NaN, pois é a variável alvo
    df.dropna(subset=['ano', 'km', 'preco_lote'], inplace=True)
    df = df[df['preco_lote'] > 0] # Garante que o preço seja positivo para cálculos significativos

    if df.empty or len(df) < 2: # Necessita de pelo menos 2 amostras para regressão
        st.warning("Dados insuficientes para estimar o valor de mercado. Necessita de pelo menos 2 amostras válidas com preço de lote positivo.")
        df['preco_estimado'] = 0.0
        df['desconto_percentual'] = 0.0
        df['oportunidade'] = False
        return df
    
    modelo = LinearRegression()
    X = df[['ano', 'km']]
    y = df['preco_lote']
    
    try:
        modelo.fit(X, y)
        df['preco_estimado'] = modelo.predict(X)
        
        # Evita divisão por zero ou preço estimado negativo
        df['desconto_percentual'] = ((df['preco_estimado'] - df['preco_lote']) / df['preco_estimado']) * 100
        df.loc[df['preco_estimado'] <= 0, 'desconto_percentual'] = 0 # Define como 0 se o preço estimado for não-positivo
        
        df['oportunidade'] = df['desconto_percentual'] > 20 # Define oportunidade se o desconto for > 20%
    except Exception as e:
        st.error(f"Erro ao estimar valores de mercado: {e}. Verifique se há variância suficiente nos dados para a regressão.")
        df['preco_estimado'] = 0.0
        df['desconto_percentual'] = 0.0
        df['oportunidade'] = False

    return df

# Função para consultar Gemini, agora com um botão de ação
def call_gemini_api(modelo, ano, km, preco_lote, preco_estimado):
    """
    Função auxiliar para chamar a API do Gemini.
    """
    if not GEMINI_API_KEY:
        st.session_state.gemini_response = "Erro: Chave de API do Gemini não configurada. Por favor, defina GEMINI_API_KEY."
        return

    # ALTERAÇÃO AQUI: Chave de API configurada globalmente.
    # Removendo 'api_key' do construtor GenerativeModel
    genai.configure(api_key=GEMINI_API_KEY)
    
    preco_lote_formatado = f"{preco_lote:.2f}" if isinstance(preco_lote, (int, float)) else str(preco_lote)
    preco_estimado_formatado = f"{preco_estimado:.2f}" if isinstance(preco_estimado, (int, float)) else str(preco_estimado)

    prompt = f"""
    Analise a seguinte oportunidade de compra de veículo em leilão:
    Modelo: {modelo}
    Ano: {ano}
    Quilometragem: {km}
    Preço no leilão: R$ {preco_lote_formatado}
    Valor estimado de mercado: R$ {preco_estimado_formatado}

    Isso representa uma boa oportunidade de compra? Qual é a sua justificativa?
    Considere o preço do leilão em comparação com o valor de mercado estimado, bem como a idade (ano) e a quilometragem do veículo.
    Se o preço do leilão for significativamente menor que o valor de mercado, é provável que seja uma boa oportunidade.
    """
    try:
        # Removendo 'api_key' do construtor GenerativeModel
        model = genai.GenerativeModel("gemini-2.0-flash") 
        with st.spinner("Consultando a inteligência artificial do Gemini..."):
            response = model.generate_content(prompt)
            st.session_state.gemini_response = response.text.strip()
    except Exception as e:
        st.session_state.gemini_response = f"Erro ao consultar Gemini: {e}"

# Interface do Streamlit
st.title("🚗 Análise de Leilões de Carros com IA")
st.markdown("Bem-vindo ao seu painel de análise de leilões! Conectamos ao seu banco de dados para identificar as melhores oportunidades de compra de veículos.")

df = carregar_dados()

if not df.empty:
    df = estimar_valor(df)
    
    # Filtra e exibe lotes com oportunidade
    st.subheader("📊 Lotes com Oportunidade de Compra")
    top_lotes = df[df['oportunidade']].copy() # Usar .copy() para evitar SettingWithCopyWarning

    if not top_lotes.empty:
        top_lotes = top_lotes.sort_values(by='desconto_percentual', ascending=False)
        st.dataframe(top_lotes[['modelo', 'ano', 'km', 'preco_lote', 'preco_estimado', 'desconto_percentual', 'oportunidade', 'source_table']])
        st.info(f"Foram encontradas {len(top_lotes)} oportunidades de compra.")
    else:
        st.info("Nenhum lote identificado como oportunidade de compra no momento (desconto percentual inferior a 20%).")

    # Avaliação Detalhada com IA
    st.subheader("🔎 Avaliação Detalhada com IA")
    
    if not top_lotes.empty:
        # Seleção do modelo
        modelos_disponiveis = top_lotes['modelo'].unique()
        modelo_selecionado = st.selectbox(
            "Escolha um modelo para análise detalhada com IA:", 
            modelos_disponiveis, 
            key="model_selector"
        )
        
        # Armazena os dados do lote selecionado no session_state para uso no callback do botão
        lote_selecionado_data = top_lotes[top_lotes['modelo'] == modelo_selecionado].iloc[0]
        st.session_state.selected_lote_data = lote_selecionado_data

        # Botão para consultar a IA
        if st.button("Consultar IA para este Lote", key="consult_gemini_button"):
            call_gemini_api(
                modelo=st.session_state.selected_lote_data['modelo'],
                ano=st.session_state.selected_lote_data['ano'],
                km=st.session_state.selected_lote_data['km'],
                preco_lote=st.session_state.selected_lote_data['preco_lote'],
                preco_estimado=st.session_state.selected_lote_data['preco_estimado']
            )
        
        # Exibe a resposta do Gemini se já tiver sido gerada
        if st.session_state.gemini_response:
            st.markdown("### 🤖 Resposta da Análise de IA (Gemini):")
            st.write(st.session_state.gemini_response)

    else:
        st.info("Não há lotes com oportunidade para análise detalhada no momento.")
else:
    st.warning("Nenhum dado encontrado para análise. Verifique a conexão com o banco de dados e se há dados nas tabelas 'leilo', 'parque_leiloes_oficial' ou 'loop'.")

st.markdown("---")
st.markdown("Desenvolvido para análise de leilões de veículos.")