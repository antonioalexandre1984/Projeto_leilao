import streamlit as st
import pandas as pd
import psycopg2
import google.generativeai as genai
from dotenv import load_dotenv
import os
import locale # Importar para formatação de moeda
from datetime import datetime, date # Importar datetime e date

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
if 'gemini_response' not in st.session_state:
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
            # Obtém as colunas reais na tabela para evitar erros de coluna inexistente
            cursor = conn.cursor()
            cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
            db_columns = [row[0] for row in cursor.fetchall()]
            cursor.close()

            # Cria um novo mapa contendo apenas as colunas que realmente existem no DB
            existing_column_map = {db_col: mapped_col for db_col, mapped_col in column_map.items() if db_col in db_columns}
            
            if not existing_column_map:
                st.warning(f"Nenhuma coluna mapeada encontrada na tabela '{table_name}'. Verifique o mapeamento ou a existência da tabela.")
                return pd.DataFrame()

            cols_to_select = ", ".join(existing_column_map.keys())
            query = f"SELECT {cols_to_select} FROM {table_name}"
            
            try:
                df = pd.read_sql(query, conn)
                df.rename(columns=existing_column_map, inplace=True)
                df['source_table'] = table_name # Adiciona a tabela de origem para rastreamento
                st.success(f"Dados da tabela '{table_name}' carregados com sucesso!")
                st.info(f"Shape de df_{table_name}: {df.shape}") # Debug: show shape of fetched table
                return df
            except Exception as e:
                st.warning(f"Não foi possível carregar dados da tabela '{table_name}'. Verifique se a tabela existe e as colunas estão corretas. Erro: {e}")
                return pd.DataFrame()

        # Mapeamento de colunas para a tabela 'leilo'
        leilo_columns = {
            "veiculo_titulo": "titulo",
            "veiculo_link_lote": "link_lote",
            "veiculo_imagem": "imagem",
            "veiculo_patio_uf": "patio_uf",
            "veiculo_ano_fabricacao": "ano_fabricacao", 
            "veiculo_km": "km",
            "veiculo_valor_lance_atual": "preco_lote", # Mapeado para preco_lote
            #"veiculo_situacao": "situacao", # Removido se não estiver presente ou for inconsistente
            "veiculo_data_leilao": "data_leilao",
            "veiculo_tipo_combustivel": "tipo_combustivel",
            "veiculo_cor": "cor",
            "veiculo_possui_chave": "possui_chave",
            "veiculo_tipo_retomada": "tipo_retomada",
            "veiculo_tipo": "tipo_veiculo", # Mapeado para tipo_veiculo
            "veiculo_valor_fipe": "valor_fipe", # Mapeado para valor_fipe
            "veiculo_modelo": "modelo",
            "veiculo_fabricante": "fabricante",
            "veiculo_patio_uf_localizacao": "patio_uf_localizacao" 
        }
        df_leilo = fetch_table_data("leilo", leilo_columns, conn)
        if not df_leilo.empty:
            all_data.append(df_leilo)

        # Mapeamento de colunas para a tabela 'parque_leiloes_oficial'
        parque_leiloes_oficial_columns = {
            "veiculo_titulo": "titulo",
            "veiculo_link_lote": "link_lote",
            "veiculo_imagem": "imagem",
            "veiculo_km": "km",
            "veiculo_valor_lance_atual": "preco_lote", # Mapeado para preco_lote
            "veiculo_data_leilao": "data_leilao",
            "veiculo_fabricante": "fabricante",
            "veiculo_final_placa": "final_placa",
            "veiculo_ano_fabricacao": "ano_fabricacao",
            "veiculo_ano_modelo": "ano_modelo",
            "veiculo_possui_chave": "possui_chave",
            "veiculo_condicao_motor": "condicao_motor",
            "veiculo_valor_fipe": "valor_fipe", # Mapeado para valor_fipe
            "veiculo_tipo_combustivel": "tipo_combustivel",
            "veiculo_tipo_retomada": "tipo_retomada",
            "veiculo_total_lances": "total_lances",
            "veiculo_modelo": "modelo",
            # Adicionar veiculo_tipo se existir nesta tabela e quiser mapear
            "veiculo_tipo": "tipo_veiculo", 
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
            "veiculo_ano_modelo": "ano_modelo", 
            "veiculo_valor_fipe": "valor_fipe", # Mapeado para valor_fipe
            "veiculo_blindado": "blindado",
            "veiculo_chave": "possui_chave", 
            "veiculo_funcionando": "funcionando",
            "veiculo_tipo_combustivel": "tipo_combustivel", 
            "veiculo_km": "km",
            "veiculo_total_lances": "total_lances", 
            "veiculo_numero_visualizacoes": "numero_visualizacoes",
            "veiculo_data_leilao": "data_leilao", 
            "veiculo_horario_leilao": "horario_leilao",
            "veiculo_lance_atual": "preco_lote", # Mapeado para preco_lote
            "veiculo_situacao_lote": "situacao",
            # Adicionar veiculo_tipo se existir nesta tabela e quiser mapear
            "veiculo_tipo": "tipo_veiculo", 
        }
        df_loop = fetch_table_data("loop", loop_columns, conn)
        if not df_loop.empty:
            all_data.append(df_loop)

        conn.close()

        if all_data:
            df_combined = pd.concat(all_data, ignore_index=True)
            
            # Harmoniza a coluna 'ano' para exibição e prompt do Gemini
            df_combined['ano'] = df_combined['ano_fabricacao'].fillna(df_combined['ano_modelo'])
            
            # Converte a coluna 'data_leilao' para datetime
            df_combined['data_leilao'] = pd.to_datetime(df_combined['data_leilao'], errors='coerce').dt.date # Convert to date object
            
            # Garante que todas as colunas padronizadas existam, preenchendo com None ou tipo default
            # Lista completa de colunas padronizadas esperadas após o mapeamento
            standard_columns = [
                "titulo", "link_lote", "imagem", "patio_uf", "ano_fabricacao", "km",
                "preco_lote", "situacao", "data_leilao", "tipo_combustivel", "cor",
                "possui_chave", "tipo_retomada", "tipo_veiculo", "valor_fipe",
                "modelo", "fabricante", "patio_uf_localizacao", "ano_modelo",
                "condicao_motor", "final_placa", "total_lances", "versao",
                "blindado", "funcionando", "numero_visualizacoes", "horario_leilao",
                "source_table", "ano", "tipo_veiculo" # Adicionado tipo_veiculo
            ]

            for col in standard_columns:
                if col not in df_combined.columns:
                    df_combined[col] = None 

            st.success("Dados carregados e combinados de todas as tabelas com sucesso!")
            st.info(f"Shape de df_combined após concatenação: {df_combined.shape}") # Debug: show shape of combined df
            return df_combined
        else:
            st.warning("Nenhum dado foi carregado de nenhuma das tabelas especificadas. Verifique as configurações do banco de dados e os nomes das tabelas/colunas.")
            return pd.DataFrame()

    except Exception as e:
        st.error(f"Erro geral ao conectar ao banco de dados ou carregar dados: {e}")
        st.warning(f"Detalhes da conexão (verificados no container 'analyzer'): Host={DB_HOST}, DB={DB_NAME}, User={DB_USER}")
        return pd.DataFrame()

# Estimar valor de mercado e calcular desconto
def estimar_valor(df):
    """
    Calcula o valor de mercado dos veículos usando a coluna 'valor_fipe'
    e calcula o percentual de desconto e oportunidade.
    """
    # Cria uma cópia para evitar SettingWithCopyWarning
    df_processed = df.copy()

    # Pré-processa as colunas de preço e valor de mercado se forem strings com formatação brasileira
    # Substitui o ponto de milhar por nada e a vírgula decimal por ponto, antes da conversão para numérico
    if df_processed['preco_lote'].dtype == 'object': # Verifica se a coluna é de strings
        df_processed['preco_lote'] = df_processed['preco_lote'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
    if df_processed['valor_fipe'].dtype == 'object': # Verifica se a coluna é de strings
        df_processed['valor_fipe'] = df_processed['valor_fipe'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)

    # Certifica-se de que os tipos de dados estão corretos após o pré-processamento
    df_processed['ano'] = pd.to_numeric(df_processed['ano'], errors='coerce') 
    df_processed['km'] = pd.to_numeric(df_processed['km'], errors='coerce')
    df_processed['preco_lote'] = pd.to_numeric(df_processed['preco_lote'], errors='coerce') 
    df_processed['valor_fipe'] = pd.to_numeric(df_processed['valor_fipe'], errors='coerce')

    # Renomeia 'valor_fipe' para 'valor_mercado' conforme solicitado
    df_processed['valor_mercado'] = df_processed['valor_fipe']

    # Inicializa as colunas de desconto e oportunidade
    df_processed['desconto_percentual'] = 0.0
    df_processed['oportunidade'] = False

    # Identifica linhas válidas para cálculo (preco_lote e valor_mercado > 0 e não NaN)
    valid_mask = (df_processed['preco_lote'].notna()) & \
                 (df_processed['valor_mercado'].notna()) & \
                 (df_processed['preco_lote'] > 0) & \
                 (df_processed['valor_mercado'] > 0)

    st.info(f"Shape de df antes do cálculo de oportunidade: {df_processed.shape}") # Debug: show shape before calculation

    if valid_mask.any(): # Verifica se há pelo menos uma linha válida para cálculo
        # Calcula o percentual de desconto apenas para as linhas válidas
        df_processed.loc[valid_mask, 'desconto_percentual'] = \
            ((df_processed.loc[valid_mask, 'valor_mercado'] - df_processed.loc[valid_mask, 'preco_lote']) / 
             df_processed.loc[valid_mask, 'valor_mercado']) * 100
        
        # Garante que o desconto não seja negativo se o preço do lote for maior que o valor de mercado
        df_processed.loc[df_processed['desconto_percentual'] < 0, 'desconto_percentual'] = 0 
        
        # Define oportunidade para as linhas válidas com desconto > 20
        df_processed.loc[valid_mask & (df_processed['desconto_percentual'] > 20), 'oportunidade'] = True
    else:
        st.warning("Nenhum dado com 'preco_lote' e 'valor_mercado' válidos para calcular o desconto e oportunidade.")
    
    st.info(f"Shape de df após cálculo de oportunidade: {df_processed.shape}") # Debug: show shape after calculation
    return df_processed

# Função para formatar valores como moeda brasileira
def formatar_moeda_brl(valor):
    if pd.isna(valor):
        return "N/A"
    # Configura o locale para português do Brasil
    try:
        locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
    except locale.Error:
        # Se 'pt_BR.UTF-8' não estiver disponível, tenta outra opção comum ou apenas formata manualmente
        # (Isso pode acontecer em alguns ambientes Docker, por exemplo)
        try:
            locale.setlocale(locale.LC_ALL, 'pt_BR')
        except locale.Error:
            # Fallback para formatação manual se locale não puder ser definido
            return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


    return locale.currency(valor, grouping=True, symbol=True)

# Função para consultar Gemini, agora com um botão de ação
def call_gemini_api(modelo, ano, km, preco_lote, valor_mercado): # Renomeado preco_estimado para valor_mercado
    """
    Função auxiliar para chamar a API do Gemini.
    """
    if not GEMINI_API_KEY:
        st.session_state.gemini_response = "Erro: Chave de API do Gemini não configurada. Por favor, defina GEMINI_API_KEY."
        return

    genai.configure(api_key=GEMINI_API_KEY)
    
    # Formata os valores para o prompt do Gemini
    preco_lote_formatado = formatar_moeda_brl(preco_lote)
    valor_mercado_formatado = formatar_moeda_brl(valor_mercado) # Renomeado preco_estimado_formatado

    prompt = f"""
    Analise a seguinte oportunidade de compra de veículo em leilão:
    Modelo: {modelo}
    Ano: {ano}
    Quilometragem: {km}
    Preço no leilão: {preco_lote_formatado}
    Valor de mercado (FIPE): {valor_mercado_formatado}

    Isso representa uma boa oportunidade de compra? Qual é a sua justificativa?
    Considere o preço do leilão em comparação com o valor de mercado (FIPE), bem como a idade (ano) e a quilometragem do veículo.
    Se o preço do leilão for significativamente menor que o valor de mercado (FIPE), é provável que seja uma boa oportunidade.
    """
    try:
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
    
    # --- Debugging Section (Temporário) ---
    st.sidebar.subheader("Debug: Dados Carregados (Todos os Lotes)")
    st.sidebar.write(f"Total de linhas carregadas e processadas: {df.shape[0]}")
    if st.sidebar.checkbox("Mostrar todos os dados processados (incluindo não-oportunidades)"):
        st.sidebar.dataframe(df)
    # --- Fim da Seção de Debugging ---

    # Filtra e exibe lotes com oportunidade
    st.subheader("📊 Lotes com Oportunidade de Compra")
    top_lotes = df[df['oportunidade']].copy() 

    if not top_lotes.empty:
        top_lotes = top_lotes.sort_values(by='desconto_percentual', ascending=False)
        
        # Reorganização dos filtros
        # Linha 1: Modelo e Ano
        col_model, col_year = st.columns(2)
        with col_model:
            modelos_disponiveis = ['Todos'] + sorted(df['modelo'].dropna().unique().tolist())
            selected_modelo = st.selectbox("Filtrar por Modelo:", modelos_disponiveis, key="filter_modelo")
            if selected_modelo != 'Todos':
                top_lotes = top_lotes[top_lotes['modelo'] == selected_modelo]
        
        with col_year:
            anos_disponiveis = ['Todos'] + sorted(top_lotes['ano'].dropna().astype(int).unique().tolist(), reverse=True)
            selected_ano = st.selectbox("Filtrar por Ano:", anos_disponiveis, key="filter_ano")
            if selected_ano != 'Todos':
                top_lotes = top_lotes[top_lotes['ano'] == selected_ano]

        # Linha 2: Preço do Lote e Valor de Mercado
        col_price_lote, col_valor_mercado = st.columns(2)
        with col_price_lote:
            if not top_lotes.empty:
                min_preco_lote = float(top_lotes['preco_lote'].min()) if not top_lotes['preco_lote'].empty else 0.0
                max_preco_lote = float(top_lotes['preco_lote'].max()) if not top_lotes['preco_lote'].empty else 1000000.0
                
                if min_preco_lote == max_preco_lote:
                    max_preco_lote += 0.01 

                preco_lote_range = st.slider(
                    "Filtrar por Preço do Lote (R$):",
                    min_value=min_preco_lote,
                    max_value=max_preco_lote,
                    value=(min_preco_lote, max_preco_lote),
                    format="R$ %.2f", # Formato C-style. Para formatação completa BRL (milhar, decimal), 'format_func' seria ideal, mas não é suportado nesta versão do Streamlit.
                    key="filter_preco_lote"
                )
                top_lotes = top_lotes[(top_lotes['preco_lote'] >= preco_lote_range[0]) & 
                                      (top_lotes['preco_lote'] <= preco_lote_range[1])]

        with col_valor_mercado:
            if not top_lotes.empty:
                min_valor_mercado = float(top_lotes['valor_mercado'].min()) if not top_lotes['valor_mercado'].empty else 0.0
                max_valor_mercado = float(top_lotes['valor_mercado'].max()) if not top_lotes['valor_mercado'].empty else 1000000.0

                if min_valor_mercado == max_valor_mercado:
                    max_valor_mercado += 0.01 

                valor_mercado_range = st.slider(
                    "Filtrar por Valor de Mercado (R$):",
                    min_value=min_valor_mercado,
                    max_value=max_valor_mercado,
                    value=(min_valor_mercado, max_valor_mercado),
                    format="R$ %.2f", # Formato C-style. Para formatação completa BRL (milhar, decimal), 'format_func' seria ideal, mas não é suportado nesta versão do Streamlit.
                    key="filter_valor_mercado"
                )
                top_lotes = top_lotes[(top_lotes['valor_mercado'] >= valor_mercado_range[0]) & 
                                      (top_lotes['valor_mercado'] <= valor_mercado_range[1])]
            
        # Linha 3: Data do Leilão (centralizado em 60%)
        col_date_left, col_date_center, col_date_right = st.columns([0.2, 0.6, 0.2]) # 20% | 60% | 20%
        with col_date_center:
            if not top_lotes.empty and not top_lotes['data_leilao'].dropna().empty:
                min_date_available = top_lotes['data_leilao'].min()
                max_date_available = top_lotes['data_leilao'].max()

                if isinstance(min_date_available, datetime):
                    min_date_available = min_date_available.date()
                if isinstance(max_date_available, datetime):
                    max_date_available = max_date_available.date()

                default_start_date = min_date_available
                default_end_date = max_date_available

                st.markdown("##### Filtrar por Data do Leilão (DD/MM/AAAA):") # Título para o filtro de data
                date_range = st.date_input(
                    "", # Rótulo vazio para que o título acima seja usado
                    value=(default_start_date, default_end_date),
                    min_value=min_date_available,
                    max_value=max_date_available,
                    key="filter_data_leilao"
                )
                # Nota: A exibição dos dias da semana em português no calendário depende da configuração de locale do ambiente/navegador.
                # O código já tenta definir o locale pt_BR.UTF-8, mas pode requerer configuração externa no ambiente de execução.
                
                if len(date_range) == 2:
                    start_date, end_date = date_range
                    top_lotes = top_lotes[
                        (top_lotes['data_leilao'] >= start_date) & 
                        (top_lotes['data_leilao'] <= end_date)
                    ]
                elif len(date_range) == 1:
                    single_date = date_range[0]
                    top_lotes = top_lotes[top_lotes['data_leilao'] == single_date]
            else:
                st.info("Nenhuma data de leilão disponível para filtro nos lotes com oportunidade.")

        # Nova Linha para o filtro de Tipo de Veículo
        col_tipo_veiculo = st.columns(1)[0]
        with col_tipo_veiculo:
            tipos_veiculo_disponiveis = ['Todos'] + sorted(df['tipo_veiculo'].dropna().unique().tolist())
            selected_tipo_veiculo = st.selectbox("Filtrar por Tipo de Veículo:", tipos_veiculo_disponiveis, key="filter_tipo_veiculo")
            if selected_tipo_veiculo != 'Todos':
                top_lotes = top_lotes[top_lotes['tipo_veiculo'] == selected_tipo_veiculo]


        # Crie uma cópia para formatar as colunas de exibição
        df_exibicao = top_lotes.copy()
        df_exibicao['preco_lote'] = df_exibicao['preco_lote'].apply(formatar_moeda_brl)
        df_exibicao['valor_mercado'] = df_exibicao['valor_mercado'].apply(formatar_moeda_brl)
        
        # Formata a coluna de desconto percentual
        df_exibicao['desconto_percentual'] = df_exibicao['desconto_percentual'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "N/A")
        
        # Formata a coluna data_leilao para o padrão DD/MM/AAAA
        df_exibicao['data_leilao'] = df_exibicao['data_leilao'].apply(lambda x: x.strftime('%d/%m/%Y') if pd.notna(x) else "N/A")


        if not df_exibicao.empty:
            st.dataframe(df_exibicao[[
                'modelo', 
                'ano', 
                'km', 
                'preco_lote', 
                'valor_mercado', 
                'desconto_percentual', 
                'oportunidade', 
                'data_leilao', 
                'source_table',
                'tipo_veiculo' # Adicionado tipo_veiculo para visualização
            ]])
            st.info(f"Foram encontradas {len(top_lotes)} oportunidades de compra com os filtros aplicados.")
        else:
            st.info("Nenhum lote corresponde aos filtros selecionados.")
    else:
        st.info("Nenhum lote identificado como oportunidade de compra no momento (desconto percentual inferior a 20%).")

    # --- Nova seção para o gráfico de variação de valores ---
    st.subheader("📈 Variação de Valores por Modelo e Ano (Comparativo por Tabela)")

    # Filtros para o gráfico
    col_chart1, col_chart2 = st.columns(2)
    with col_chart1:
        modelos_para_grafico = sorted(df['modelo'].dropna().unique().tolist())
        selected_modelo_grafico = st.selectbox(
            "Selecione o Modelo para o Gráfico:", 
            modelos_para_grafico,
            key="chart_model_selector"
        )
    
    with col_chart2:
        anos_para_grafico = sorted(df[df['modelo'] == selected_modelo_grafico]['ano'].dropna().astype(int).unique().tolist(), reverse=True)
        if len(anos_para_grafico) > 1:
            anos_para_grafico = ['Todos'] + anos_para_grafico
        
        selected_ano_grafico = st.selectbox(
            "Selecione o Ano para o Gráfico:", 
            anos_para_grafico,
            key="chart_year_selector"
        )
    
    # Filtra os dados para o gráfico
    df_chart_filtered = df[(df['modelo'] == selected_modelo_grafico)].copy()
    if selected_ano_grafico != 'Todos':
        df_chart_filtered = df_chart_filtered[df_chart_filtered['ano'] == selected_ano_grafico]
    
    # Aplicar filtro de tipo de veículo ao gráfico
    if selected_tipo_veiculo != 'Todos':
        df_chart_filtered = df_chart_filtered[df_chart_filtered['tipo_veiculo'] == selected_tipo_veiculo]


    # Remove NaNs das colunas usadas no gráfico e garante que são numéricas
    df_chart_filtered = df_chart_filtered.dropna(subset=['preco_lote', 'valor_mercado', 'data_leilao', 'source_table'])
    df_chart_filtered['preco_lote'] = pd.to_numeric(df_chart_filtered['preco_lote'], errors='coerce')
    df_chart_filtered['valor_mercado'] = pd.to_numeric(df_chart_filtered['valor_mercado'], errors='coerce')
    df_chart_filtered = df_chart_filtered.dropna(subset=['preco_lote', 'valor_mercado']) 

    if not df_chart_filtered.empty:
        # Ordena os dados para o gráfico por data do leilão e tabela de origem
        df_chart_filtered = df_chart_filtered.sort_values(by=['data_leilao', 'source_table'])

        st.markdown(f"##### Evolução de Preços para {selected_modelo_grafico} (Ano: {selected_ano_grafico if selected_ano_grafico != 'Todos' else 'Todos os Anos'})")
        
        # Garante que 'data_leilao' é datetime antes de usar .dt accessor para strftime
        df_chart_filtered['data_leilao_dt'] = pd.to_datetime(df_chart_filtered['data_leilao'])
        
        # Criar um identificador único para cada barra, combinando data e tabela de origem
        df_chart_filtered['data_source_id'] = df_chart_filtered['data_leilao_dt'].dt.strftime('%d/%m/%Y') + ' (' + df_chart_filtered['source_table'] + ')'
        
        # Definir o índice para o gráfico
        chart_data = df_chart_filtered.set_index('data_source_id')[['preco_lote', 'valor_mercado']]
        
        st.bar_chart(chart_data)

        st.markdown("---")
        st.markdown("###### Estatísticas para o Modelo e Ano Selecionados:")
        st.write(f"**Média Preço Lote:** {formatar_moeda_brl(df_chart_filtered['preco_lote'].mean())}")
        st.write(f"**Média Valor Mercado (FIPE):** {formatar_moeda_brl(df_chart_filtered['valor_mercado'].mean())}")
        st.write(f"**Número de Veículos:** {len(df_chart_filtered)}")

    else:
        st.info("Selecione um Modelo e Ano para visualizar a variação de valores. Nenhum dado disponível para a seleção atual.")


    # Avaliação Detalhada com IA (mantém a lógica existente, usando top_lotes)
    st.subheader("🔎 Avaliação Detalhada com IA")
    
    if not top_lotes.empty:
        # Usa os modelos disponíveis após a filtragem da seção de Oportunidades de Compra
        modelos_disponiveis_para_ia = top_lotes['modelo'].unique() 
        modelo_selecionado = st.selectbox(
            "Escolha um modelo para análise detalhada com IA:", 
            modelos_disponiveis_para_ia,
            key="model_selector"
        )
        
        lote_selecionado_data = top_lotes[top_lotes['modelo'] == modelo_selecionado].iloc[0]
        st.session_state.selected_lote_data = lote_selecionado_data

        if st.button("Consultar IA para este Lote", key="consult_gemini_button"):
            call_gemini_api(
                modelo=st.session_state.selected_lote_data['modelo'],
                ano=st.session_state.selected_lote_data['ano'],
                km=st.session_state.selected_lote_data['km'],
                preco_lote=st.session_state.selected_lote_data['preco_lote'],
                valor_mercado=st.session_state.selected_lote_data['valor_mercado']
            )
        
        if st.session_state.gemini_response:
            st.markdown("### 🤖 Resposta da Análise de IA (Gemini):")
            st.write(st.session_state.gemini_response)

    else:
        st.info("Não há lotes com oportunidade para análise detalhada no momento (ou após a filtragem).")
else:
    st.warning("Nenhum dado encontrado para análise. Verifique a conexão com o banco de dados e se há dados nas tabelas 'leilo', 'parque_leiloes_oficial' ou 'loop'.")

st.markdown("---")
st.markdown("Desenvolvido para análise de leilões de veículos.")
