# Case Técnico iFood: Otimização da Estratégia de Cupons via Teste A/B e Análise de Dados

## Visão Geral do Projeto

Este repositório contém a solução desenvolvida para o Case Técnico de Data Analysis do iFood, focado em utilizar dados para direcionar uma estratégia de cupons como alavanca de crescimento. O projeto abrange desde a ingestão e tratamento de dados até a análise aprofundada de um teste A/B, segmentação de usuários, avaliação de viabilidade financeira e proposição de próximos passos estratégicos para a liderança de negócio.

## O Desafio

O objetivo central foi analisar os resultados de um teste A/B recente, desenhado para avaliar o impacto de cupons na retenção de usuários. Especificamente, o desafio incluiu:

1.  Definir e analisar indicadores de sucesso da campanha.
2.  Realizar uma análise de viabilidade financeira, explicitando premissas.
3.  Recomendar melhorias e propor um novo teste A/B.
4.  Definir segmentações de usuários relevantes, estabelecer critérios e analisar os resultados do teste A/B por segmento.
5.  Sugerir próximos passos com base na análise, defendendo-os para as lideranças de negócio com previsão de impacto.

## Metodologia e Abordagem

A solução foi construída seguindo um pipeline estruturado:

1.  **Início do Ambiente Spark:** Configuração da sessão Spark e importação das bibliotecas necessárias (PySpark para manipulação de dados, `requests`, `tarfile`, `os`, `urllib.request` para download e extração de arquivos).
2.  **Ingestão de Dados:** Leitura dos datasets brutos (Pedidos, Usuários, Restaurantes e Marcação A/B) diretamente do S3.
3.  **Preparação e Tratamento de Dados (ETL):** Aplicação de boas práticas de ETL, incluindo:
      * Verificação e ajuste de schemas.
      * Tipagem de colunas para otimização e correção de formato (ex: `StringType`, `TimestampType`, `FloatType`, `BooleanType`).
      * Persistência dos dados processados na camada **Bronze** do Data Lake no formato Delta Lake, utilizando `mode("overwrite")` para este caso ou `mode("append")` / `mergeInto` para cargas incrementais futuras.
4.  **Análise de Dados:**
      * **Definição de KPIs:** Taxa de conversão, ticket médio por usuário, ticket médio por pedido e frequência de pedidos.
      * **Comparação A/B:** Análise comparativa entre grupo de controle e grupo de teste, com foco em significância estatística.
      * **Viabilidade Financeira:** Cálculo do ROI baseado em premissas de custo de cupom e margem de contribuição.
      * **Segmentação:** Criação de novos segmentos de usuários (ex: "Sudeste Gourmet", "Veteranos Gourmet", "Food Fiéis Sudeste") baseados em características como região de entrega, faixa de preço de restaurantes e tempo de cadastro.
      * **Análise Segmentada:** Avaliação do impacto do teste A/B dentro de cada segmento.

## Estrutura do Repositório

  * `1_Case_iFood_Data_Ingestion_AB_Test.ipynb`: Notebook Python/PySpark para a ingestão, tratamento e preparação inicial dos dados.
  * `2_Case_iFood_Data_Analysis_AB_Test.ipynb`: Notebook Python/PySpark contendo a análise completa do teste A/B, cálculos de KPIs, análise de viabilidade financeira e exploração de segmentações.
  * `Case_iFood.pdf`: Apresentação em formato PDF com os resultados, insights e recomendações para as lideranças de negócio.

## Como Executar o Projeto

Para replicar o projeto, é recomendado um ambiente com suporte a PySpark, como Databricks Community Edition, Google Colab ou um ambiente local configurado.

**Pré-requisitos:**

  * Python 3.x
  * Apache Spark (para ambiente local, PySpark deve estar instalado)
  * Bibliotecas Python: `pyspark`, `requests`, `tarfile`, `os`, `urllib.request`

**Passos para Execução:**

1.  **Clone o Repositório:**
    ```bash
    git clone https://github.com/kenjoao/case_ifood.git
    cd case_ifood
    ```
2.  **Configuração do Ambiente Spark:**
      * **Databricks/Google Colab:** Carregue os notebooks diretamente na plataforma e configure o cluster/ambiente Spark.
      * **Ambiente Local:** Certifique-se de que sua sessão Spark está configurada corretamente para acessar arquivos S3 (pode exigir configurações de credenciais AWS, ou adaptar os caminhos dos arquivos para um diretório local após download manual).
3.  **Executar o Notebook de Ingestão (`1_Case_iFood_Data_Ingestion_AB_Test.ipynb`):**
      * Abra o notebook em seu ambiente PySpark.
      * Execute todas as células sequencialmente. Este notebook fará o download, extração e carregamento dos dados para a camada `ifood_bronze` em formato Delta Lake.
4.  **Executar o Notebook de Análise (`2_Case_iFood_Data_Analysis_AB_Test.ipynb`):**
      * Abra este notebook APÓS a conclusão do notebook de ingestão.
      * Execute todas as células sequencialmente para realizar as análises, gerar os resultados dos KPIs e as segmentações.

## Principais Resultados e Recomendações

### Impacto Geral da Campanha:

  * A **taxa de conversão** não foi um indicador impactante, pois a maioria dos usuários em ambos os grupos realizou pedidos.
  * O público abordado pela campanha teve um **ticket médio 12,8% maior** que o grupo controle, superando a margem de erro do experimento (0,16 p.p.).
  * A **frequência de pedidos por cliente** no grupo de teste foi **13,2% maior** que no grupo controle.
  * O **ticket por pedido** foi minimamente afetado, com o grupo controle sendo 0,4% maior, não sendo um fator impactante.

### Viabilidade Financeira:

  * Apesar dos aumentos em ticket médio e frequência, o **ROI inicial foi negativo (-21,8%)** com um cupom de R$ 10,00.
  * No entanto, o aumento na frequência e no ticket por cliente indica **potencial de retorno positivo a médio prazo** se o engajamento se mantiver.

### Análise por Segmento:

| Segmento | Diferença no Ticket Médio | Significância (Ticket) | Diferença na Frequência de Pedidos | Significância (Frequência) |
| :------- | :------------------------ | :--------------------- | :--------------------------------- | :------------------------- |
| **Sudeste Gourmet** | **+9,7%** (R$ 266,07) | ✅ Alta | **+4,4%** (+0,0859) | ✅ Significativa |
| **Veteranos Gourmet** | **+2,6%** (R$ 6,51) | ⚠️ Limítrofe | **+2,7%** (+0,0579) | ⚠️ Baixa |
| **Food Fiéis Sudeste** | **+11,6%** (R$ 30,38) | ✅ Alta | **+11,9%** (+0,4051) | ✅ Significativa |

### Próximos Passos Sugeridos:

1.  **Escalar Campanha para "Sudeste Gourmet":**
      * **Justificativa:** Maior ticket médio absoluto (R$ 3.006,51) e alta frequência, com potencial de R$ 26,6 milhões em receita incremental bruta a cada 100 mil clientes com investimento de R$ 1 milhão, garantindo um ROI altamente positivo.
      * **Ação:** Expandir para perfis semelhantes nacionalmente, testando otimizações de cupom.
2.  **Reforçar Campanhas com "Food Fiéis Sudeste":**
      * **Justificativa:** Grande base de clientes (158 mil) com ticket médio (+11,6%) e frequência (+11,9%) significativamente maiores que o grupo controle.
      * **Ação:** Desenvolver jornadas de CRM personalizadas (cupons sazonais, gamificação, badges).
3.  **Aprofundar Teste em "Veteranos Gourmet":**
      * **Justificativa:** Bom perfil, mas resposta inicial a cupons foi marginal.
      * **Ação:** Testar novas abordagens (comunicação emocional, incentivos não-financeiros como prioridade de entrega/funcionalidades) para identificar estímulos mais eficazes.
4.  **Melhorias Contínuas em Processos de Dados:**
      * **Ação:** Padronizar critérios de segmentação, criar repositório de segmentos validados e automatizar testes A/B com pipelines Delta + alertas para agilidade e escalabilidade.

## Apresentação dos Resultados

Uma apresentação detalhada dos resultados e recomendações, destinada a líderes de negócio, está disponível em `Case_iFood.pdf` neste repositório.

-----

**Autor:** Kleyton Kenji

**Data:** Junho de 2025

-----
