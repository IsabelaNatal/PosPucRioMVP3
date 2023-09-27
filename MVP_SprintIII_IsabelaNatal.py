# Databricks notebook source
# MAGIC %md
# MAGIC # PUC Rio: Pós Graduação em Ciência de Dados e Analytics
# MAGIC MVP Sprint III: Engenharia de Dados (40530010057_20230_01)
# MAGIC
# MAGIC Aluna: Isabela Fernanda Natal Batista Abreu Gomes
# MAGIC
# MAGIC Setembro/2023

# COMMAND ----------

# MAGIC %md
# MAGIC # Introdução
# MAGIC Programa para obtenção, tratamento e análise do histórico de Energia Natural Afluente (ENA), da Energia Armazenada (EArm), da Demanda/Carga e do Custo Marginal da Operação (CMO) por subsistema, a partir da área de Dados Abertos do Operador Nacional do Sistema Elétrico (ONS): https://dados.ons.org.br/
# MAGIC > No Brasil, a projeção de preços da energia elétrica baseia-se na previsão, centralizada, de despacho das usinas pelo Operador Nacional do Sistema Elétrico (ONS).
# MAGIC
# MAGIC > Em linhas gerais, o preço da energia, também chamado de Preço de Liquidação das Diferenças (PLD) tem como base o Custo Marginal da Operação (CMO), dado pelo planejamento da operação hidrotérmica do Sistema Interligado Nacional. Os modelos utilizados para projeção oficial do despacho das usinas pelo ONS (Operador Nacional do Sistema) denominam-se NEWAVE, DECOMP e DESSEM, todos desenvolvidos e fornecidos pelo Centro de Pesquisas da Eletrobras (CEPEL), conforme ilustrado na Figura 1.
# MAGIC ![Resumo Modelos](https://sprintiiiisabelanatal.blob.core.windows.net/cont-sprintiii-isabelanatal/Resumo_Setor.jpg?sp=r&st=2023-09-25T01:18:45Z&se=2023-09-25T09:18:45Z&spr=https&sv=2022-11-02&sr=b&sig=HuQz3LlmyYBRJF0ENzado0MCHty3WszrFGS1uWp8dM4%3D)
# MAGIC
# MAGIC Figura 1 - Resumo do Problema de Planejamento Eletroenergético Brasileiro

# COMMAND ----------

# MAGIC %md
# MAGIC > Para a projeção do preço da energia elétrica, uma série de grandezas são utilizadas como entrada. Uma vez que a matriz eletroenergética brasileira é predominantemente hidráulica (e os modelos consideram este aspecto no equacionamento do problema de otimização), a diferença entre as vazões previstas e verificadas dos postos associados às usinas hidroelétricas responde por cerca de 50% das variações no PLD/CMO, conforme ilustrado na Figura 2, disponibilizada pela Câmara de Comercialização de Energia (CCEE). A Energia Armazenada e a Carga também desempenham papel relevante na influência do PLD/CMO. Juntas, as três grandezas respondem por mais de 70% das variações no custo marginal da operação sendo, portanto, as variáveis de interesse na primeira abordagem deste MVP.
# MAGIC ![Gráfico Pizza CCEE](https://sprintiiiisabelanatal.blob.core.windows.net/cont-sprintiii-isabelanatal/GraficoPizza_CCEE.jpg?sp=r&st=2023-09-25T01:13:50Z&se=2023-09-25T09:13:50Z&spr=https&sv=2022-11-02&sr=b&sig=ZHQJ9pjeMR2%2BozppYGE8ljzEiMkcnabvMymCTQPa2mQ%3D)
# MAGIC
# MAGIC Figura 2 - Variação do PLD/CMO por variável de entrada nos modelos de planejamento eletroenergético do SIN. Fonte: CCEE
# MAGIC >Neste contexto, considerando a atuação na área de Inteligência de Mercado de Gás e Energia em um dos principais agentes geradores do Brasil, faz sentido estruturarmos a coleta e tratamento dos dados referentes às grandezas mencionadas, visando à melhor tomada de decisão sob a ótica da operação do parque e da comercialização e logística do gás e da energia elétrica.

# COMMAND ----------

# MAGIC %md
# MAGIC # Seção I: Obtenção e Tratamento Inicial dos Dados
# MAGIC
# MAGIC
# MAGIC 1.   [Energia Natural Afluente (ENA) por Subsistema](https://dados.ons.org.br/dataset/ena-diario-por-subsistema)
# MAGIC 2.   [Energia Armazenada (EArm) por Subsistema](https://dados.ons.org.br/dataset/ear-diario-por-subsistema)
# MAGIC 3.   [Demanda por Subsistema](https://dados.ons.org.br/dataset/carga-energia)
# MAGIC 4.   [Custo Marginal da Operação (CMO) Semanal por Subsistema](https://dados.ons.org.br/dataset/cmo-semanal)

# COMMAND ----------

# MAGIC %md
# MAGIC Diariamente, o Operador Nacional do Sistema Elétrico disponibiliza os dados verificados das grandezas supracitadas, para cada um dos 4 subsistemas (Sudeste, Sul, Nordeste e Norte) que compõem eletroenergeticamente o Sistema Interligado Nacional. Para atender ao presente trabalho, estes dados serão tratados, manipulados, pré-processados e avaliados.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importação de Biblioteas
# MAGIC Inicialmente, iremos importar e avaliar (utilizando a biblioteca Pandas, para fazer uma rápida análise exploratória) os dataset de "entrada", variáveis explicativas para a obtenção do CMO: ENA, EArm e Demanda, nesta ordem.

# COMMAND ----------

# Primeiro bloco: Importação das bibliotecas e módulos
# Configuração para não exibir os warnings
import warnings
warnings.filterwarnings("ignore")
#
# Importando as bibliotecas pandas, matplotlib (pyplot), seaborn e datetime
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import datetime
import numpy as np

# COMMAND ----------

# MAGIC %md
# MAGIC ## Energia Natural Afluente: ENA
# MAGIC
# MAGIC ***ENA DIÁRIO POR SUBSISTEMA*** (Segundo o ONS)
# MAGIC
# MAGIC Dados das grandezas de energia natural afluente (ENA) dos reservatórios com periodicidade diária por Subsistemas.
# MAGIC
# MAGIC A Energia Natural Afluente (ENA) Bruta representa a energia produzível pela usina e é calculada pelo produto das vazões naturais aos reservatórios com as produtividades a 65% dos volumes úteis. A ENA Armazenável considera as vazões naturais descontadas das vazões vertidas nos reservatórios.
# MAGIC
# MAGIC Como esses dados podem ser utilizados: os dados podem servir de insumo para estudos energéticos e projeção do custo marginal de operação. Contudo, saiba que os dados disponibilizados fazem parte de um processo de consistência recorrente e, portanto, podem ser atualizados após a sua publicação.

# COMMAND ----------

# Seção I.1: Programa para obtenção das Energias Naturais Afluentes - Dados Abertos Operador Nacional do Sistema Elétrico
# Dados por subsistema do Sistema Interligado Nacional, atualizados diariamente.
# ENA - Arquivos com os dados anuais, com histórico desde 2001
ano_zero=2001
ano_inicio=ano_zero+1
data_fim=datetime.datetime.now() - datetime.timedelta(days=5) #ano vigente, considerando um possível atraso de até 5 dias na publicação, o que é comum na virada de ano
ano_fim=data_fim.strftime("%Y")
ano_fim=int(ano_fim)
print(data_fim)
print(ano_fim)

# COMMAND ----------

#Importando os dados da url para o dataframe, começando pelo ano inicial do histórico, até o ano vigente
#Conforme dicionário de dados disponibilizado na página de Arquitetura Aberta do ONS, os arquivos .csv estão no formato UTF-8, com delimitador do tipo ponto-e-vírgula
ena=pd.read_csv("https://ons-dl-prod-opendata.s3.amazonaws.com/dataset/ena_subsistema_di/ENA_DIARIO_SUBSISTEMA_"+str(ano_zero)+".csv",delimiter=";",encoding = 'utf8')
ena=pd.DataFrame(ena)
for ano in range (ano_inicio,ano_fim+1):
    ano_str=str(ano)
    ena_ano=pd.read_csv("https://ons-dl-prod-opendata.s3.amazonaws.com/dataset/ena_subsistema_di/ENA_DIARIO_SUBSISTEMA_"+ano_str+".csv",delimiter=";",encoding = 'utf8')
    ena_ano=pd.DataFrame(ena_ano)
    ena=pd.concat([ena,ena_ano])
print(ena)

# COMMAND ----------

# Conferindo as primeiras 10 linhas, para check da importação de dados
ena.head(10)

# COMMAND ----------

# Conferindo a quantidade de dias (considerando 4 subsistemas)
len(ena)/4

# COMMAND ----------

# Verificando os tipos de coluna do dataframe ena
ena.dtypes

# COMMAND ----------

# Iniciando o tratamento dos dados
#Convertendo os nomes dos subsistemas para 1ª letra em maiúscula: SE->Sudeste; S->Sul; NE->Nordeste; N->Norte
ena['nom_subsistema']=ena['nom_subsistema'].replace({'SUDESTE':'Sudeste',
                                                'SUL':'Sul',
                                                'NORDESTE':'Nordeste',
                                                'NORTE':'Norte'})

# COMMAND ----------

# Renomeando a coluna de data e de subsistema, de modo a serem os mesmos nomes em todos os DataFrames/consultas, para facilitar a chave de mesclagem
ena=ena.rename(columns = {'ena_data':'Data'})
ena=ena.rename(columns = {'nom_subsistema':'Subsistema'})
print(ena)

# COMMAND ----------

# Alterando o tipo da coluna "Data"
ena['Data'] = pd.to_datetime(ena['Data'])

# COMMAND ----------

# Reordenando o dataframe ena, pela coluna Data (em ordem decrescente)
ena=ena.sort_values(by='Data', ascending=False)
ena.head(20)

# COMMAND ----------

# Verificando os tipos de coluna do dataframe ena
ena.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Energia Armazenada: EAR
# MAGIC
# MAGIC ***EAR DIÁRIO POR SUBSISTEMA*** (Segundo o ONS)
# MAGIC
# MAGIC Dados das grandezas de energia armazenada (EAR) em periodicidade diária por Subsistemas.
# MAGIC
# MAGIC A Energia Armazenada (EAR) representa a energia associada ao volume de água disponível nos reservatórios que pode ser convertido em geração na própria usina e em todas as usinas à jusante na cascata. A grandeza de EAR leva em conta nível verificado nos reservatórios na data de referência. A grandeza de EAR máxima representa a capacidade de armazenamento caso todos os reservatórios do sistema estivessem cheios. A grandeza de EAR para o subsistema à jusante considera a utilização da água do reservatório para produzir energia em uma usina à jusante que está em um subsistema diferente.
# MAGIC
# MAGIC Como esses dados podem ser utilizados: os dados podem servir de insumo para estudos energéticos e projeção do custo marginal de operação. Contudo, saiba que os dados disponibilizados fazem parte de um processo de consistência recorrente e, portanto, podem ser atualizados após a sua publicação.

# COMMAND ----------

# Seção I.2: Programa para obtenção das Energias Armazenadas - Dados Abertos Operador Nacional do Sistema Elétrico
# Dados por subsistema do Sistema Interligado Nacional, atualizados diariamente.

# EArm - Arquivos com os dados anuais, com histórico desde 2001.
# Portanto, serão utilizados os mesmos parâmetros temporais base (que das demais grandezas)
# Importando os dados da url para o dataframe, começando pelo ano inicial do histórico, até o ano vigente
# Conforme dicionário de dados disponibilizado na página de Arquitetura Aberta do ONS, os arquivos .csv estão no formato UTF-8, com delimitador do tipo ponto-e-vírgula
earm=pd.read_csv("https://ons-dl-prod-opendata.s3.amazonaws.com/dataset/ear_subsistema_di/EAR_DIARIO_SUBSISTEMA_"+str(ano_zero)+".csv",delimiter=";",encoding = 'utf8')
earm=pd.DataFrame(earm)
for ano in range (ano_inicio,ano_fim+1):
    ano_str=str(ano)
    earm_ano=pd.read_csv("https://ons-dl-prod-opendata.s3.amazonaws.com/dataset/ear_subsistema_di/EAR_DIARIO_SUBSISTEMA_"+ano_str+".csv",delimiter=";",encoding = 'utf8')
    earm_ano=pd.DataFrame(earm_ano)
    earm=pd.concat([earm,earm_ano])
print (earm)

# COMMAND ----------

# Conferindo as primeiras 10 linhas, para check da importação de dados
earm.head(10)

# COMMAND ----------

# Conferindo a quantidade de dias (considerando 4 subsistemas)
len(earm)/4

# COMMAND ----------

# Verificando os tipos de coluna do dataframe ena
earm.dtypes

# COMMAND ----------

# Iniciando o tratamento dos dados
#Convertendo os nomes dos subsistemas para 1ª letra em maiúscula: SE->Sudeste; S->Sul; NE->Nordeste; N->Norte
earm['nom_subsistema']=earm['nom_subsistema'].replace({'SUDESTE':'Sudeste',
                                                'SUL':'Sul',
                                                'NORDESTE':'Nordeste',
                                                'NORTE':'Norte'})

# COMMAND ----------

# Renomeando a coluna de data e de subsistema, de modo a serem os mesmos nomes em todos os DataFrames/consultas, para facilitar a chave de mesclagem
earm=earm.rename(columns = {'ear_data':'Data'})
earm=earm.rename(columns = {'nom_subsistema':'Subsistema'})
print(earm)

# COMMAND ----------

# Alterando o tipo da coluna "Data"
earm['Data'] = pd.to_datetime(earm['Data'])

# COMMAND ----------

# Reordenando o dataframe earm, pela coluna Data (em ordem decrescente)
earm=earm.sort_values(by='Data', ascending=False)
earm.head(20)

# COMMAND ----------

# Verificando os tipos de coluna do dataframe earm
earm.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga (Demanda) Eletroenergética
# MAGIC
# MAGIC ***CARGA DE ENERGIA DIÁRIA*** (Segundo o ONS)
# MAGIC
# MAGIC Dados de carga por subsistema em base diária, medida em MWmed.
# MAGIC
# MAGIC Até fevereiro/2021, os dados representam a carga atendida por usinas despachadas e/ou programadas pelo ONS, com base em dados recebidos pelo Sistema de Supervisão e Controle do ONS. Entre março/2021 e abril/23, os dados representam a carga atendida por usinas despachadas e/ou programadas pelo ONS, com base em dados recebidos pelo Sistema de Supervisão e Controle do ONS, mais a previsão de geração de usinas não despachadas pelo ONS. A partir de 29/04/2023, além dos dados anteriormente considerados, passou a ser incorporado o valor estimado da micro e minigeração distribuída (MMGD), com base em dados meteorológicos previstos.

# COMMAND ----------

# Seção I.3: Programa para obtenção da Carga Elétrica - Dados Abertos Operador Nacional do Sistema Elétrico
# Dados por subsistema do Sistema Interligado Nacional, atualizados diariamente.

# Carga - Arquivos com os dados anuais, com histórico desde 2001.
# Portanto, serão utilizados os mesmos parâmetros temporais base (que das demais grandezas)
# Importando os dados da url para o dataframe, começando pelo ano inicial do histórico, até o ano vigente
# Conforme dicionário de dados disponibilizado na página de Arquitetura Aberta do ONS, os arquivos .csv estão no formato UTF-8, com delimitador do tipo ponto-e-vírgula
carga=pd.read_csv("https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/carga_energia_di/CARGA_ENERGIA_"+str(ano_zero)+".csv",delimiter=";",encoding = 'utf8')
carga=pd.DataFrame(carga)

# COMMAND ----------

# No dia da elaboração deste trabalho, a sintaxe da URL para o ano de 2023 (na nuvem) estava diferente dos demais anos:
for ano in range (ano_inicio,ano_fim+1):
    ano_str=str(ano)
    if ano<ano_fim:
        carga_ano=pd.read_csv("https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/carga_energia_di/CARGA_ENERGIA_"+ano_str+".csv",delimiter=";",encoding = 'utf8')
    else:
        carga_ano=pd.read_csv(" https://ons-dl-prod-opendata.s3.amazonaws.com/dataset/carga_energia_di/CARGA_ENERGIA_"+ano_str+".csv",delimiter=";",encoding = 'utf8')        
    carga_ano=pd.DataFrame(carga_ano)
    carga=pd.concat([carga,carga_ano])
print(carga)

# COMMAND ----------

# Conferindo as primeiras 10 linhas, para check da importação de dados
carga.head(10)

# COMMAND ----------

# Conferindo a quantidade de dias (considerando 4 subsistemas)
len(carga)/4

# COMMAND ----------

# Tratando valores de carga nulos
carga_null=(carga['val_cargaenergiamwmed'].isnull() == True)
carganull=carga.loc[carga_null]
print(carganull)

# COMMAND ----------

# MAGIC %md
# MAGIC >Constata-se, pela quantidade de dias, que o dataframe "carga" tem uma linha a mais que os dataframes "ena" e "earm". Ao mesclarmos as consultas, desejaremos excluir essa linha a mais.

# COMMAND ----------

# Verificando os tipos de coluna do dataframe carga
carga.dtypes

# COMMAND ----------

# Iniciando o tratamento dos dados
# Convertendo os nomes dos subsistemas para 1ª letra em maiúscula: SE->Sudeste; S->Sul; NE->Nordeste; N->Norte
carga['nom_subsistema']=carga['nom_subsistema'].replace({'Sudeste/Centro-Oeste':'Sudeste',
                                                'SUL':'Sul',
                                                'NORDESTE':'Nordeste',
                                                'NORTE':'Norte'})

# COMMAND ----------

# Renomeando a coluna de data, de modo a ser o mesmo nome em todos os DataFrames, para facilitar a chave de mesclagem
carga=carga.rename(columns = {'din_instante':'Data'})
carga=carga.rename(columns = {'nom_subsistema':'Subsistema'})
print(carga)

# COMMAND ----------

# Alterando o tipo da coluna "Data"
carga['Data'] = pd.to_datetime(carga['Data'])
carga = carga.dropna(subset=["val_cargaenergiamwmed"])

# COMMAND ----------

# Reordenando o dataframe carga, pela coluna Data (em ordem decrescente)
carga=carga.sort_values(by='Data', ascending=False)
carga.head(20)

# COMMAND ----------

# Verificando os tipos de coluna do dataframe carga
carga.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC # Seção II: Spark e Hive
# MAGIC > O Apache Spark é um mecanismo de análise unificada para código aberto em computação distribuída. Será utilizado no presente trabalho para o processamento de dados em grande escala, com módulos integrados para SQL e Python.

# COMMAND ----------

# Criando os DataFrames relacionados à ENA, EAR e Carga, a partir dos DataFrames Pandas ena, earm e carga
spark_ena = spark.createDataFrame(ena)
spark_earm = spark.createDataFrame(earm)
spark_carga = spark.createDataFrame(carga)

# COMMAND ----------

# Criando uma visualização temporária para cada query: DW_ENA; DW_EARM; DW_CARGA
spark_ena.createOrReplaceTempView("DW_ENA")
spark_earm.createOrReplaceTempView("DW_EARM")
spark_carga.createOrReplaceTempView("DW_CARGA")

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM hive_metastore.sprintiii_isabelanatal.DWTABLE_ENA

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM hive_metastore.sprintiii_isabelanatal.DWTABLE_EARM

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM hive_metastore.sprintiii_isabelanatal.DWTABLE_CARGA

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS hive_metastore.sprintiii_isabelanatal.DWTABLE_ENA

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS hive_metastore.sprintiii_isabelanatal.DWTABLE_EARM

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS hive_metastore.sprintiii_isabelanatal.DWTABLE_CARGA

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS hive_metastore.sprintiii_isabelanatal

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS hive_metastore.sprintiii_isabelanatal

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE hive_metastore.sprintiii_isabelanatal.DWTABLE_ENA
# MAGIC (
# MAGIC  id_subsistema STRING COMMENT 'Código do Subsistema - Valores possíveis: NE, N, SE, S',
# MAGIC  Subsistema STRING COMMENT 'Nome do Subsistema - Valores possíveis: Nordeste, Norte, Sudeste, Sul',
# MAGIC  Data DATE COMMENT 'Data da medida observada - Valores a partir de 2001, até a data presente (até dois dias antes)',
# MAGIC  ena_bruta_regiao_mwmed DOUBLE COMMENT 'Valor de Energia Natural Afluente bruta por Subsistema na unidade de medida MWmed. Representa a energia produzível pelas usinas hidroelétricas que compõem cada subsistema e é calculada pelo produto das vazões naturais aos reservatórios com as produtividades a 65% dos volumes úteis. Apenas valores positivos ou nulos.',
# MAGIC  ena_bruta_regiao_percentualmlt DOUBLE COMMENT 'Valor de Energia Natural Afluente bruta por Subsistema na unidade de medida em percentual da Média de Longo Termo (MLT), a qual é computada a partir do histórico desde 1931. Apenas valores positivos ou nulos.',
# MAGIC  ena_armazenavel_regiao_mwmed DOUBLE COMMENT 'Valor de Energia Natural Afluente Armazenável por Subsistema na unidade de medida MWmed. Seu cômputo considera as vazões naturais, descontadas das vazões vertidas nos reservatórios das hidroelétricas que compõem cada subsistema. Apenas valores positivos ou nulos.',
# MAGIC  ena_armazenavel_regiao_percentualmlt DOUBLE COMMENT 'Valor de Energia Natural Afluente Armazenável por Subsistema na unidade de medida em percentual da Média de Longo Termo (MLT), a qual é computada a partir do histórico desde 1931. Apenas valores positivos ou nulos.'
# MAGIC ) COMMENT 'Dados das grandezas de energia natural afluente (ENA) dos reservatórios com periodicidade diária por Bacias Hidro Energéticas.
# MAGIC
# MAGIC Conforme o Operador Nacional do Sistema Elétrico, esses dados podem servir de insumo para estudos energéticos e projeção do custo marginal de operação. Contudo, é necessário ter conhecimento de que os dados disponibilizados fazem parte de um processo de consistência recorrente e, portanto, podem ser atualizados após a sua publicação.'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE TABLE hive_metastore.sprintiii_isabelanatal.DWTABLE_ENA
# MAGIC SELECT * FROM DW_ENA

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE hive_metastore.sprintiii_isabelanatal.DWTABLE_EARM
# MAGIC (
# MAGIC  id_subsistema STRING COMMENT 'Código do Subsistema - Valores possíveis: NE, N, SE, S',
# MAGIC  Subsistema STRING COMMENT 'Nome do Subsistema - Valores possíveis: Nordeste, Norte, Sudeste, Sul',
# MAGIC  Data DATE COMMENT 'Data da medida observada - Valores a partir de 2001, até a data presente (até dois dias antes)',
# MAGIC  ear_max_subsistema DOUBLE COMMENT 'Valor de Energia armazenada máxima nos reservatórios das hidroelétricas por subsistema na unidade de medida MWmês. Apenas valores positivos.',
# MAGIC  ear_verif_subsistema_mwmes DOUBLE COMMENT 'Valor de Energia Armazenada verificada no dia nos reservatórios das hidroelétricas por subsistema na unidade de medida MWmês.Apenas valores positivos.',
# MAGIC  ear_verif_subsistema_percentual DOUBLE COMMENT 'Valor de Energia Armazenada verificada no dia por subsistema na unidade de medida % do Volume útil armazenável (ear_max_subsistema) do próprio subsistema. Apenas valores positivos.'
# MAGIC ) COMMENT 'Dados das grandezas de energia armazenada (EAR) em periodicidade diária por Subsistemas.
# MAGIC
# MAGIC A Energia Armazenada (EAR) representa a energia associada ao volume de água disponível nos reservatórios que pode ser convertido em geração na própria usina e em todas as usinas à jusante na cascata. A grandeza de EAR leva em conta nível verificado nos reservatórios na data de referência. A grandeza de EAR máxima representa a capacidade de armazenamento caso todos os reservatórios do sistema estivessem cheios. A grandeza de EAR para o subsistema à jusante considera a utilização da água do reservatório para produzir energia em uma usina à jusante que está em um subsistema diferente.
# MAGIC
# MAGIC Os dados podem servir de insumo para estudos energéticos e projeção do custo marginal de operação. Contudo, é necessário saber que os dados disponibilizados fazem parte de um processo de consistência recorrente e, portanto, podem ser atualizados após a sua publicação.'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE TABLE hive_metastore.sprintiii_isabelanatal.DWTABLE_EARM
# MAGIC SELECT * FROM DW_EARM

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE hive_metastore.sprintiii_isabelanatal.DWTABLE_CARGA
# MAGIC (
# MAGIC  id_subsistema STRING COMMENT 'Código do Subsistema - Valores possíveis: NE, N, SE, S',
# MAGIC  Subsistema STRING COMMENT 'Nome do Subsistema - Valores possíveis: Nordeste, Norte, Sudeste, Sul',
# MAGIC  Data DATE COMMENT 'Data da medida observada - Valores a partir de 2001, até a data presente (até dois dias antes)',
# MAGIC  val_cargaenergiamwmed  DOUBLE COMMENT 'Valor da demanda de energia elétrica, por subsistema, na média diária. Valores medidos em MWmed. Apenas valores positivos.'
# MAGIC ) COMMENT 'Dados de carga por subsistema em base diária, medida em MWmed. Até fevereiro/2021, os dados representam a carga atendida por usinas despachadas e/ou programadas pelo ONS, com base em dados recebidos pelo Sistema de Supervisão e Controle do ONS. Entre março/2021 e abril/23, os dados representam a carga atendida por usinas despachadas e/ou programadas pelo ONS, com base em dados recebidos pelo Sistema de Supervisão e Controle do ONS, mais a previsão de geração de usinas não despachadas pelo ONS. A partir de 29/04/2023, além dos dados anteriormente considerados, passou a ser incorporado o valor estimado da micro e minigeração distribuída (MMGD), com base em dados meteorológicos previstos.
# MAGIC
# MAGIC Os dados disponibilizados fazem parte de um processo de consistência recorrente e, portanto, podem ser atualizados após a sua publicação.'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE TABLE hive_metastore.sprintiii_isabelanatal.DWTABLE_CARGA
# MAGIC SELECT * FROM DW_CARGA

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.sprintiii_isabelanatal.DWTABLE_ENA

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.sprintiii_isabelanatal.DWTABLE_EARM

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.sprintiii_isabelanatal.DWTABLE_CARGA
