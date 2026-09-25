**Esse documento ainda será atualizado, para uma versão final com imagens**

# ATUALIZAÇÃO METODOLÓGICA — ANÁLISES SOBRE TABELAS DATABRICKS

Nesta versão, **a maior parte das análises é executada sobre tabelas Delta persistidas no Databricks**. Python e PySpark permanecem essenciais para ingestão, limpeza, qualidade, transformação e criação das camadas, mas os produtos usados na análise são materializados.

## Estrutura física

- Bronze: `countries_bronze`
- Silver: `countries_silver`
- Gold detalhada: `countries_environmental_analysis`
- Gold auxiliar: `analysis_by_region`
- Gold auxiliar: `analysis_by_hdi_band`
- Gold auxiliar: `analysis_by_gdp_band`
- Gold auxiliar: `analysis_correlations`
- Gold auxiliar: `analysis_outliers`

As análises finais utilizam principalmente Spark SQL sobre essas tabelas. Os gráficos devem, preferencialmente, ser criados no próprio Databricks a partir das saídas SQL. Python/Matplotlib fica como recurso complementar.

---

# MVP — Pipeline de Dados na Nuvem
## Relação entre desenvolvimento socioeconômico e pegada ecológica

## 1. Contexto de Negócio e Perguntas (Etapa 2 e 4.1)

Este MVP investiga a relação entre o **Índice de Desenvolvimento Humano (HDI)** e o **PIB per capita (GDP per Capita)** com quatro componentes da pegada ecológica: **Carbon Footprint, Fish Footprint, Cropland Footprint e Grazing Footprint**.

O objetivo não é demonstrar causalidade, mas identificar associações, padrões regionais e observações atípicas por meio de um pipeline reprodutível em nuvem.

### Perguntas
1. Qual é a relação entre HDI e Carbon Footprint?
2. Qual é a relação entre GDP per Capita e Carbon Footprint?
3. HDI e GDP per Capita apresentam relação com Fish Footprint?
4. Como HDI e GDP per Capita se relacionam com Cropland Footprint?
5. Como HDI e GDP per Capita se relacionam com Grazing Footprint?
6. As relações mudam por região?
7. Existem países que se comportam como outliers?
8. Pearson e Spearman conduzem às mesmas interpretações gerais?

### Dados brutos
O arquivo utilizado é `countries.csv`, com **188 registros e 21 colunas** na versão fornecida para desenvolvimento. Cada registro representa um país.

Principais atributos: Country, Region, Population (millions), HDI, GDP per Capita, Cropland Footprint, Grazing Footprint, Forest Footprint, Carbon Footprint, Fish Footprint, Total Ecological Footprint e atributos de biocapacidade.

### Fonte e licença

O conjunto de dados utilizado foi originalmente obtido na platoforma KAGGLE, pertencente ao GOOGLE e amplamente utilizada para utilização de datasets destinados a projetos de Ciência de Dados. O endereço original do arquivo é 2016 Global Eco https://www.kaggle.com/datasets/footprintnetwork/ecological-footprint?resource=download logical Footprint. Esse dataset é de propriedade da Global Footprint Network, e está disponível sob a licença CC BY-NC-SA 4.0 (Attribution – NonCommercial – ShareAlike 4.0 International).

**Link RAW do GitHub:** `https://raw.githubusercontent.com/tfiorio/pos-pucrio/refs/heads/sprint03/countries.csv`

---

## 2. Carga dos Dados (Etapa 4.2)

Plataforma: **Databricks Free Edition**.

O notebook baixa o CSV por meio do endereço RAW configurado em `DATA_SOURCE_URL`, lê o arquivo com Spark e o persiste em Delta.

### Se o RAW não funcionar
Faça upload manual do `countries.csv` no Databricks e substitua somente a etapa de ingestão. Mantenha todo o restante do pipeline.

Exemplo conceitual:
```python
SOURCE_PATH = "/Volumes/<catalog>/<schema>/<volume>/countries.csv"
df_raw = spark.read.option("header", True).option("inferSchema", True).csv(SOURCE_PATH)
```

### Pontos que precisam ser substituídos
- `DATA_SOURCE_URL`
- `GITHUB_REPOSITORY_URL`
- `CATALOG`, se `workspace` não estiver disponível
- `SCHEMA`, se desejar outro nome
- `SOURCE_PATH`, somente se optar por upload manual

**[SCREENSHOT 01]** arquivo/fonte disponível no Databricks ou execução da célula de ingestão.

---

## 3. Modelagem e Catálogo de Dados (Etapa 4.3)

Foi adotada arquitetura Medalhão.

### Bronze — `countries_bronze`
Objetivo: preservar o dado recebido e adicionar metadados técnicos.

Campos adicionais:
- `ingestion_timestamp`: timestamp da ingestão.
- `source_system`: origem lógica.
- `source_file`: arquivo recebido.

### Silver — `countries_silver`
Objetivo: produzir uma versão limpa, tipada e validada.

Principais transformações:
- nomes em snake_case;
- GDP per Capita convertido de texto monetário para `double`;
- tipos numéricos explicitados;
- remoção de duplicidades;
- regras de validade para HDI e footprints;
- flag `is_complete_for_analysis`;
- timestamp de processamento.

### Gold — `countries_environmental_analysis`
Modelo flat analítico, com um registro por país. O modelo flat foi escolhido porque as perguntas usam atributos de uma única entidade analítica (país) e não há fatos transacionais que justifiquem um Star Schema artificial.

### Catálogo resumido da Gold

| Campo | Tipo | Descrição | Linhagem |
|---|---|---|---|
| country | string | País | Fonte → Bronze → Silver → Gold |
| region | string | Região | Preservado |
| population_millions | double | População em milhões | Tipado na Silver |
| hdi | double | HDI, esperado entre 0 e 1 | Validado na Silver |
| gdp_per_capita | double | PIB per capita numérico | Remoção de `$` e vírgulas |
| carbon_footprint | double | Pegada de carbono | Validada como não negativa |
| fish_footprint | double | Pegada de pesca | Validada como não negativa |
| cropland_footprint | double | Pegada de cultivo | Validada como não negativa |
| grazing_footprint | double | Pegada de pastagem | Validada como não negativa |
| hdi_band | string | Faixa derivada de HDI | Criada na Gold |
| gdp_per_capita_band | string | Faixa de PIB per capita | Criada na Gold |
| data_quality | string | Indicador da fonte | Preservado |
| gold_processed_timestamp | timestamp | Momento de processamento | Gerado na Gold |

**[SCREENSHOT 02]** Unity Catalog mostrando Bronze.  
**[SCREENSHOT 03]** Unity Catalog mostrando Silver.  
**[SCREENSHOT 04]** Unity Catalog mostrando Gold e suas colunas.

---

## 4. Pipeline de Dados (Etapa 4.4)

Fluxo:

`GitHub RAW/CSV → Bronze → Silver → Gold → Análises`

O pipeline está concentrado em um notebook para facilitar a avaliação e a reprodução do MVP. Cada seção representa uma etapa lógica independente.

### Extract
Download/leitura do CSV.

### Transform
Padronização, tipagem, limpeza do PIB, validações, tratamento de duplicidades, avaliação de completude e criação de atributos derivados.

### Load
Persistência de Bronze, Silver e Gold como tabelas Delta.

**[SCREENSHOT 05]** célula confirmando persistência Bronze.  
**[SCREENSHOT 06]** célula confirmando persistência Silver.  
**[SCREENSHOT 07]** célula confirmando persistência Gold.

---

## 5. Qualidade de Dados (Etapa 4.5)

Na versão do CSV fornecida para desenvolvimento:
- 188 linhas;
- 21 colunas;
- nenhuma linha integralmente duplicada;
- nenhum país duplicado;
- HDI possui 16 valores ausentes;
- GDP per Capita possui 15 valores ausentes após conversão;
- Carbon, Fish, Cropland e Grazing Footprint possuem 15 valores ausentes cada.

A Silver **não elimina silenciosamente** registros incompletos: ela os preserva e sinaliza a completude. A Gold faz o recorte analítico, garantindo que correlações entre as seis variáveis centrais sejam calculadas sobre registros completos.

O GDP per Capita requer transformação porque aparece originalmente como texto, por exemplo `$4,534.37`.

Outliers são identificados pelo IQR, mas não removidos automaticamente. Um valor extremo pode representar um país real e ser importante para a interpretação.

**[SCREENSHOT 08]** contagem de nulos.  
**[SCREENSHOT 09]** regras de qualidade após Silver.  
**[SCREENSHOT 10]** saída da análise de outliers.

---

## 6. Análise de Dados (Etapa 4.5)

A análise utiliza estatísticas descritivas, Pearson, Spearman, dispersões, análise regional e outliers.

### Validação realizada sobre o CSV fornecido
Estes números servem para conferência da execução. Atualize-os se o arquivo final mudar.

| Relação | Pearson |
|---|---:|
| HDI × Carbon Footprint | 0.699 |
| GDP per Capita × Carbon Footprint | 0.824 |
| HDI × Fish Footprint | 0.210 |
| GDP per Capita × Fish Footprint | 0.152 |
| HDI × Cropland Footprint | 0.567 |
| GDP per Capita × Cropland Footprint | 0.506 |
| HDI × Grazing Footprint | 0.092 |
| GDP per Capita × Grazing Footprint | 0.105 |

### Interpretação
A relação mais evidente é observada na pegada de carbono. No CSV fornecido, GDP per Capita × Carbon Footprint apresenta Pearson de **0.824**, enquanto HDI × Carbon Footprint apresenta **0.699**. Isso indica associação positiva: países com maiores níveis desses indicadores tendem, nesta base, a apresentar maior componente de pegada de carbono.

Cropland Footprint apresenta associação positiva moderada com desenvolvimento, enquanto Fish Footprint e Grazing Footprint apresentam relações lineares bem mais fracas. Isso é importante porque mostra que os diferentes componentes ambientais não respondem de maneira uniforme aos indicadores socioeconômicos.

A comparação com Spearman deve ser observada no notebook. Diferenças relevantes entre Pearson e Spearman sugerem influência de assimetria, valores extremos ou relações monotônicas que não são estritamente lineares.

### Limitações
- análise transversal, sem dimensão temporal;
- correlação não implica causalidade;
- países podem diferir em matriz energética, clima, estrutura produtiva e comércio;
- valores ausentes reduzem a amostra analítica;
- a interpretação da unidade exata de cada footprint deve seguir a documentação original da fonte.

**[SCREENSHOT 11]** matriz de Pearson.  
**[SCREENSHOT 12]** matriz de Spearman.  
**[SCREENSHOT 13]** HDI × Carbon Footprint.  
**[SCREENSHOT 14]** GDP per Capita × Carbon Footprint.  
**[SCREENSHOT 15]** tabela regional.  
**[SCREENSHOT 16]** consulta SQL com países de maior Carbon Footprint.

---

## 7. Autoavaliação

O MVP atingiu o objetivo de construir um pipeline funcional de ponta a ponta, preservando os dados originais, criando uma camada limpa e uma camada analítica e utilizando essa estrutura para responder às perguntas propostas.

A principal transformação técnica foi a normalização do GDP per Capita, que estava representado como texto monetário. Também foi necessário tratar explicitamente a incompletude das variáveis centrais sem perder rastreabilidade.

A arquitetura Bronze/Silver/Gold tornou claras as responsabilidades de cada etapa. Para o escopo deste dataset, o modelo flat na Gold mostrou-se suficiente e evitou complexidade sem benefício analítico.

As análises permitiram observar que a associação entre desenvolvimento e pegada ecológica varia bastante conforme o componente analisado, sendo mais pronunciada para Carbon Footprint e mais fraca para Fish e Grazing Footprint.

Como evolução, o projeto poderia incorporar séries históricas, matriz energética, urbanização, participação industrial, comércio internacional e emissões territoriais/por consumo. O pipeline também poderia ser automatizado com Workflows e monitoramento formal de qualidade.

---

## 8. Arquivos do repositório

- `MVP_IDH_PIB_Pegada_Ecologica_Databricks.ipynb`
- `consultas_analiticas.sql`
- `README.md`
- opcionalmente `countries.csv` (o enunciado não exige disponibilizar os dados)

## 9. Repositório

`SUBSTITUIR_PELA_URL_DO_REPOSITORIO_GITHUB`

## 10. Checklist de entrega

- [ ] Executar todas as células sem erro.
- [ ] Substituir todos os textos `SUBSTITUIR_...`.
- [ ] Confirmar a licença na fonte original.
- [ ] Inserir os screenshots indicados.
- [ ] Conferir os números da seção de análise.
- [ ] Publicar o repositório como público.
- [ ] Conferir se notebook e README estão legíveis no GitHub.
