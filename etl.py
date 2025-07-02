import pandas as pd
import unicodedata

# ========================
# CAMADA BRONZE
# ========================

print("Iniciando camada Bronze...")

df_raw = pd.read_csv('cota-parlamentar.csv', encoding='utf-8', sep=',', low_memory=False)

print("Colunas originais (após leitura do Parquet):")
print(df_raw.columns.tolist())

# Salvar em Parquet
df_raw.to_parquet("data/bronze/gastos_deputados_raw.parquet", index=False)
print("Camada Bronze criada")

# ========================
# CAMADA SILVER
# ========================

print("Iniciando camada Silver...")

df = pd.read_parquet("data/bronze/gastos_deputados_raw.parquet")

print("\n🔎 Colunas originais lidas:")
print(df.columns.tolist())

# Renomear direto do nome REAL para o nome padronizado que usaremos
df = df.rename(columns={
    "txnomeparlamentar": "txNomeParlamentar",
    "sgpartido": "sgPartido",
    "sguf": "sgUF",
    "txtdescricao": "txtDescricao",
    "txtfornecedor": "txtFornecedor",
    "vlrdocumento": "vlrDocumento",
    "numano": "numAno",
    "nummes": "numMes"
})

# Selecionar apenas colunas relevantes
colunas_uteis = [
    "txNomeParlamentar", "sgPartido", "sgUF", "txtDescricao", "txtFornecedor", 
    "vlrDocumento", "numAno", "numMes"
]
df = df[colunas_uteis]

# Limpeza e normalização
df['txNomeParlamentar'] = df['txNomeParlamentar'].str.strip().str.title()
df['txtFornecedor'] = df['txtFornecedor'].str.strip().str.title()
df['txtDescricao'] = df['txtDescricao'].str.strip().str.title()
df['sgPartido'] = df['sgPartido'].str.strip().str.upper()

df = df.dropna(subset=["vlrDocumento"])
df['vlrDocumento'] = pd.to_numeric(df['vlrDocumento'], errors='coerce')
df = df.dropna(subset=['vlrDocumento'])

df.to_parquet("data/silver/gastos_deputados_tratado.parquet", index=False)
print("Camada Silver criada")


# ========================
# CAMADA GOLD
# ========================

print("Iniciando camada Gold...")

# Tabela 1: Gastos por parlamentar
df_parlamentar = df.groupby("txNomeParlamentar")["vlrDocumento"].sum().reset_index()
df_parlamentar.columns = ["Parlamentar", "Total_Gasto"]
df_parlamentar.to_parquet("data/gold/gastos_por_parlamentar.parquet", index=False)

# Tabela 2: Gastos por categoria, partido e ano
df_categoria_partido = df.groupby(["numAno", "txtDescricao", "sgPartido"])["vlrDocumento"].sum().reset_index()
df_categoria_partido.columns = ["Ano", "Categoria", "Partido", "Total_Gasto"]
df_categoria_partido.to_parquet("data/gold/gastos_categoria_partido.parquet", index=False)

print("Camada Gold criada")