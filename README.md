# projeto-etl
Pipeline ETL que organiza, trata e analisa os dados de gastos da cota parlamentar entre os anos de 2009 e 2020 -> foi upado somente os scripts das consultas e do etl pois as camadas e o csv ultrapassam o tamanho que o github suporta
## Pipeline ETL

- **Camada Bronze:** Leitura do CSV original e conversão para Parquet.
- **Camada Silver:** Limpeza, padronização de colunas e tratamento de dados.
- **Camada Gold:** Agregações analíticas (gastos por parlamentar, por partido e por categoria).

## Consultas com DuckDB

Consultas SQL diretamente sobre os arquivos `.parquet`, como:

- Top 10 parlamentares que mais gastaram
- Gastos por partido em 2020
- Gastos por categoria em 2019

## ▶️ Vídeo explicativo

[🔗 Link para o vídeo explicando o funcionamento do projeto no YouTube](https://youtube.com)  
_(adicione aqui o link após subir o vídeo)_

---

## 🎓 Autor

Gabriel Paulino Alves  RA: 006668
Sistemas de Informação – Libertas Faculdades Integradas
