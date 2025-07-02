import duckdb

# Conectando ao DuckDB
con = duckdb.connect()

print("Consulta 1: Top 10 parlamentares que mais gastaram")
q1 = con.execute("""
    SELECT * 
    FROM 'data/gold/gastos_por_parlamentar.parquet'
    ORDER BY Total_Gasto DESC
    LIMIT 10
""").df()
print(q1)

print("\nConsulta 2: Gastos por partido no ano de 2020")
q2 = con.execute("""
    SELECT Partido, SUM(Total_Gasto) AS Gasto_Total
    FROM 'data/gold/gastos_categoria_partido.parquet'
    WHERE Ano = 2020
    GROUP BY Partido
    ORDER BY Gasto_Total DESC
""").df()
print(q2)

print("\nConsulta 3: Gastos por categoria no ano de 2019")
q3 = con.execute("""
    SELECT Categoria, SUM(Total_Gasto) AS Gasto_Total
    FROM 'data/gold/gastos_categoria_partido.parquet'
    WHERE Ano = 2019
    GROUP BY Categoria
    ORDER BY Gasto_Total DESC
""").df()
print(q3)
