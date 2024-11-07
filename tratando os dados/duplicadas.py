import pandas as pd

# Importar CSV e criar DataFrame
df = pd.read_csv("output/df_marco_filtrado.csv")

# total_linhas = len(df)
# print(f"Total de linhas antes : {total_linhas}")

# Identificar e remover as linhas duplicadas (mantendo apenas a primeira ocorrência)
df_sem_duplicatas = df.drop_duplicates()

# Ordenar o DataFrame pela coluna "Data de Utilizacao"
df_ordenado = df_sem_duplicatas.sort_values(by="Data de Utilizacao")

# Exibir o DataFrame ordenado sem duplicatas
print(df_ordenado)

# total_linhas = len(df_sem_duplicatas)
# print(f"Total de linhas depois: {total_linhas}")

# # Caso deseje salvar o DataFrame final em um novo arquivo CSV
# df_ordenado.to_csv("output/df_marco_filtrado_limpo.csv", index=False)
