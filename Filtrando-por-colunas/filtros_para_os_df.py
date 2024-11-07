import os
import pandas as pd
import csv

# Lista de arquivos CSV para processar, dentro da pasta "dataset"
arquivos_csv = [
    'dataset/marco.csv', 'dataset/abril.csv', 'dataset/maio.csv',
    'dataset/junho.csv', 'dataset/julho.csv', 'dataset/agosto.csv', 'dataset/setembro.csv'
]

# Definindo as colunas que deseja visualizar
colunas_desejadas = ["Placa", "Data de Utilizacao", "Valor Cobrado", "Endereco do Estabelecimento", "Nome do Estabelecimento"]

# Removendo espaços adicionais das colunas escolhidas
colunas_desejadas = [col.strip() for col in colunas_desejadas]

# Criar uma pasta "output" para salvar os arquivos filtrados (se não existir)
output_dir = 'output'
os.makedirs(output_dir, exist_ok=True)

# 
# Função para ler, filtrar, remover duplicatas e ordenar colunas de um arquivo CSV
def processar_csv(nome_arquivo, colunas):
    df_filtrado = pd.DataFrame()  # Criar DataFrame vazio para armazenar os dados filtrados

    # Lendo o CSV em chunks e filtrando apenas as colunas escolhidas
    chunk_size = 1000  # Lê 1000 linhas por vez
    for chunk in pd.read_csv(nome_arquivo, sep=';', quoting=csv.QUOTE_NONE, on_bad_lines='skip', chunksize=chunk_size):
        try:
            # Filtrar colunas desejadas
            chunk_filtrado = chunk[colunas]
            
            # Remover espaços em branco extras de cada célula
            chunk_filtrado = chunk_filtrado.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
            
            # Concatenar o chunk ao DataFrame completo
            df_filtrado = pd.concat([df_filtrado, chunk_filtrado], ignore_index=True)
        except KeyError as e:
            print(f"Erro: {e}. Algumas das colunas não foram encontradas no chunk do arquivo {nome_arquivo}.")
    
    # 
    # Remover linhas duplicadas
    df_filtrado_sem_duplicatas = df_filtrado.drop_duplicates()

    # Ordenar o DataFrame pela coluna "Data de Utilizacao"
    df_ordenado = df_filtrado_sem_duplicatas.sort_values(by="Data de Utilizacao").reset_index(drop=True)

    return df_ordenado

# 
# Processar todos os arquivos e salvar os DataFrames filtrados, sem duplicatas e ordenados
for arquivo in arquivos_csv:
    df_final = processar_csv(arquivo, colunas_desejadas)
    
    # Gerar nome do arquivo de saída automaticamente e salvá-lo na pasta "output"
    nome_saida = f"{output_dir}/df_{arquivo.split('/')[-1].split('.')[0]}_filtrado.csv"
    df_final.to_csv(nome_saida, index=False)

    print(f"Arquivo {nome_saida} salvo com sucesso.")
