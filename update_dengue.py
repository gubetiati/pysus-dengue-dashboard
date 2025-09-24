from pysus import SINAN
import pandas as pd
import datetime

# O nome do arquivo a ser gerado
ano_atual = datetime.date.today().year
ARQUIVO_FINAL = f"dengue_pr_{ano_atual}.csv"

sinan = SINAN().load()

def baixar_ano(ano):
    print(f"Baixando ano {ano}...")
    arquivos = sinan.get_files(dis_code="DENG", year=ano)

    dfs_ano = []
    if isinstance(arquivos, list):
        for pq in arquivos:
            parquet = pq.download()
            df = parquet.to_dataframe()
            dfs_ano.append(df[df["SG_UF_NOT"] == "41"])
    else:
        df = arquivos.to_dataframe()
        dfs_ano.append(df[df["SG_UF_NOT"] == "41"])

    df_final_ano = pd.concat(dfs_ano, ignore_index=True)
    print(f"Ano {ano}: {len(df_final_ano)} registros encontrados para PR.")
    return df_final_ano

if __name__ == "__main__":
    try:
        ano_atual = datetime.date.today().year

        df_atual = baixar_ano(ano_atual)
        df_atual.to_csv(ARQUIVO_FINAL, index=False)
        print(f"✅ Base salva com {len(df_atual)} registros totais em '{ARQUIVO_FINAL}'.")
    except Exception as e:
        print(f"Erro na execução: {e}")
