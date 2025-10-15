
import requests
import pandas as pd
from datetime import datetime
import time

def obter_municipios_parana():
    """
    Obtém lista completa de todos os 399 municípios do Paraná via API do IBGE
    
    Returns:
        dict: Dicionário com {nome_municipio: codigo_ibge}
    """
    print("Buscando lista completa dos municípios do Paraná via API IBGE...")
    
    try:
        # API oficial do IBGE para municípios do Paraná (código UF: 41)
        url = "https://servicodados.ibge.gov.br/api/v1/localidades/estados/41/municipios"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        municipios_data = response.json()
        municipios = {mun['nome']: mun['id'] for mun in municipios_data}
        
        print(f"✓ {len(municipios)} municípios encontrados via API IBGE")
        return municipios
        
    except Exception as e:
        print(f"✗ ERRO CRÍTICO ao buscar municípios via API IBGE: {e}")
        print("  O script não pode continuar sem a lista de municípios.")
        return None

def baixar_dados_infodengue(geocode, cidade, disease='dengue', anoInicial=2023, anoFinal=2025):
    """
    Baixa dados da API InfoDengue para uma cidade específica
    
    Args:
        geocode: Código IBGE da cidade
        cidade: Nome da cidade
        disease: Tipo de doença (dengue, chikungunya, zika)
        ano: Ano para consulta
    
    Returns:
        DataFrame com os dados ou None em caso de erro
    """
    url = "https://info.dengue.mat.br/api/alertcity"
    
    params = {
        'geocode': geocode,
        'disease': disease,
        'format': 'json',
        'ew_start': 1,
        'ew_end': 53,
        'ey_start': anoInicial,
        'ey_end': anoFinal
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if data:
            df = pd.DataFrame(data)
            df['cidade'] = cidade
            df['geocode'] = geocode
            
        if 'data_iniSE' in df.columns:
            df['data_iniSE'] = pd.to_datetime(df['data_iniSE'], unit='ms')
            
            print(f"✓ {cidade}: {len(df)} registros")
            return df
        else:
            print(f"○ {cidade}: Sem dados")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"✗ {cidade}: Erro - {e}")
        return None
    except Exception as e:
        print(f"✗ {cidade}: Erro inesperado - {e}")
        return None

def main():
    """
    Função principal que baixa dados de todas as cidades do Paraná
    """
    print("=" * 80)
    print(f"INFODENGUE - Download Completo - PARANÁ - Ano 2025")
    print("=" * 80)
    print(f"Data da execução: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("=" * 80)
    print()
    
    # Obtém lista completa dos municípios
    municipios = obter_municipios_parana()
    
    if municipios is None:
        print()
        print("✗ Não foi possível obter a lista de municípios. Encerrando...")
        return None
    
    print()
    print("=" * 80)
    print(f"TOTAL DE MUNICÍPIOS A PROCESSAR: {len(municipios)}")
    print("=" * 80)
    print()
    
    todos_dados = []
    sucessos = 0
    sem_dados = 0
    falhas = 0
    
    total = len(municipios)
    for idx, (cidade, geocode) in enumerate(municipios.items(), 1):
        print(f"[{idx}/{total}] ", end="")
        df = baixar_dados_infodengue(geocode, cidade)
        
        if df is not None and not df.empty:
            todos_dados.append(df)
            sucessos += 1
        elif df is not None and df.empty:
            sem_dados += 1
        else:
            falhas += 1
        
        # Pausa para não sobrecarregar a API
        time.sleep(0.3)
    
    print()
    print("=" * 80)
    print("RESUMO DO DOWNLOAD")
    print("=" * 80)
    print(f"Total de municípios processados: {total}")
    print(f"Cidades com dados baixados: {sucessos}")
    print(f"Cidades sem dados disponíveis: {sem_dados}")
    print(f"Cidades com erro na requisição: {falhas}")
    print("=" * 80)
    
    if todos_dados:
        # Concatena todos os DataFrames
        df_final = pd.concat(todos_dados, ignore_index=True)
        
        # Salva em CSV
        nome_arquivo = "infodengue.csv"
        df_final.to_csv(nome_arquivo, index=False, sep=",", encoding='utf-8')
        
        print()
        print(f"✓ Arquivo salvo: {nome_arquivo}")
        print(f"✓ Total de registros: {len(df_final)}")
        print(f"✓ Período: Semanas {df_final['SE'].min()} a {df_final['SE'].max()} de 2025")
        print(f"✓ Municípios únicos no dataset: {df_final['cidade'].nunique()}")
        print()
        print("COLUNAS DISPONÍVEIS NO ARQUIVO:")
        print(", ".join(df_final.columns.tolist()))
        
        return df_final
    else:
        print()
        print("✗ ATENÇÃO: Nenhum dado foi baixado com sucesso!")
        print("  Possíveis causas:")
        print("  - API InfoDengue pode estar fora do ar")
        print("  - Dados de 2025 ainda não disponíveis")
        print("  - Problemas de conexão com a internet")
        return None

# Executa o script
if __name__ == "__main__":

    df_parana = main()

