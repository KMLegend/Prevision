import requests
import json
import os
import time # Importe a biblioteca 'time' para usar o sleep
import re
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import openpyxl

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ==============================================================================
# 1. CONFIGURAÇÕES E CARREGAMENTO DE VARIÁVEIS DE AMBIENTE
# ==============================================================================
print("▶️ Carregando variáveis de ambiente...")
try:
    # load_dotenv(dotenv_path=r"\\192.168.100.3\dados city\Inovação e Sistemas\01-INOVACAO\02-DESENVOLVIMENTO\04-ARQUIVOS\.env")
    load_dotenv(dotenv_path=r"/mnt/dados_city/Inovação e Sistemas/01-INOVACAO/02-DESENVOLVIMENTO/04-ARQUIVOS/.env")
    print("✅ Arquivo .env encontrado e carregado.")
except Exception as e:
    print(f"❌ Atenção: Não foi possível carregar o arquivo .env. Verifique o caminho. Erro: {e}")

API_TOKEN = os.getenv("API_TOKEN_PREVISION")
BASE_URL_REST = 'https://api.prevision.com.br'
URL_GRAPHQL = 'https://api.prevision.com.br/graphql'
USER_AGENT = os.getenv("USER_AGENT_PREVISION", "insomnia/11.6.1")

def _get_int_env(var_name: str, default: int) -> int:
    try:
        return int(os.getenv(var_name, default))
    except (TypeError, ValueError):
        return default

WAIT_INTERVAL_SECONDS = _get_int_env("PREVISION_WAIT_SECONDS", 300)
REQUEST_TIMEOUT_SECONDS = _get_int_env("PREVISION_REQUEST_TIMEOUT", 300)
MAX_REQUEST_RETRIES = _get_int_env("PREVISION_MAX_RETRIES", 3)
RETRY_DELAY_SECONDS = _get_int_env("PREVISION_RETRY_DELAY", 300)
RAW_JSON_DIR = os.path.join(BASE_DIR, "responses_raw")
os.makedirs(RAW_JSON_DIR, exist_ok=True)

if not API_TOKEN:
    print("❌ ERRO CRÍTICO: A variável de ambiente 'API_TOKEN' não foi encontrada.")
    exit()

# ==============================================================================
# 2. FUNÇÃO PARA API GRAPHQL (Formatando como String manualmente)
# ==============================================================================

def busca_activies(id_project: int):
    timestamp_ms = int(time.time() * 1000)
    filename = f'New_Request-{timestamp_ms}.json'
    filepath = os.path.join(RAW_JSON_DIR, filename)
    tentativa = 0
    while tentativa < MAX_REQUEST_RETRIES:
        tentativa += 1
        try:
            response = requests.get(
                f"{BASE_URL_REST}/construction/api/v1/projects/{id_project}/schedule",
                headers={
                    'Accept': 'application/json',
                    'Content-Type': 'application/json',
                    'Authorization': API_TOKEN,
                    'User-Agent': USER_AGENT
                },
                timeout=REQUEST_TIMEOUT_SECONDS
            )
        except requests.RequestException as exc:
            error_message = f"⚠️ Erro de requisição para o projeto {id_project}: {exc}"
            response = None
        else:
            if response.status_code == 200:
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(response.json(), f, ensure_ascii=False, indent=4)
                return filepath
            error_message = (
                f"❌ Erro ao buscar activities para o projeto {id_project}: "
                f"{response.status_code} - {response.text}"
            )
        print(error_message)
        if tentativa < MAX_REQUEST_RETRIES:
            print(
                f"🔁 Tentativa {tentativa}/{MAX_REQUEST_RETRIES} falhou. "
                f"Tentando novamente em {RETRY_DELAY_SECONDS} segundos..."
            )
            if RETRY_DELAY_SECONDS > 0:
                time.sleep(RETRY_DELAY_SECONDS)
    print(f"🚫 Não foi possível obter as atividades do projeto {id_project} após {MAX_REQUEST_RETRIES} tentativas.")
    return None
def aguardar_intervalo(segundos: int = 300):
    """Exibe uma barra de progresso amigável durante a espera entre requisições."""
    if segundos <= 0:
        print("Intervalo configurado para 0 segundos. Prosseguindo imediatamente.")
        return
    if tqdm is None:
        print(f"Aguardando {segundos} segundos para a próxima requisição...")
        time.sleep(segundos)
        return
    passo = max(1, segundos // 100)
    restante = segundos
    with tqdm(total=segundos, desc="Aguardando próxima requisição", unit="s") as barra:
        while restante > 0:
            intervalo = min(passo, restante)
            time.sleep(intervalo)
            barra.update(intervalo)
            restante -= intervalo

def listar_projetos():
    response = requests.get(
        f"{BASE_URL_REST}/construction/api/v1/projects",
        headers={
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': API_TOKEN
        }
    )
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"❌ Erro ao listar projetos: {response.status_code} - {response.text}")
        return None
def exportar_xlsx(input_json_path, output_excel_path, project_id=None, project_name=None):
    # input_json_path = '/home/kevin/Documentos/Projetos/Prevision/New_Request-1761069509258.json'
    # # MUDANÇA AQUI: Novo nome de arquivo com extensão .xlsx
    # output_excel_path = '/home/kevin/Documentos/Projetos/Prevision/activities_output.xlsx'
    # --- 2. Carregar o arquivo JSON ---
    with open(input_json_path, 'r', encoding='utf-8') as f:
        data_dict = json.load(f)
    # --- 3. Normalizar o JSON ---
    df = pd.json_normalize(data_dict, record_path='activities')
    # --- 4. Definir e aplicar os tipos de dados (Isto ainda é uma boa prática!) ---
    # Dicionário com os tipos desejados
    tipos_de_dados = {
        "activity_level": str,
        "baseline_linked_cost": float,
        "categorization": str,
        "critical_path": str,
        "delay_reasons": str,
        "duration": float,
        "floor": str,
        "id": int,
        "last_measurement_base": float,  # Garantindo que isto é um float
        "last_measurement_expected": float,
        "last_measurement_realized": float,
        "last_measurement_unit_progress": float,
        "linked_cost": float,
        "material_resources": str,
        "part_counter": str,
        "physical_progress_percentage_base": float,
        "physical_progress_percentage_expected": float,
        "physical_progress_percentage_realized": float,
        "physical_progress_unit_amount": float,
        "physical_progress_unit_base": float,
        "physical_progress_unit_expected": float,
        "physical_progress_unit_realized": float,
        "physical_progress_unit_realized_description": str,
        "physical_progress_unit_remainder": float,
        "predecessors": str,
        "real_date_duration": str,
        "real_date_end_at": str,
        "replication_group": str,
        "responsible": str,
        "service": str,
        "service_position": int,
        "successors": str,
        "unit": str
    }

    # Lista de colunas de data para converter
    colunas_de_data = [
        "baseline_step_end",
        "baseline_step_start",
        "end_date",
        "first_measured_in",
        "last_measured_in",
        "last_measurement_date",
        "real_date_start_at",
        "reference_date",
        "start_date",
        "unit_reference_date"
    ]

    # Aplicar a conversão de tipos
    try:
        for coluna, tipo in tipos_de_dados.items():
            if coluna not in df.columns:
                continue
            if tipo is float:
                df[coluna] = pd.to_numeric(df[coluna], errors='coerce')
            elif tipo is int:
                df[coluna] = pd.to_numeric(df[coluna], errors='coerce').astype('Int64')
            else:
                df[coluna] = df[coluna].astype(tipo)
        
        for col in colunas_de_data:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce', utc=True).dt.tz_convert(None)
                
        print("Conversão de tipos realizada com sucesso.")
    except KeyError as e:
        print(f"Aviso: A coluna {e} não foi encontrada no JSON e será ignorada.")
    except Exception as e:
        print(f"Erro durante a conversão de tipos: {e}")
    project_identifier = (
        project_id
        or data_dict.get("project_id")
        or data_dict.get("projectId")
        or (data_dict.get("project") or {}).get("id")
    )
    if project_identifier is None:
        raise ValueError(
            "Identificador do projeto não encontrado. Informe o parâmetro 'project_id' ao chamar exportar_xlsx."
        )
    os.makedirs(output_excel_path, exist_ok=True)
    caminho_completo = f'{output_excel_path}/activities_project_{project_identifier}.xlsx'
    df.insert(0, "project_id", project_identifier)
    if project_name is not None:
        df.insert(1, "project_name", str(project_name))
    # --- 5. MUDANÇA PRINCIPAL: Salvar em EXCEL ---
    # Trocamos to_csv por to_excel
    # Os parâmetros 'sep', 'decimal' e 'encoding' não são necessários
    df.to_excel(
        caminho_completo, 
        index=False,         # Para não salvar o índice do Pandas
        engine='openpyxl'    # Especifica a biblioteca que estamos usando
    )
    return caminho_completo
def consolidar_planilha_atividades(excel_files, output_file):
    all_activities = pd.DataFrame()
    output_dir = os.path.dirname(output_file) or "."
    os.makedirs(output_dir, exist_ok=True)
    if not excel_files:
        print("⚠️ Nenhuma planilha de atividades encontrada para consolidar.")
        return
    for file in excel_files:
        df = pd.read_excel(file, engine='openpyxl')
        all_activities = pd.concat([all_activities, df], ignore_index=True)
        all_activities.to_excel(output_file, index=False, engine='openpyxl')
def limpar_respostas_raw():
    """Remove os arquivos JSON gerados após a consolidação."""
    try:
        arquivos = [
            os.path.join(RAW_JSON_DIR, arquivo)
            for arquivo in os.listdir(RAW_JSON_DIR)
            if arquivo.endswith(".json")
        ]
        for arquivo in arquivos:
            os.remove(arquivo)
        if arquivos:
            print(f"🧹 Removidos {len(arquivos)} arquivos JSON temporários.")
    except FileNotFoundError:
        pass
    except Exception as exc:
        print(f"⚠️ Não foi possível limpar os arquivos temporários: {exc}")

# ==============================================================================
# 5. FUNÇÃO PRINCIPAL (ORQUESTRADOR GERAL)
# ==============================================================================
def main():
    print("\n🚀 Iniciando o processo de extração da API Prevision...")
    # input_json_path = '/home/kevin/Documentos/Projetos/Prevision/New_Request-1761069509258.json'
    output_excel_path = '/home/kevin/Documentos/Projetos/Prevision/projects/'
    os.makedirs(output_excel_path, exist_ok=True)
    json_projects = listar_projetos()
    
    for project in json_projects['projects']:
        print(f"ID do Projeto: {project['id']} - Nome: {project['name']}")
        
        project_id = project['id']
        json_response = busca_activies(project_id)
        
        aguardar_intervalo(WAIT_INTERVAL_SECONDS)  # Aguardar 5 minutos entre as requisições para evitar o erro 429
        if not json_response:
            print(f"⏭️ Requisição para o projeto {project_id} falhou. Pulando exportação.")
            continue
        exportar_xlsx(json_response, output_excel_path, project_id, project.get("name"))
    consolidar_planilha_atividades(
        excel_files=[output_excel_path + f for f in os.listdir(output_excel_path) if f.startswith('activities_project_') and f.endswith('.xlsx')],
        output_file='/home/kevin/Documentos/Projetos/Prevision/atividades_consolidadas.xlsx'
    )
    limpar_respostas_raw()
    print("\n🎉 Processo concluído!")

# ==============================================================================
# PONTO DE ENTRADA DO SCRIPT
# ==============================================================================
if __name__ == "__main__":
    main()