import json

def carregar_projetos(arquivo_json):
    """
    Carrega os projetos de um arquivo JSON.
    
    :param arquivo_json: Caminho para o arquivo JSON contendo os projetos.
    :return: Lista de projetos carregados do arquivo JSON.
    """
    try:
        with open(arquivo_json, 'r', encoding='utf-8') as file:
            projetos = json.load(file)
        print(f"✅ {len(projetos)} projetos carregados do arquivo '{arquivo_json}'.")
        projetos_list_id = [projeto['id'] for projeto in projetos if 'id' in projeto]
        if not projetos_list_id:
            print("❗ Nenhum projeto encontrado no arquivo.")
            return []
        return projetos_list_id
    
    except FileNotFoundError:
        print(f"❌ Arquivo '{arquivo_json}' não encontrado.")
        return []
    except json.JSONDecodeError:
        print(f"❌ Erro ao decodificar o JSON no arquivo '{arquivo_json}'.")
        return []
    
def carregar_orcamentos(arquivo_json):
    """
    Carrega os projetos de um arquivo JSON.
    
    :param arquivo_json: Caminho para o arquivo JSON contendo os projetos.
    :return: Lista de projetos carregados do arquivo JSON.
    """
    try:
        with open(arquivo_json, 'r', encoding='utf-8') as file:
            orcamentos = json.load(file)
        print(f"✅ {len(orcamentos)} orcamentos carregados do arquivo '{arquivo_json}'.")
        orcamentos_list_id = [orcamento['id'] for orcamento in orcamentos if 'id' in orcamento]
        if not orcamentos_list_id:
            print("❗ Nenhum projeto encontrado no arquivo.")
            return []
        return orcamentos_list_id
    
    except FileNotFoundError:
        print(f"❌ Arquivo '{arquivo_json}' não encontrado.")
        return []
    except json.JSONDecodeError:
        print(f"❌ Erro ao decodificar o JSON no arquivo '{arquivo_json}'.")
        return []