import re
import requests
import math
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi import HTTPException
from .models import ErrorDetail, ErrorType
from .utils import converte_html_para_markdown_memoria
from .config import settings

# Estado global para circuit breaker
_circuit_breaker_state = {
    "failures": 0,
    "last_failure_time": None,
    "is_open": False,
    "timeout_progression": [15, 20, 25, 30]  # Timeouts progressivos
}


def _check_circuit_breaker():
    """
    Verifica se o circuit breaker está aberto
    """
    if not _circuit_breaker_state["is_open"]:
        return True
    
    # Se passou mais de 60 segundos desde a última falha, tenta reabrir
    if _circuit_breaker_state["last_failure_time"]:
        time_since_failure = datetime.now() - _circuit_breaker_state["last_failure_time"]
        if time_since_failure > timedelta(seconds=60):
            print("[DEBUG] Circuit breaker: tentando reabrir após 60s")
            _circuit_breaker_state["is_open"] = False
            _circuit_breaker_state["failures"] = 0
            return True
    
    return False

def _handle_circuit_breaker_failure():
    """
    Registra falha no circuit breaker
    """
    _circuit_breaker_state["failures"] += 1
    _circuit_breaker_state["last_failure_time"] = datetime.now()
    
    # Abre o circuit breaker após 3 falhas consecutivas
    if _circuit_breaker_state["failures"] >= 3:
        _circuit_breaker_state["is_open"] = True
        print("[WARN] Circuit breaker ABERTO - muitas falhas consecutivas")

def _handle_circuit_breaker_success():
    """
    Registra sucesso no circuit breaker
    """
    if _circuit_breaker_state["failures"] > 0:
        _circuit_breaker_state["failures"] = 0
        _circuit_breaker_state["is_open"] = False
        print("[DEBUG] Circuit breaker resetado após sucesso")

def _get_progressive_timeout(tentativa: int) -> int:
    """
    Retorna timeout progressivo baseado na tentativa
    """
    timeouts = _circuit_breaker_state["timeout_progression"]
    idx = min(tentativa, len(timeouts) - 1)
    return timeouts[idx]

def _fazer_requisicao_com_retry(url: str, headers: dict, params: dict, max_tentativas: int = 3, timeout: int = 30):
    """
    Faz uma requisição HTTP com retry automático, circuit breaker e timeout progressivo
    """
    # Verifica circuit breaker
    if not _check_circuit_breaker():
        raise requests.exceptions.ConnectionError("Circuit breaker está aberto - sistema instável")
    
    for tentativa in range(max_tentativas):
        try:
            # Usa timeout progressivo se não especificado
            current_timeout = _get_progressive_timeout(tentativa) if timeout == 30 else timeout
            
            response = requests.get(url, headers=headers, params=params, timeout=current_timeout)
            
            # Sucesso - reseta circuit breaker
            _handle_circuit_breaker_success()
            return response
            
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            print(f"[DEBUG] Tentativa {tentativa + 1}/{max_tentativas} falhou (timeout={current_timeout}s): {str(e)}")
            
            # Registra falha no circuit breaker
            _handle_circuit_breaker_failure()
            
            if tentativa == max_tentativas - 1:
                raise e
            
            # Backoff exponencial mais agressivo: 2s, 4s, 8s
            tempo_espera = 2 ** (tentativa + 1)
            print(f"[DEBUG] Aguardando {tempo_espera}s antes da próxima tentativa")
            time.sleep(tempo_espera)
            
        except requests.exceptions.RequestException as e:
            # Para outros erros, registra falha e falha imediatamente
            _handle_circuit_breaker_failure()
            raise e

def _buscar_pagina_documentos(token: str, protocolo: str, id_unidade: str, pagina: int, quantidade_por_pagina: int):
    """
    Função auxiliar para buscar uma página específica de documentos com retry otimizado
    """
    try:
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/procedimentos/documentos"
        params = {
            "protocolo_procedimento": protocolo,
            "pagina": pagina,
            "quantidade": quantidade_por_pagina,
            "sinal_geracao": "N",
            "sinal_assinaturas": "N", 
            "sinal_publicacao": "N",
            "sinal_campos": "N",
            "sinal_completo": "S"
        }
        headers = {"accept": "application/json", "token": f'"{token}"'}
        
        # Timeout reduzido e tentativas limitadas para páginas
        response = _fazer_requisicao_com_retry(url, headers, params, max_tentativas=2, timeout=30)
        
        if response.status_code == 504:
            print(f"[WARN] Timeout na página {pagina}, aguardando 1s e tentando novamente")
            time.sleep(1)
            # Uma tentativa extra para timeouts
            try:
                response = _fazer_requisicao_com_retry(url, headers, params, max_tentativas=1, timeout=20)
                if response.status_code != 200:
                    return []
            except:
                return []
        
        if response.status_code == 429:  # Rate limit
            print(f"[WARN] Rate limit na página {pagina}, aguardando 3s")
            time.sleep(3)
            try:
                response = _fazer_requisicao_com_retry(url, headers, params, max_tentativas=1, timeout=30)
                if response.status_code != 200:
                    return []
            except:
                return []
        
        if response.status_code != 200:
            print(f"[WARN] Erro na página {pagina} (status {response.status_code})")
            return []
        
        documentos = response.json().get("Documentos", [])
        
        # Validação básica dos documentos retornados
        documentos_validos = []
        for doc in documentos:
            if isinstance(doc, dict) and doc.get('DocumentoFormatado'):
                documentos_validos.append(doc)
            else:
                print(f"[WARN] Documento inválido na página {pagina}: {doc}")
        
        return documentos_validos
        
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
        print(f"[WARN] Timeout/conexão na página {pagina}: {str(e)}")
        return []
    except requests.RequestException as e:
        print(f"[WARN] Erro de requisição na página {pagina}: {str(e)}")
        return []
    except Exception as e:
        print(f"[WARN] Erro inesperado na página {pagina}: {str(e)}")
        return []

def listar_documentos(token: str, protocolo: str, id_unidade: str):
    try:
        # Primeira requisição para descobrir o total de itens
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/procedimentos/documentos"
        params = {
            "protocolo_procedimento": protocolo,
            "pagina": 1,
            "quantidade": 1,
            "sinal_geracao": "N",
            "sinal_assinaturas": "N", 
            "sinal_publicacao": "N",
            "sinal_campos": "N",
            "sinal_completo": "S"
        }
        headers = {"accept": "application/json", "token": f'"{token}"'}
        print(f"[DEBUG] Fazendo requisição inicial para processo: {protocolo}")
        
        try:
            response = _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=30)
        except requests.exceptions.ConnectionError as e:
            if "Circuit breaker" in str(e):
                raise HTTPException(
                    status_code=503,
                    detail=ErrorDetail(
                        type=ErrorType.EXTERNAL_SERVICE_ERROR,
                        message="Serviço SEI temporariamente indisponível - muitas falhas recentes",
                        details={"error": "Circuit breaker ativo", "retry_after": 60}
                    ).dict()
                )
            raise e
        
        if response.status_code != 200:
            raise HTTPException(
                status_code=500,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message="Falha ao listar documentos no SEI",
                    details={"status_code": response.status_code, "response": response.text}
                ).dict()
            )
        
        total_itens = response.json().get("Info", {}).get("TotalItens", 0)
        print(f"[DEBUG] Total de documentos encontrados: {total_itens}")
        
        if total_itens == 0:
            return []
        
        # Se total é pequeno, fazer uma única requisição
        if total_itens <= 20:  # Reduzido ainda mais para casos simples
            params["quantidade"] = total_itens
            response = _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=30)
            if response.status_code != 200:
                raise HTTPException(
                    status_code=500,
                    detail=ErrorDetail(
                        type=ErrorType.EXTERNAL_SERVICE_ERROR,
                        message="Falha ao listar documentos no SEI",
                        details={"status_code": response.status_code, "response": response.text}
                    ).dict()
                )
            return response.json().get("Documentos", [])
        
        # Para totais maiores, usar estratégia resiliente
        quantidade_por_pagina = 15  # Reduzido ainda mais para melhor estabilidade
        total_paginas = math.ceil(total_itens / quantidade_por_pagina)
        
        print(f"[DEBUG] Processando {total_paginas} páginas com {quantidade_por_pagina} documentos cada")
        
        # Estratégia de fallback: tentar paralelo primeiro, depois sequencial
        todos_documentos = []
        falhas_consecutivas = 0
        
        # Usar apenas 2 workers para reduzir sobrecarga
        max_workers = min(2, total_paginas)
        
        # Tentar processamento em lotes pequenos com fallback
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            lote_size = 2  # Lotes ainda menores
            
            for inicio in range(0, total_paginas, lote_size):
                fim = min(inicio + lote_size, total_paginas)
                
                # Se muitas falhas consecutivas, muda para sequencial
                if falhas_consecutivas >= 3:
                    print("[WARN] Muitas falhas - mudando para processamento sequencial")
                    
                    for pagina in range(inicio + 1, fim + 1):
                        try:
                            documentos_pagina = _buscar_pagina_documentos(token, protocolo, id_unidade, pagina, quantidade_por_pagina)
                            if documentos_pagina:
                                todos_documentos.extend(documentos_pagina)
                                print(f"[DEBUG] Página {pagina}/{total_paginas} (sequencial): {len(documentos_pagina)} documentos")
                                falhas_consecutivas = 0  # Reset no sucesso
                            else:
                                falhas_consecutivas += 1
                                print(f"[WARN] Página {pagina}/{total_paginas} (sequencial): sem documentos")
                        except Exception as e:
                            falhas_consecutivas += 1
                            print(f"[ERRO] Página {pagina}/{total_paginas} (sequencial) falhou: {str(e)}")
                        
                        # Pausa entre páginas sequenciais
                        time.sleep(1)
                    continue
                
                # Processamento paralelo normal
                futures = {
                    executor.submit(_buscar_pagina_documentos, token, protocolo, id_unidade, pagina + 1, quantidade_por_pagina): pagina + 1
                    for pagina in range(inicio, fim)
                }
                
                lote_documentos = []
                falhas_lote = 0
                
                for future in as_completed(futures, timeout=45):  # Timeout no lote
                    pagina = futures[future]
                    try:
                        documentos_pagina = future.result()
                        if documentos_pagina:
                            lote_documentos.extend(documentos_pagina)
                            print(f"[DEBUG] Página {pagina}/{total_paginas}: {len(documentos_pagina)} documentos")
                        else:
                            falhas_lote += 1
                            print(f"[WARN] Página {pagina}/{total_paginas}: sem documentos retornados")
                    except Exception as e:
                        falhas_lote += 1
                        print(f"[ERRO] Página {pagina}/{total_paginas} falhou: {str(e)}")
                
                todos_documentos.extend(lote_documentos)
                
                # Atualiza contador de falhas consecutivas
                if falhas_lote > 0:
                    falhas_consecutivas += falhas_lote
                else:
                    falhas_consecutivas = 0
                
                # Pausa adaptativa baseada nas falhas
                if falhas_lote > 1:
                    pausa = min(5, falhas_lote * 2)  # Máximo 5s
                    print(f"[DEBUG] {falhas_lote} falhas no lote, aguardando {pausa}s")
                    time.sleep(pausa)
                else:
                    time.sleep(0.5)
        
        print(f"[DEBUG] Total final: {len(todos_documentos)}/{total_itens} documentos carregados")
        
        # Se obteve menos de 50% dos documentos, avisa mas não falha
        porcentagem = (len(todos_documentos) / total_itens) * 100
        if porcentagem < 50:
            print(f"[WARN] Apenas {porcentagem:.1f}% dos documentos foram carregados devido a instabilidades da API")
        
        # Ordenar por ordem de criação para manter consistência
        if todos_documentos:
            try:
                todos_documentos.sort(key=lambda x: x.get('DataCriacao', ''))
                print(f"[DEBUG] Documentos ordenados por data de criação")
            except Exception as e:
                print(f"[WARN] Não foi possível ordenar documentos: {str(e)}")
        
        return todos_documentos
        
    except requests.RequestException as e:
        # Tratamento específico para diferentes tipos de erro
        error_msg = str(e)
        
        if "Read timed out" in error_msg:
            raise HTTPException(
                status_code=504,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message="Timeout ao conectar com o serviço SEI - tente novamente em alguns minutos",
                    details={"error": error_msg, "suggestion": "A API do SEI pode estar sobrecarregada"}
                ).dict()
            )
        elif "Connection" in error_msg:
            raise HTTPException(
                status_code=502,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message="Erro de conexão com o serviço SEI",
                    details={"error": error_msg, "suggestion": "Verifique a conectividade de rede"}
                ).dict()
            )
        else:
            raise HTTPException(
                status_code=500,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message="Erro ao conectar com o serviço SEI para listar documentos",
                    details={"error": error_msg}
                ).dict()
            )

def _buscar_pagina_andamentos(token: str, protocolo: str, id_unidade: str, pagina: int, quantidade_por_pagina: int):
    """
    Função auxiliar para buscar uma página específica de andamentos com retry
    """
    try:
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/procedimentos/andamentos"
        params = {
            "protocolo_procedimento": protocolo,
            "sinal_atributos": "S",
            "pagina": pagina,
            "quantidade": quantidade_por_pagina
        }
        headers = {"accept": "application/json", "token": f'"{token}"'}
        
        response = _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=45)
        
        if response.status_code == 504:
            print(f"[WARN] Timeout na página {pagina} de andamentos, pulando esta página")
            return []
        
        if response.status_code != 200:
            print(f"[WARN] Erro na página {pagina} de andamentos (status {response.status_code}), pulando esta página")
            return []
        
        return response.json().get("Andamentos", [])
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
        print(f"[WARN] Timeout/conexão falhou na página {pagina} de andamentos, pulando: {str(e)}")
        return []
    except requests.RequestException as e:
        print(f"[WARN] Erro de requisição na página {pagina} de andamentos, pulando: {str(e)}")
        return []
    except Exception as e:
        print(f"[WARN] Erro inesperado na página {pagina} de andamentos, pulando: {str(e)}")
        return []

def listar_tarefa(token: str, protocolo: str, id_unidade: str):
    try:
        # Primeira requisição para descobrir o total de itens
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/procedimentos/andamentos"
        params = {
            "protocolo_procedimento": protocolo,
            "sinal_atributos": "S",
            "pagina": 1,
            "quantidade": 1
        }
        headers = {"accept": "application/json", "token": f'"{token}"'}
        response = _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=45)
        
        if response.status_code != 200:
            raise HTTPException(
                status_code=500,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message="Falha ao consultar andamentos no SEI",
                    details={"status_code": response.status_code, "response": response.text}
                ).dict()
            )
        
        total_itens = response.json().get("Info", {}).get("TotalItens", 0)
        
        if total_itens == 0:
            return []
        
        # Se total é pequeno, fazer uma única requisição
        if total_itens <= 10:
            params["quantidade"] = total_itens
            response = _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=45)
            if response.status_code != 200:
                raise HTTPException(
                    status_code=500,
                    detail=ErrorDetail(
                        type=ErrorType.EXTERNAL_SERVICE_ERROR,
                        message="Falha ao consultar andamentos no SEI",
                        details={"status_code": response.status_code, "response": response.text}
                    ).dict()
                )
            return response.json().get("Andamentos", [])
        
        # Para totais maiores, usar paginação paralela
        quantidade_por_pagina = 10  # Quantidade fixa de itens por página para andamentos
        total_paginas = math.ceil(total_itens / quantidade_por_pagina)
        
        print(f"[DEBUG] Total de andamentos: {total_itens}, Páginas: {total_paginas}, Itens por página: {quantidade_por_pagina}")
        
        # Executar requisições em paralelo
        todas_tarefas = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Submeter todas as páginas para execução paralela
            futures = {
                executor.submit(_buscar_pagina_andamentos, token, protocolo, id_unidade, pagina, quantidade_por_pagina): pagina
                for pagina in range(1, total_paginas + 1)
            }
            
            # Coletar resultados conforme completam
            itens_coletados = 0
            for future in as_completed(futures):
                pagina = futures[future]
                try:
                    tarefas_pagina = future.result()
                    todas_tarefas.extend(tarefas_pagina)
                    itens_coletados += len(tarefas_pagina)
                    print(f"[DEBUG] Página {pagina}/{total_paginas} carregada: {len(tarefas_pagina)} andamentos (Total coletado: {itens_coletados}/{total_itens})")
                except Exception as e:
                    print(f"[ERRO] Falha na página {pagina}: {str(e)}")
                    # Continua com outras páginas mesmo se uma falhar
        
        print(f"[DEBUG] Paginação concluída. Total esperado: {total_itens}, Total coletado: {len(todas_tarefas)}")
        return todas_tarefas
    except requests.RequestException as e:
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.EXTERNAL_SERVICE_ERROR,
                message="Erro ao conectar com o serviço SEI para consultar andamentos",
                details={"error": str(e)}
            ).dict()
        )

def consultar_documento(token: str, id_unidade: str, documento_formatado: str):
    try:
        print(f"[DEBUG] Consultando documento: {documento_formatado}")
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/documentos"
        params = {"protocolo_documento": documento_formatado, "sinal_completo": "N"}
        headers = {"accept": "application/json", "token": f'"{token}"'}
        print(f"[DEBUG] URL: {url}")
        print(f"[DEBUG] Params: {params}")
        response = _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=45)
        if response.status_code != 200:
            raise HTTPException(
                status_code=500,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message="Falha ao consultar documento no SEI",
                    details={"status_code": response.status_code, "response": response.text}
                ).dict()
            )
        return response.json()
    except requests.RequestException as e:
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.EXTERNAL_SERVICE_ERROR,
                message="Erro ao conectar com o serviço SEI para consultar documento",
                details={"error": str(e)}
            ).dict()
        )

def baixar_documento(token: str, id_unidade: str, documento_formatado: str, numero_processo: str = None):
    try:
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/documentos/baixar"
        headers = {"accept": "application/json", "token": f'"{token}"'}
        params = {"protocolo_documento": documento_formatado}
        response = _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=60)

        if response.status_code != 200:
            raise HTTPException(
                status_code=500,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message="Falha ao baixar documento do SEI",
                    details={"status_code": response.status_code, "response": response.text}
                ).dict()
            )

        content_disposition = response.headers.get("content-disposition", "")
        match = re.search(r'filename="(.+)"', content_disposition)
        filename = match.group(1) if match else f"documento_{documento_formatado}.html"

        # Processar documento para Markdown se for HTML - apenas em memória
        if filename.lower().endswith(".html") or filename.lower().endswith(".htm"):
            try:
                # Converter HTML para Markdown diretamente em memória
                html_content = response.content.decode('utf-8')
                md_content = converte_html_para_markdown_memoria(html_content)
                return md_content
            except Exception as e:
                print(f"[ERRO] Falha na conversão HTML->MD: {str(e)}")
                return None
        else:
            return None
    except requests.RequestException as e:
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.EXTERNAL_SERVICE_ERROR,
                message="Erro ao conectar com o serviço SEI para baixar documento",
                details={"error": str(e)}
            ).dict()
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.PROCESSING_ERROR,
                message="Erro ao processar documento baixado",
                details={"error": str(e)}
            ).dict()
        )
