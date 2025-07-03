"""
Processador principal para arquivos do Microsoft Planner.

Este script coordena o processamento de arquivos Excel exportados do Planner,
aplicando transformações e gerando arquivos de saída organizados.
"""

import sys
import logging
import pandas as pd
from pathlib import Path
from typing import Optional

import pipeline
import test_validation


def setup_logging(base_dir: Path) -> None:
    """
    Configura o sistema de logging.
    
    Args:
        base_dir: Diretório base do projeto para criar pasta logs
    """
    # Criar pasta logs se não existir
    logs_dir = base_dir / "logs"
    logs_dir.mkdir(exist_ok=True, parents=True)
    
    # Nome do arquivo de log com timestamp
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"processar_planner_{timestamp}.log"
    
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding='utf-8')
        ]
    )


def validate_paths(input_path: Path, output_dir: Path) -> bool:
    """
    Valida se os caminhos de entrada e saída são válidos.
    
    Args:
        input_path: Caminho do arquivo de entrada
        output_dir: Diretório de saída
        
    Returns:
        True se todos os caminhos são válidos, False caso contrário
    """
    if not input_path.exists():
        logging.error(f"Arquivo de entrada não encontrado: {input_path}")
        return False
    
    if not input_path.suffix.lower() in ['.xlsx', '.xls']:
        logging.error(f"Arquivo deve ser Excel (.xlsx ou .xls): {input_path}")
        return False
    
    try:
        output_dir.mkdir(exist_ok=True, parents=True)
        logging.info(f"Diretório de saída criado/validado: {output_dir}")
    except Exception as e:
        logging.error(f"Erro ao criar diretório de saída {output_dir}: {e}")
        return False
    
    return True


def find_latest_planner_file(input_dir: Path) -> Optional[Path]:
    """
    Procura pelo arquivo Excel mais recente do Planner no diretório.
    
    Busca por arquivos que começam com "Gerenciamento de Projetos" ou 
    padrões similares do Planner.
    
    Args:
        input_dir: Diretório onde procurar os arquivos
        
    Returns:
        Caminho do arquivo mais recente encontrado ou None
    """
    # Padrões de nomes de arquivo do Planner
    patterns = [
        "Gerenciamento de Projetos*.xlsx",
        "Gerenciamento de Projetos*.xls", 
        "TarefasPlanner*.xlsx",
        "TarefasPlanner*.xls"
    ]
    
    all_files = []
    
    # Buscar arquivos com todos os padrões
    for pattern in patterns:
        files = list(input_dir.glob(pattern))
        all_files.extend(files)
    
    if not all_files:
        logging.error("Nenhum arquivo do Planner encontrado no diretório de entrada")
        logging.info("Padrões procurados:")
        for pattern in patterns:
            logging.info(f"  - {pattern}")
        return None
    
    # Ordenar por data de modificação (mais recente primeiro)
    latest_file = max(all_files, key=lambda f: f.stat().st_mtime)
    
    logging.info(f"Arquivo encontrado: {latest_file.name}")
    
    # Se há múltiplos arquivos, avisar
    if len(all_files) > 1:
        logging.info(f"Múltiplos arquivos encontrados ({len(all_files)}), usando o mais recente")
        
    return latest_file


def get_config() -> dict:
    """
    Retorna a configuração de caminhos do projeto.
    
    Returns:
        Dicionário com os caminhos configurados
    """
    base_dir = Path(r"C:\Users\artur.almeida\Documents\progs\PowerBi_Projetos")
    
    return {
        'base_dir': base_dir,
        'input_dir': base_dir / "inputs",
        'output_dir': base_dir / "outputs",
        # Removido input_file fixo - será detectado automaticamente
    }


def main(input_filename: Optional[str] = None, run_validation: bool = True) -> None:
    """
    Função principal do processamento.
    
    Args:
        input_filename: Nome específico do arquivo (opcional).
                       Se não fornecido, busca automaticamente por:
                       - "Gerenciamento de Projetos*.xlsx"
                       - "TarefasPlanner*.xlsx"
        run_validation: Se True, executa validação após processamento
    """
    # Carregar configuração primeiro
    config = get_config()
    
    # Setup logging com o diretório base
    setup_logging(config['base_dir'])
    
    logging.info("INICIANDO PROCESSAMENTO DO PLANNER")
    
    try:
        # Buscar arquivo automaticamente ou usar o especificado
        if input_filename:
            # Arquivo específico fornecido via linha de comando
            input_path = config['input_dir'] / input_filename
            if not input_path.exists():
                logging.error(f"Arquivo especificado não encontrado: {input_filename}")
                sys.exit(1)
        else:
            # Busca automática pelo arquivo mais recente
            input_path = find_latest_planner_file(config['input_dir'])
            if input_path is None:
                logging.error("Nenhum arquivo do Planner encontrado para processar")
                sys.exit(1)
        
        output_dir = config['output_dir']
        
        # Validar caminhos
        if not validate_paths(input_path, output_dir):
            logging.error("Validação de caminhos falhou. Encerrando processamento.")
            sys.exit(1)
        
        # Executar pipeline
        output_file = pipeline.main(input_path=input_path, output_dir=output_dir)
        
        # Executar validação se solicitado
        if run_validation and output_file:
            logging.info("Iniciando validação dos resultados...")
            validation_passed = test_validation.main(input_path, output_file)
            
            if validation_passed:
                logging.info("✅ Validação concluída: Todos os testes críticos passaram")
            else:
                logging.warning("⚠️ Validação detectou problemas - revisar logs acima")
        
        logging.info("PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
        
    except KeyboardInterrupt:
        logging.warning("Processamento interrompido pelo usuário (Ctrl+C)")
        sys.exit(130)
    except Exception as e:
        logging.error(f"Erro durante o processamento: {e}")
        logging.exception("Detalhes do erro:")
        sys.exit(1)


if __name__ == "__main__":
    # Processar argumentos da linha de comando
    import argparse
    
    parser = argparse.ArgumentParser(description="Processador de arquivos do Planner")
    parser.add_argument("arquivo", nargs="?", help="Nome do arquivo específico (opcional)")
    parser.add_argument("--no-validation", action="store_true", 
                       help="Pular validação após processamento")
    parser.add_argument("--help-extended", action="store_true",
                       help="Mostrar ajuda detalhada")
    
    # Parse argumentos ou usar sys.argv se preferir o método antigo
    if len(sys.argv) > 1 and sys.argv[1] in ["--help", "-h", "--help-extended"]:
        if "--help-extended" in sys.argv:
            print("="*60)
            print("PROCESSADOR DE ARQUIVOS DO PLANNER")
            print("="*60)
            print()
            print("USO:")
            print("  python processar_planner.py                    # Busca automática + validação")
            print("  python processar_planner.py arquivo.xlsx       # Arquivo específico + validação")
            print("  python processar_planner.py --no-validation    # Sem validação")
            print("  python processar_planner.py arquivo.xlsx --no-validation")
            print()
            print("BUSCA AUTOMÁTICA:")
            print("  O script procura automaticamente por:")
            print("  - Gerenciamento de Projetos*.xlsx")
            print("  - TarefasPlanner*.xlsx")
            print("  - Usa sempre o arquivo mais recente")
            print()
            print("VALIDAÇÃO:")
            print("  Por padrão, executa validação completa:")
            print("  - Verifica se dados importantes foram perdidos")
            print("  - Valida regras de bucket")
            print("  - Confirma integridade dos dados")
            print("  - Gera relatório detalhado")
            print()
            print("EXEMPLOS:")
            print("  python processar_planner.py")
            print("  python processar_planner.py \"Gerenciamento de Projetos (2).xlsx\"")
            print("  python processar_planner.py --no-validation")
            print("="*60)
        else:
            parser.print_help()
        sys.exit(0)
    
    # Método simples para compatibilidade
    input_file = None
    run_validation = True
    
    for arg in sys.argv[1:]:
        if arg == "--no-validation":
            run_validation = False
        elif not arg.startswith("--"):
            input_file = arg
    
    main(input_filename=input_file, run_validation=run_validation)