#!/usr/bin/env python3
"""
Pipeline de processamento para dados do Microsoft Planner.

Este módulo contém todas as transformações aplicadas aos dados exportados
do Planner, incluindo limpeza, normalização e formatação.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from parser import parse_dates, debug_missing_due


# Configurações centralizadas
class PlannerConfig:
    """Configurações do pipeline do Planner."""
    
    # Mapeamento de colunas (inglês -> português)
    COLUMN_MAPPING = {
        'Identificação da tarefa': 'ID da Tarefa',
        'Nome da tarefa': 'Título',
        'Nome do Bucket': 'Bucket',
        'Progresso': 'Progresso (%)',
        'Prioridade': 'Prioridade',
        'Atribuído a': 'Atribuído',
        'Criado por': 'Criado por',
        'Criado em': 'Data de criação',
        'Data de início': 'Data de início',
        'Data de conclusão': 'Data de entrega',
        'Concluído em': 'Data de conclusão',
    }
    
    # Buckets que devem ter datas zeradas
    INACTIVE_BUCKETS = ['backlog', 'a fazer']
    
    # Buckets que PODEM ter datas de início e entrega
    ACTIVE_BUCKETS = ['execução', 'aguardando validação', 'concluídos', 'concluido', 'concluida', 'concluidas']
    
    # Colunas de data para processamento
    DATE_COLUMNS = [
        'Data de criação',
        'Data de início', 
        'Data de entrega',
        'Data de conclusão'
    ]
    
    # Valor padrão para atribuições vazias
    DEFAULT_ASSIGNEE = 'Michael Oneil; Artur Almeida'
    
    # Configuração para preenchimento de datas de início
    START_DATE_FILL_DAYS = 20  # Quantos dias antes da entrega


def load_and_rename(input_path: Path) -> pd.DataFrame:
    """
    Carrega arquivo Excel e renomeia colunas para padrão português.
    
    Args:
        input_path: Caminho do arquivo Excel
        
    Returns:
        DataFrame com colunas renomeadas
        
    Raises:
        FileNotFoundError: Se arquivo não existir
        pd.errors.EmptyDataError: Se arquivo estiver vazio
    """
    try:
        df = pd.read_excel(input_path)
        
        if df.empty:
            raise pd.errors.EmptyDataError("Arquivo Excel está vazio")
        
        logging.info(f"Arquivo carregado: {len(df)} linhas, {len(df.columns)} colunas")
        
        # Aplicar renomeação
        df = df.rename(columns=PlannerConfig.COLUMN_MAPPING)
        
        return df
        
    except FileNotFoundError:
        logging.error(f"Arquivo não encontrado")
        raise
    except Exception as e:
        logging.error(f"Erro ao carregar arquivo: {e}")
        raise


def validate_required_columns(df: pd.DataFrame) -> None:
    """
    Valida se todas as colunas essenciais estão presentes.
    
    Args:
        df: DataFrame para validar
        
    Raises:
        KeyError: Se colunas obrigatórias estiverem faltando
    """
    required_columns = ['Bucket', 'Data de entrega', 'Título']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        error_msg = f"Colunas obrigatórias ausentes: {missing_columns}"
        logging.error(error_msg)
        raise KeyError(error_msg)
    
    logging.info("Validação de colunas: OK")


def clean_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpa colunas de texto removendo espaços extras.
    
    Args:
        df: DataFrame para limpar
        
    Returns:
        DataFrame com strings limpas
    """
    string_columns = df.select_dtypes(include=['object']).columns
    
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    
    return df


def fill_default_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preenche valores vazios com padrões definidos.
    
    Args:
        df: DataFrame para preencher
        
    Returns:
        DataFrame com valores padrão aplicados
    """
    fill_values = {
        'Atribuído': PlannerConfig.DEFAULT_ASSIGNEE
    }
    
    df = df.fillna(fill_values)
    
    return df


def validate_dates_by_bucket(df: pd.DataFrame, 
                            bucket_col: str = 'Bucket',
                            start_col: str = 'Data de início',
                            due_col: str = 'Data de entrega') -> pd.DataFrame:
    """
    Remove datas de início e entrega para buckets que não devem ter essas datas.
    
    Apenas os buckets 'EXECUÇÃO', 'AGUARDANDO VALIDAÇÃO' e 'CONCLUÍDOS' 
    podem ter datas de início e entrega. Outros buckets terão essas datas removidas.
    
    Args:
        df: DataFrame para processar
        bucket_col: Nome da coluna de bucket
        start_col: Nome da coluna de data de início  
        due_col: Nome da coluna de data de entrega
        
    Returns:
        DataFrame com datas validadas por bucket
    """
    if bucket_col not in df.columns:
        logging.warning(f"Coluna '{bucket_col}' não encontrada. Pulando validação de datas por bucket.")
        return df
    
    # PRIMEIRO: Log dos buckets únicos encontrados para debug
    unique_buckets = df[bucket_col].dropna().unique()
    logging.info(f"Buckets encontrados no arquivo: {list(unique_buckets)}")
    
    # Normalizar nomes dos buckets para comparação
    df_copy = df.copy()
    df_copy['bucket_normalized'] = (df_copy[bucket_col]
                                   .str.strip()
                                   .str.lower()
                                   .str.replace('ç', 'c')
                                   .str.replace('ã', 'a')
                                   .str.replace('í', 'i')
                                   .str.replace('ú', 'u')
                                   .str.replace('õ', 'o')
                                   .str.replace('ê', 'e')
                                   .str.replace('é', 'e')
                                   .str.replace('á', 'a')
                                   .str.replace('ó', 'o'))
    
    # Normalizar buckets permitidos também
    allowed_buckets_normalized = []
    for bucket in PlannerConfig.ACTIVE_BUCKETS:
        normalized = (bucket.lower()
                     .replace('ç', 'c')
                     .replace('ã', 'a') 
                     .replace('í', 'i')
                     .replace('ú', 'u')
                     .replace('õ', 'o')
                     .replace('ê', 'e')
                     .replace('é', 'e')
                     .replace('á', 'a')
                     .replace('ó', 'o'))
        allowed_buckets_normalized.append(normalized)
    
    logging.info(f"Buckets permitidos (normalizados): {allowed_buckets_normalized}")
    
    # Log dos buckets normalizados encontrados
    unique_normalized = df_copy['bucket_normalized'].dropna().unique()
    logging.info(f"Buckets encontrados (normalizados): {list(unique_normalized)}")
    
    # Identificar buckets que NÃO podem ter datas
    mask_invalid_bucket = ~df_copy['bucket_normalized'].isin(allowed_buckets_normalized)
    
    # Log detalhado dos buckets que serão afetados
    invalid_buckets_detail = df_copy.loc[mask_invalid_bucket, [bucket_col, 'bucket_normalized']].drop_duplicates()
    if len(invalid_buckets_detail) > 0:
        logging.info("Buckets que terão datas removidas:")
        for _, row in invalid_buckets_detail.iterrows():
            logging.info(f"  Original: '{row[bucket_col]}' -> Normalizado: '{row['bucket_normalized']}'")
    
    # Contar registros que serão afetados
    invalid_with_start = mask_invalid_bucket & df_copy[start_col].notna()
    invalid_with_due = mask_invalid_bucket & df_copy[due_col].notna()
    
    start_removals = invalid_with_start.sum()
    due_removals = invalid_with_due.sum()
    total_invalid_buckets = mask_invalid_bucket.sum()
    
    if start_removals > 0 or due_removals > 0:
        logging.info(f"Removendo datas de {total_invalid_buckets} registros com buckets não permitidos")
        logging.info(f"  - {start_removals} datas de início removidas")
        logging.info(f"  - {due_removals} datas de entrega removidas")
        
        # Mostrar exemplos dos buckets problemáticos
        invalid_buckets = df_copy.loc[mask_invalid_bucket, bucket_col].value_counts()
        logging.info("Buckets com datas removidas:")
        for bucket, count in invalid_buckets.items():
            logging.info(f"  - '{bucket}': {count} registros")
    else:
        logging.info("✅ Nenhuma data removida - todos os buckets estão permitidos")
    
    # Remover as datas dos buckets inválidos
    df_copy.loc[mask_invalid_bucket, start_col] = pd.NaT
    df_copy.loc[mask_invalid_bucket, due_col] = pd.NaT
    
    # Remover coluna temporária
    df_copy = df_copy.drop(columns=['bucket_normalized'])
    
    return df_copy


def mask_bucket_dates(df: pd.DataFrame, 
                     bucket_col: str, 
                     date_cols: List[str]) -> pd.DataFrame:
    """
    Zera datas para registros em buckets inativos (backlog, a fazer).
    
    DEPRECATED: Esta função foi substituída por validate_dates_by_bucket()
    que é mais específica e robusta.
    
    Args:
        df: DataFrame para processar
        bucket_col: Nome da coluna de bucket
        date_cols: Lista de colunas de data para zerar
        
    Returns:
        DataFrame com datas mascaradas
    """
    # Manter por compatibilidade, mas usar nova validação
    return validate_dates_by_bucket(df, bucket_col)


def create_backup_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria colunas de backup para debug/auditoria.
    
    Args:
        df: DataFrame para processar
        
    Returns:
        DataFrame com colunas de backup
    """
    if 'Data de entrega' in df.columns:
        df['Due_raw'] = df['Data de entrega'].astype(str)
    
    return df


def fill_missing_start_dates(df: pd.DataFrame, 
                            start_col: str = 'Data de início',
                            due_col: str = 'Data de entrega',
                            days_before: int = 20) -> pd.DataFrame:
    """
    Preenche datas de início ausentes baseadas na data de entrega.
    
    Para registros que têm data de entrega mas não têm data de início,
    calcula data de início como: data_entrega - days_before dias.
    
    Args:
        df: DataFrame para processar
        start_col: Nome da coluna de data de início
        due_col: Nome da coluna de data de entrega
        days_before: Quantos dias antes da entrega para definir início
        
    Returns:
        DataFrame com datas de início preenchidas
    """
    if start_col not in df.columns or due_col not in df.columns:
        logging.warning(f"Colunas {start_col} ou {due_col} não encontradas. Pulando preenchimento de datas de início.")
        return df
    
    # Identificar registros que precisam de preenchimento
    mask_has_due = df[due_col].notna()
    mask_missing_start = df[start_col].isna()
    mask_to_fill = mask_has_due & mask_missing_start
    
    records_to_fill = mask_to_fill.sum()
    
    if records_to_fill == 0:
        logging.info("Nenhum registro precisa de data de início preenchida")
        return df
    
    logging.info(f"Preenchendo data de início para {records_to_fill} registros")
    logging.info(f"Regra: Data de início = Data de entrega - {days_before} dias")
    
    # Calcular novas datas de início
    df.loc[mask_to_fill, start_col] = df.loc[mask_to_fill, due_col] - pd.Timedelta(days=days_before)
    
    # Log de alguns exemplos
    if records_to_fill > 0:
        sample_data = df.loc[mask_to_fill, ['Título', start_col, due_col]].head(3)
        logging.debug("Exemplos de datas preenchidas:")
        for idx, row in sample_data.iterrows():
            logging.debug(f"  '{row['Título']}': Início={row[start_col].strftime('%d/%m/%Y')} "
                         f"-> Entrega={row[due_col].strftime('%d/%m/%Y')}")
    
    # Validar se as datas calculadas fazem sentido
    invalid_dates = df.loc[mask_to_fill, start_col] > df.loc[mask_to_fill, due_col]
    if invalid_dates.any():
        logging.warning(f"ATENÇÃO: {invalid_dates.sum()} datas de início ficaram posteriores à entrega!")
    
    return df


def log_processing_stats(df: pd.DataFrame) -> None:
    """
    Registra estatísticas do processamento.
    
    Args:
        df: DataFrame para analisar
    """
    total_rows = len(df)
    
    if 'Bucket' in df.columns:
        bucket_counts = df['Bucket'].str.strip().str.lower().value_counts()
        logging.info(f"Distribuição por bucket:")
        for bucket, count in bucket_counts.items():
            logging.info(f"  {bucket}: {count} registros")
    
    # Estatísticas de datas
    date_columns = ['Data de início', 'Data de entrega', 'Data de conclusão']
    for col in date_columns:
        if col in df.columns:
            filled_count = df[col].notna().sum()
            logging.info(f"  {col}: {filled_count}/{total_rows} preenchidas ({filled_count/total_rows*100:.1f}%)")
    
    logging.info(f"Total de registros processados: {total_rows}")


def generate_output_filename(output_dir: Path, 
                           prefix: str = "TarefasPlanner") -> Path:
    """
    Gera nome único para arquivo de saída.
    
    Args:
        output_dir: Diretório de saída
        prefix: Prefixo do nome do arquivo
        
    Returns:
        Caminho completo do arquivo de saída
    """
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{prefix}_{timestamp}.xlsx"
    return output_dir / filename


def save_dataframe(df: pd.DataFrame, output_path: Path) -> None:
    """
    Salva DataFrame como arquivo Excel.
    
    Args:
        df: DataFrame para salvar
        output_path: Caminho do arquivo de saída
    """
    try:
        df.to_excel(output_path, index=False, engine='openpyxl')
        logging.info(f"Arquivo salvo com sucesso: {output_path.name}")
        
    except Exception as e:
        logging.error(f"Erro ao salvar arquivo: {e}")
        raise


def main(input_path: Path, output_dir: Path) -> Optional[Path]:
    """
    Função principal do pipeline de processamento.
    
    Args:
        input_path: Caminho do arquivo de entrada
        output_dir: Diretório de saída
        
    Returns:
        Caminho do arquivo de saída gerado ou None em caso de erro
        
    Raises:
        Various exceptions relacionadas ao processamento
    """
    try:
        logging.info("Iniciando pipeline de processamento")
        
        # 1. Carregamento e renomeação
        df = load_and_rename(input_path)
        
        # 2. Validações
        validate_required_columns(df)
        
        # 3. Limpeza inicial
        df = clean_string_columns(df)
        
        # 4. Backup para debug
        df = create_backup_columns(df)
        
        # 5. Pipeline de transformações usando method chaining
        logging.info("Aplicando transformações...")
        
        df = (df
              .pipe(fill_default_values)
              .pipe(
                  validate_dates_by_bucket,
                  bucket_col='Bucket'
              )
              .pipe(
                  parse_dates,
                  date_cols=PlannerConfig.DATE_COLUMNS
              )
              .pipe(
                  fill_missing_start_dates,
                  start_col='Data de início',
                  due_col='Data de entrega',
                  days_before=PlannerConfig.START_DATE_FILL_DAYS
              )
              .pipe(
                  debug_missing_due,
                  bucket_col='Bucket',
                  due_raw_col='Due_raw',
                  due_parsed_col='Data de entrega'
              ))
        
        # 6. Estatísticas e logs
        log_processing_stats(df)
        
        # 7. Salvamento
        output_path = generate_output_filename(output_dir)
        save_dataframe(df, output_path)
        
        logging.info("Pipeline concluído com sucesso")
        
        return output_path
        
    except Exception as e:
        logging.error(f"Erro no pipeline: {e}")
        raise