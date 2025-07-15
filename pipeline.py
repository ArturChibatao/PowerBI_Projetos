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
        'Identificação da tarefa': 'Quantidade de Tarefa',  # Mudança: ID -> Quantidade
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
    INACTIVE_BUCKETS = ['backlog', 'pendente']  # Mudança: 'a fazer' -> 'pendente'
    
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


def normalize_bucket_values(df: pd.DataFrame, bucket_col: str = 'Bucket') -> pd.DataFrame:
    """
    Normaliza e padroniza valores da coluna de bucket.
    
    Args:
        df: DataFrame para processar
        bucket_col: Nome da coluna de bucket
        
    Returns:
        DataFrame com valores de bucket normalizados
    """
    if bucket_col not in df.columns:
        logging.warning(f"Coluna '{bucket_col}' não encontrada - pulando normalização de buckets")
        return df
    
    df_copy = df.copy()
    
    # Mapeamento de valores antigos para novos
    bucket_mapping = {
        'A fazer': 'Pendente',
        'a fazer': 'Pendente',
        'A FAZER': 'Pendente',
        'pendente': 'Pendente',
        'PENDENTE': 'Pendente'
    }
    
    # Contar quantos registros serão afetados
    changes_count = 0
    for old_value, new_value in bucket_mapping.items():
        mask = df_copy[bucket_col].astype(str).str.strip() == old_value
        affected = mask.sum()
        if affected > 0:
            df_copy.loc[mask, bucket_col] = new_value
            changes_count += affected
            logging.info(f"Renomeado '{old_value}' → '{new_value}' ({affected} registros)")
    
    if changes_count == 0:
        logging.info("✅ Nenhum bucket precisou ser renomeado")
    else:
        logging.info(f"✅ Total de {changes_count} registros com buckets renomeados")
    
    return df_copy


def create_task_counter(df: pd.DataFrame, 
                       id_col: str = 'Quantidade de Tarefa') -> pd.DataFrame:
    """
    Substitui a coluna de ID por um contador sequencial de tarefas.
    
    Args:
        df: DataFrame para processar
        id_col: Nome da coluna que será o contador
        
    Returns:
        DataFrame com coluna de contador sequencial
    """
    df_copy = df.copy()
    
    # Verificar se a coluna existe
    if id_col not in df_copy.columns:
        logging.warning(f"Coluna '{id_col}' não encontrada - pulando criação do contador")
        return df_copy
    
    # Criar numeração sequencial começando do 1
    total_tarefas = len(df_copy)
    df_copy[id_col] = range(1, total_tarefas + 1)
    
    logging.info(f"Criado contador sequencial: 1 a {total_tarefas} tarefas")
    logging.info(f"Total de projetos/tarefas catalogados: {total_tarefas}")
    
    return df_copy


def fill_default_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preenche valores vazios com padrões definidos.
    
    Args:
        df: DataFrame para preencher
        
    Returns:
        DataFrame com valores padrão aplicados
    """
    df_copy = df.copy()
    
    # Verificar se coluna 'Atribuído' existe
    if 'Atribuído' not in df_copy.columns:
        logging.warning("Coluna 'Atribuído' não encontrada - pulando preenchimento")
        return df_copy
    
    # Contar valores vazios antes do preenchimento
    empty_before = df_copy['Atribuído'].isna().sum()
    
    # Também considerar strings vazias e "nan"
    empty_strings = (df_copy['Atribuído'].astype(str).str.strip() == '').sum()
    nan_strings = (df_copy['Atribuído'].astype(str).str.lower() == 'nan').sum()
    
    total_empty = empty_before + empty_strings + nan_strings
    
    if total_empty > 0:
        logging.info(f"Preenchendo {total_empty} campos vazios na coluna 'Atribuído'")
        
        # Preencher valores nulos
        df_copy['Atribuído'] = df_copy['Atribuído'].fillna(PlannerConfig.DEFAULT_ASSIGNEE)
        
        # Preencher strings vazias
        mask_empty_strings = df_copy['Atribuído'].astype(str).str.strip() == ''
        df_copy.loc[mask_empty_strings, 'Atribuído'] = PlannerConfig.DEFAULT_ASSIGNEE
        
        # Preencher "nan" strings
        mask_nan_strings = df_copy['Atribuído'].astype(str).str.lower() == 'nan'
        df_copy.loc[mask_nan_strings, 'Atribuído'] = PlannerConfig.DEFAULT_ASSIGNEE
        
        # Verificar resultado
        empty_after = df_copy['Atribuído'].isna().sum()
        remaining_empty = (df_copy['Atribuído'].astype(str).str.strip() == '').sum()
        
        if empty_after == 0 and remaining_empty == 0:
            logging.info(f"✅ Todos os {total_empty} campos preenchidos com: '{PlannerConfig.DEFAULT_ASSIGNEE}'")
        else:
            logging.warning(f"⚠️ Ainda restam {empty_after + remaining_empty} campos vazios")
    else:
        logging.info("✅ Coluna 'Atribuído' já está completamente preenchida")
    
    return df_copy


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


def validate_start_vs_creation_dates(df: pd.DataFrame,
                                    creation_col: str = 'Data de criação',
                                    start_col: str = 'Data de início') -> pd.DataFrame:
    """
    Valida e corrige datas de criação baseadas na data de início.
    
    Se a data de início for menor que a data de criação, copia a data de início
    para a data de criação (ajusta a criação para ser compatível com o início planejado).
    
    Args:
        df: DataFrame para processar
        creation_col: Nome da coluna de data de criação
        start_col: Nome da coluna de data de início
        
    Returns:
        DataFrame com datas de criação corrigidas
    """
    if creation_col not in df.columns or start_col not in df.columns:
        logging.warning(f"Colunas '{creation_col}' ou '{start_col}' não encontradas - pulando validação de datas")
        return df
    
    df_copy = df.copy()
    
    # Debug: mostrar alguns valores antes da correção
    logging.info("Verificando datas de início vs criação...")
    
    # Encontrar registros onde ambas as datas existem
    mask_both_dates = (df_copy[creation_col].notna() & df_copy[start_col].notna())
    
    if mask_both_dates.sum() == 0:
        logging.info("✅ Nenhum registro com ambas as datas para validar")
        return df_copy
    
    logging.info(f"Analisando {mask_both_dates.sum()} registros com ambas as datas...")
    
    # Encontrar datas de início menores que datas de criação
    mask_invalid_dates = (
        mask_both_dates & 
        (df_copy[start_col] < df_copy[creation_col])
    )
    
    invalid_count = mask_invalid_dates.sum()
    total_with_dates = mask_both_dates.sum()
    
    # Debug: mostrar alguns exemplos ANTES da correção
    if mask_both_dates.sum() > 0:
        logging.info("Exemplos de datas ANTES da validação:")
        for idx in df_copy[mask_both_dates].index[:3]:
            creation_date = df_copy.loc[idx, creation_col]
            start_date = df_copy.loc[idx, start_col]
            title = df_copy.loc[idx, 'Título'] if 'Título' in df_copy.columns else f"Linha {idx}"
            
            if pd.notna(creation_date) and pd.notna(start_date):
                creation_str = creation_date.strftime('%d/%m/%Y') if hasattr(creation_date, 'strftime') else str(creation_date)
                start_str = start_date.strftime('%d/%m/%Y') if hasattr(start_date, 'strftime') else str(start_date)
                status = "❌ PRECISA CORREÇÃO" if start_date < creation_date else "✅ VÁLIDA"
                logging.info(f"  '{title}': Criação={creation_str}, Início={start_str} {status}")
    
    if invalid_count > 0:
        logging.info(f"🔧 CORRIGINDO {invalid_count} datas de criação baseadas no início planejado")
        
        # Aplicar correção: copiar data de INÍCIO para data de CRIAÇÃO
        df_copy.loc[mask_invalid_dates, creation_col] = df_copy.loc[mask_invalid_dates, start_col]
        
        # Mostrar exemplos APÓS a correção
        logging.info("Exemplos APÓS a correção:")
        for idx in df_copy[mask_invalid_dates].index[:3]:
            creation_date = df_copy.loc[idx, creation_col]  # Agora já corrigida
            start_date = df_copy.loc[idx, start_col]
            title = df_copy.loc[idx, 'Título'] if 'Título' in df_copy.columns else f"Linha {idx}"
            
            if pd.notna(creation_date) and pd.notna(start_date):
                creation_str = creation_date.strftime('%d/%m/%Y') if hasattr(creation_date, 'strftime') else str(creation_date)
                start_str = start_date.strftime('%d/%m/%Y') if hasattr(start_date, 'strftime') else str(start_date)
                logging.info(f"  '{title}': Criação={creation_str} ✅, Início={start_str} (CRIAÇÃO AJUSTADA)")
        
        # Validar se correção funcionou
        remaining_invalid = (
            (df_copy[creation_col].notna() & df_copy[start_col].notna()) &
            (df_copy[start_col] < df_copy[creation_col])
        ).sum()
        
        if remaining_invalid == 0:
            logging.info(f"✅ Todas as {invalid_count} datas de criação foram ajustadas com sucesso")
        else:
            logging.warning(f"⚠️ Ainda restam {remaining_invalid} datas inconsistentes após correção")
            
    else:
        logging.info(f"✅ Todas as {total_with_dates} datas estão consistentes (criação ≤ início)")
    
    return df_copy


def clean_backup_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove colunas de backup e temporárias desnecessárias.
    
    Args:
        df: DataFrame para limpar
        
    Returns:
        DataFrame sem colunas de backup
    """
    columns_to_remove = [
        'Due_raw',
        'Data de início_original', 
        'Data de entrega_original',
        'Data de conclusão_original'
    ]
    
    removed_columns = []
    df_copy = df.copy()
    
    for col in columns_to_remove:
        if col in df_copy.columns:
            df_copy = df_copy.drop(columns=[col])
            removed_columns.append(col)
    
    if removed_columns:
        logging.info(f"Removidas {len(removed_columns)} colunas de backup: {removed_columns}")
    else:
        logging.info("✅ Nenhuma coluna de backup encontrada para remover")
    
    return df_copy


def handle_null_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Trata valores nulos nas colunas de data substituindo por "NÃO DEFINIDO".
    
    Args:
        df: DataFrame para processar
        
    Returns:
        DataFrame com datas nulas tratadas
    """
    date_columns = ['Data de início', 'Data de entrega', 'Data de conclusão']
    df_copy = df.copy()
    
    total_nulls_replaced = 0
    
    for col in date_columns:
        if col in df_copy.columns:
            # Contar valores nulos antes da substituição
            null_count = df_copy[col].isna().sum()
            
            if null_count > 0:
                # Substituir valores nulos por "NÃO DEFINIDO"
                df_copy[col] = df_copy[col].fillna("NÃO DEFINIDO")
                total_nulls_replaced += null_count
                logging.info(f"  {col}: {null_count} valores nulos → 'NÃO DEFINIDO'")
    
    if total_nulls_replaced > 0:
        logging.info(f"✅ Total de {total_nulls_replaced} valores nulos substituídos por 'NÃO DEFINIDO'")
    else:
        logging.info("✅ Nenhum valor nulo encontrado nas colunas de data")
    
    return df_copy


def simplify_boolean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Simplifica colunas booleanas para formato "SIM"/"NÃO".
    
    Args:
        df: DataFrame para processar
        
    Returns:
        DataFrame com colunas booleanas simplificadas
    """
    boolean_columns = ['É Recorrente', 'Atrasados']
    df_copy = df.copy()
    
    simplified_count = 0
    
    for col in boolean_columns:
        if col in df_copy.columns:
            # Contar valores únicos antes da conversão
            unique_before = df_copy[col].nunique()
            
            # Converter valores booleanos para SIM/NÃO
            df_copy[col] = df_copy[col].map({
                True: "SIM",
                False: "NÃO",
                "True": "SIM",
                "False": "NÃO",
                "true": "SIM", 
                "false": "NÃO",
                1: "SIM",
                0: "NÃO",
                "1": "SIM",
                "0": "NÃO",
                "Yes": "SIM",
                "No": "NÃO",
                "yes": "SIM",
                "no": "NÃO"
            }).fillna("NÃO")  # Valores nulos ou não mapeados viram "NÃO"
            
            # Contar valores únicos após conversão
            unique_after = df_copy[col].nunique()
            
            simplified_count += 1
            logging.info(f"  {col}: {unique_before} valores únicos → {unique_after} ('SIM'/'NÃO')")
    
    if simplified_count > 0:
        logging.info(f"✅ {simplified_count} colunas booleanas simplificadas para 'SIM'/'NÃO'")
    else:
        logging.info("✅ Nenhuma coluna booleana encontrada para simplificar")
    
    return df_copy


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
    
    # Estatísticas de datas (agora incluindo "NÃO DEFINIDO")
    date_columns = ['Data de início', 'Data de entrega', 'Data de conclusão']
    for col in date_columns:
        if col in df.columns:
            # Contar valores que não são "NÃO DEFINIDO"
            defined_count = (df[col] != "NÃO DEFINIDO").sum()
            undefined_count = (df[col] == "NÃO DEFINIDO").sum()
            logging.info(f"  {col}: {defined_count} definidas, {undefined_count} não definidas ({defined_count/total_rows*100:.1f}% preenchidas)")
    
    # Estatísticas da coluna Atribuído
    if 'Atribuído' in df.columns:
        assigned_filled = df['Atribuído'].notna().sum()
        empty_strings = (df['Atribuído'].astype(str).str.strip() == '').sum()
        effective_filled = assigned_filled - empty_strings
        
        logging.info(f"  Atribuído: {effective_filled}/{total_rows} preenchidos ({effective_filled/total_rows*100:.1f}%)")
        
        if effective_filled < total_rows:
            logging.warning(f"⚠️ {total_rows - effective_filled} registros ainda sem atribuição")
    
    # Estatísticas de colunas booleanas
    boolean_columns = ['É Recorrente', 'Atrasados']
    for col in boolean_columns:
        if col in df.columns:
            sim_count = (df[col] == "SIM").sum()
            nao_count = (df[col] == "NÃO").sum()
            logging.info(f"  {col}: {sim_count} SIM, {nao_count} NÃO")
    
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
        
        # 4. Normalizar valores de bucket
        df = normalize_bucket_values(df)
        
        # 5. Criar contador de tarefas
        df = create_task_counter(df)
        
        # 6. Backup para debug
        df = create_backup_columns(df)
        
        # 7. Pipeline de transformações usando method chaining
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
                  validate_start_vs_creation_dates,
                  creation_col='Data de criação',
                  start_col='Data de início'
              )
              .pipe(
                  handle_null_dates
              )
              .pipe(
                  simplify_boolean_columns
              )
              .pipe(
                  clean_backup_columns
              ))
        
        # 8. Estatísticas e logs
        log_processing_stats(df)
        
        # 9. Salvamento
        output_path = generate_output_filename(output_dir)
        save_dataframe(df, output_path)
        
        logging.info("Pipeline concluído com sucesso")
        
        return output_path
        
    except Exception as e:
        logging.error(f"Erro no pipeline: {e}")
        raise