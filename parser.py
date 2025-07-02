#!/usr/bin/env python3
"""
Módulo de parsing e processamento de datas para dados do Planner.

Este módulo contém funções especializadas para conversão de datas e
diagnóstico de problemas de parsing em dados do Microsoft Planner.
"""

import logging
import re
from datetime import datetime
from typing import List, Dict, Optional, Tuple

import pandas as pd


class DateParsingConfig:
    """Configurações para parsing de datas."""
    
    # Formatos de data comuns do Planner
    DATE_FORMATS = [
        '%d/%m/%Y',
        '%d/%m/%Y %H:%M',
        '%d/%m/%Y %H:%M:%S',
        '%Y-%m-%d',
        '%Y-%m-%d %H:%M:%S',
        '%d-%m-%Y',
        '%d.%m.%Y',
    ]
    
    # Padrões regex para validação
    DATE_PATTERNS = {
        'dd/mm/yyyy': r'^\d{1,2}/\d{1,2}/\d{4}$',
        'dd/mm/yyyy_time': r'^\d{1,2}/\d{1,2}/\d{4}\s\d{1,2}:\d{1,2}',
        'yyyy-mm-dd': r'^\d{4}-\d{1,2}-\d{1,2}',
        'dd-mm-yyyy': r'^\d{1,2}-\d{1,2}-\d{4}',
        'dd.mm.yyyy': r'^\d{1,2}\.\d{1,2}\.\d{4}',
    }


def validate_date_string(date_str: str) -> bool:
    """
    Valida se uma string tem formato de data reconhecido.
    
    Args:
        date_str: String para validar
        
    Returns:
        True se string parece ser uma data válida
    """
    if not isinstance(date_str, str) or not date_str.strip():
        return False
    
    date_str = date_str.strip()
    
    # Verificar padrões conhecidos
    for pattern_name, pattern in DateParsingConfig.DATE_PATTERNS.items():
        if re.match(pattern, date_str):
            return True
    
    return False


def parse_single_date(date_value: any, 
                     dayfirst: bool = True, 
                     strict: bool = False) -> Optional[pd.Timestamp]:
    """
    Tenta parsear uma única data usando múltiplos formatos.
    
    Args:
        date_value: Valor para parsear (string, datetime, etc.)
        dayfirst: Se True, assume formato DD/MM/YYYY
        strict: Se True, falha em formatos ambíguos
        
    Returns:
        pd.Timestamp se parsing bem-sucedido, None caso contrário
    """
    if pd.isna(date_value) or date_value in ['nan', 'NaT', '', None]:
        return None
    
    # Já é datetime
    if isinstance(date_value, (datetime, pd.Timestamp)):
        return pd.Timestamp(date_value)
    
    # Converter para string
    date_str = str(date_value).strip()
    
    if not validate_date_string(date_str):
        logging.debug(f"Formato de data não reconhecido: '{date_str}'")
        return None
    
    # Tentar pandas built-in primeiro
    try:
        return pd.to_datetime(date_str, dayfirst=dayfirst, errors='raise')
    except:
        pass
    
    # Tentar formatos específicos
    for fmt in DateParsingConfig.DATE_FORMATS:
        try:
            parsed = datetime.strptime(date_str, fmt)
            return pd.Timestamp(parsed)
        except ValueError:
            continue
    
    logging.debug(f"Falha ao parsear data: '{date_str}'")
    return None


def analyze_date_column(df: pd.DataFrame, col: str) -> Dict:
    """
    Analisa uma coluna de datas e retorna estatísticas.
    
    Args:
        df: DataFrame para analisar
        col: Nome da coluna
        
    Returns:
        Dicionário com estatísticas da coluna
    """
    if col not in df.columns:
        return {'error': f'Coluna {col} não encontrada'}
    
    series = df[col]
    total = len(series)
    
    # Contar diferentes tipos de valores
    null_count = series.isna().sum()
    empty_count = (series.astype(str).str.strip() == '').sum()
    nan_string_count = (series.astype(str).str.lower() == 'nan').sum()
    
    valid_data = total - null_count - empty_count - nan_string_count
    
    # Analisar formatos de data
    sample_values = series.dropna().astype(str).str.strip()
    sample_values = sample_values[sample_values != 'nan'].head(10)
    
    formats_found = []
    for value in sample_values:
        for pattern_name, pattern in DateParsingConfig.DATE_PATTERNS.items():
            if re.match(pattern, value):
                formats_found.append(pattern_name)
                break
    
    return {
        'column': col,
        'total_rows': total,
        'null_values': null_count,
        'empty_values': empty_count,
        'nan_strings': nan_string_count,
        'valid_data': valid_data,
        'formats_detected': list(set(formats_found)),
        'sample_values': sample_values.tolist()
    }


def parse_dates(df: pd.DataFrame, 
               date_cols: List[str], 
               dayfirst: bool = True,
               verbose: bool = True) -> pd.DataFrame:
    """
    Converte colunas de data de strings para datetime com análise detalhada.
    
    Args:
        df: DataFrame para processar
        date_cols: Lista de colunas de data para converter
        dayfirst: Se True, assume formato DD/MM/YYYY
        verbose: Se True, registra estatísticas detalhadas
        
    Returns:
        DataFrame com colunas de data convertidas
    """
    df_copy = df.copy()
    parsing_stats = {}
    
    logging.info(f"Convertendo {len(date_cols)} colunas de data")
    
    for col in date_cols:
        if col not in df_copy.columns:
            continue
        
        # Backup da coluna original
        backup_col = f"{col}_original"
        df_copy[backup_col] = df_copy[col].astype(str)
        
        # Parsing usando pandas built-in
        original_values = df_copy[col].copy()
        df_copy[col] = pd.to_datetime(df_copy[col], dayfirst=dayfirst, errors='coerce')
        
        # Contar sucessos e falhas
        successful = df_copy[col].notna().sum()
        failed = df_copy[col].isna().sum() - original_values.isna().sum()
        
        parsing_stats[col] = {
            'successful': successful,
            'failed': failed,
            'success_rate': successful / (successful + failed) * 100 if (successful + failed) > 0 else 0
        }
        
        if verbose and failed > 0:
            logging.warning(f"{col}: {failed} datas falharam no parsing")
        
        # Remover coluna de backup se parsing foi 100% bem-sucedido
        if failed == 0:
            df_copy = df_copy.drop(columns=[backup_col])
    
    return df_copy


def debug_missing_due(df: pd.DataFrame, 
                     bucket_col: str, 
                     due_raw_col: str, 
                     due_parsed_col: str,
                     target_bucket: str = 'execução') -> pd.DataFrame:
    """
    Identifica e reporta problemas de parsing de datas para buckets especÍficos.
    
    Args:
        df: DataFrame para analisar
        bucket_col: Nome da coluna de bucket
        due_raw_col: Nome da coluna com dados originais
        due_parsed_col: Nome da coluna com dados parseados
        target_bucket: Bucket para focar a análise
        
    Returns:
        DataFrame original (sem modificações)
    """
    try:
        # Máscaras para identificar problemas
        mask_target_bucket = (df[bucket_col]
                             .str.strip()
                             .str.lower() == target_bucket.lower())
        
        mask_raw_has_data = (df[due_raw_col].notna() & 
                            (df[due_raw_col].astype(str).str.strip() != '') &
                            (df[due_raw_col].astype(str).str.lower() != 'nan'))
        
        mask_parsed_failed = df[due_parsed_col].isna()
        
        # Combinar máscaras para encontrar problemas
        problem_mask = mask_target_bucket & mask_raw_has_data & mask_parsed_failed
        
        problem_count = problem_mask.sum()
        
        if problem_count > 0:
            logging.warning(f"Encontrados {problem_count} registros com problemas de parsing em '{target_bucket}'")
        
    except Exception as e:
        logging.error(f"Erro durante análise de problemas de parsing: {e}")
    
    return df


def validate_parsed_dates(df: pd.DataFrame, 
                         date_cols: List[str],
                         min_year: int = 2020,
                         max_year: int = 2030) -> Dict:
    """
    Valida se as datas parseadas estão em faixas razoáveis.
    
    Args:
        df: DataFrame para validar
        date_cols: Colunas de data para validar
        min_year: Ano mínimo aceitável
        max_year: Ano máximo aceitável
        
    Returns:
        Dicionário com estatísticas de validação
    """
    validation_results = {}
    
    for col in date_cols:
        if col not in df.columns:
            continue
        
        date_series = df[col].dropna()
        if date_series.empty:
            validation_results[col] = {'status': 'empty'}
            continue
        
        # Verificar faixa de anos
        years = date_series.dt.year
        invalid_years = ((years < min_year) | (years > max_year)).sum()
        
        # Verificar datas futuras (para algumas colunas)
        today = pd.Timestamp.now()
        future_dates = (date_series > today).sum()
        
        validation_results[col] = {
            'total_dates': len(date_series),
            'invalid_years': invalid_years,
            'future_dates': future_dates,
            'year_range': f"{years.min()}-{years.max()}",
            'status': 'valid' if invalid_years == 0 else 'has_issues'
        }
        
        if invalid_years > 0:
            logging.warning(f"Coluna {col}: {invalid_years} datas com anos inválidos")
        
        if future_dates > 0 and col in ['Data de criação', 'Data de conclusão']:
            logging.warning(f"Coluna {col}: {future_dates} datas futuras (pode ser normal)")
    
    return validation_results