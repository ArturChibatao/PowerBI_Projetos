import pandas as pd

def parse_dates(df: pd.DataFrame, date_cols: list[str]) -> pd.DataFrame:
    """
    Converte as colunas em date_cols de strings para datetime,
    usando dayfirst=True para o formato DD/MM/YYYY.
    """
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
    return df

def debug_missing_due(df: pd.DataFrame, bucket_col: str, due_raw_col: str, due_parsed_col: str):
    """
    Imprime as linhas em que bucket='execução', o raw de due não era null
    mas falhou ao parsear, ficando NaT em due_parsed_col.
    """
    mask_exec = df[bucket_col].str.strip().str.lower() == 'execução'
    mask_raw_notnull = df[due_raw_col].notna() & (df[due_raw_col] != 'nan')
    mask_parsed_null = df[due_parsed_col].isna()
    problem = mask_exec & mask_raw_notnull & mask_parsed_null

    print(f"[DEBUG] Execução com {due_raw_col} NÃO-NULL mas parse falhou:", problem.sum(), "linhas")
    if problem.any():
        print(df.loc[problem, ['Título', due_raw_col]])
    return df
