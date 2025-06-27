import logging
import pandas as pd

logger = logging.getLogger(__name__)

def parse_dates(df: pd.DataFrame, date_cols: list[str]) -> pd.DataFrame:
    """Converte colunas em ``date_cols`` para ``datetime``.

    Utiliza ``dayfirst=True`` para interpretar datas no formato DD/MM/YYYY.
    """
    df[date_cols] = df[date_cols].apply(
        pd.to_datetime, dayfirst=True, errors="coerce"
    )
    return df

def debug_missing_due(
    df: pd.DataFrame, bucket_col: str, due_raw_col: str, due_parsed_col: str
) -> pd.DataFrame:
    """Registra linhas em que ``due_raw_col`` não pôde ser convertido."""

    mask_exec = df[bucket_col].str.strip().str.lower() == "execução"
    mask_raw_notnull = df[due_raw_col].notna() & (df[due_raw_col] != "nan")
    mask_parsed_null = df[due_parsed_col].isna()
    problem = mask_exec & mask_raw_notnull & mask_parsed_null

    logger.debug(
        "Execução com %s NÃO-NULL mas parse falhou: %s linhas",
        due_raw_col,
        problem.sum(),
    )
    if problem.any():
        logger.debug("\n%s", df.loc[problem, ["Título", due_raw_col]])
    return df
