from pathlib import Path
import pandas as pd

from parser import parse_dates, debug_missing_due

def load_and_rename(input_path: Path) -> pd.DataFrame:
    """
    1) Carrega o Excel em um DataFrame
    2) Renomeia as colunas para os nomes em português
    3) Retorna o DataFrame pronto para o pipeline
    """
    df = pd.read_excel(input_path)

    print("[DEBUG] colunas originais:", df.columns.tolist())

    rename_map = {
        'Identificação da tarefa': 'ID da Tarefa',
        'Nome da tarefa'           : 'Título',
        'Nome do Bucket'           : 'Bucket',
        'Progresso'                : 'Progresso (%)',
        'Prioridade'               : 'Prioridade',
        'Atribuído a'              : 'Atribuído',
        'Criado por'               : 'Criado por',
        'Criado em'                : 'Data de criação',
        'Data de início'           : 'Data de início',
        'Data de conclusão'        : 'Data de entrega',
        'Concluído em'             : 'Data de conclusão',
    }

    df = df.rename(columns=rename_map)
    print("[DEBUG] colunas renomeadas:", df.columns.tolist())

    return df


def mask_bucket_dates(df: pd.DataFrame, bucket_col: str, date_cols: list[str]) -> pd.DataFrame:
    """
    Para todos os registros cujo df[bucket_col] for 'backlog' ou 'a fazer',
    zera (pd.NaT) as colunas em date_cols.
    """
    mask = df[bucket_col].str.strip().str.lower().isin(['backlog', 'a fazer'])
    for dc in date_cols:
        df.loc[mask, dc] = pd.NaT
    return df


def main(input_path: Path, output_dir: Path):
    # 1) load + rename
    df = load_and_rename(input_path)

    # 2) preservar raw da data de entrega (debug)
    if 'Data de entrega' in df.columns:
        df['Due_raw'] = df['Data de entrega'].astype(str)
    else:
        raise KeyError("Não encontrei a coluna 'Data de entrega' após o rename!")

    # 3) debug antes do parse
    mask_exec_before = df['Bucket'].str.strip().str.lower() == 'execução'
    print(f"[DEBUG] Total Execução ANTES do parse: {mask_exec_before.sum()}")
    print(df.loc[mask_exec_before, ['Título','Bucket','Data de entrega']].head())

    df = (
        df
        .fillna({'Atribuído': 'Michael Oneil; Artur Almeida'})
        .pipe(
            mask_bucket_dates,
            bucket_col='Bucket',
            date_cols=['Data de início','Data de entrega','Data de conclusão']
        )
        .pipe(
            parse_dates,
            date_cols=['Data de criação','Data de início','Data de entrega','Data de conclusão']
        )
        .pipe(
            debug_missing_due,
            bucket_col='Bucket',
            due_raw_col='Due_raw',
            due_parsed_col='Data de entrega'
        )
    )

    outfile = output_dir / f"TarefasPlanner_{pd.Timestamp.now():%Y%m%d_%H%M%S}.xlsx"
    df.to_excel(outfile, index=False)
    print("[INFO] Arquivo salvo em:", outfile)

