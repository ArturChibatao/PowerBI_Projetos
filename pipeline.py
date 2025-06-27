from pathlib import Path
import logging
import pandas as pd

from parser import debug_missing_due, parse_dates

logger = logging.getLogger(__name__)

RENAME_MAP = {
    "Identificação da tarefa": "ID da Tarefa",
    "Nome da tarefa": "Título",
    "Nome do Bucket": "Bucket",
    "Progresso": "Progresso (%)",
    "Prioridade": "Prioridade",
    "Atribuído a": "Atribuído",
    "Criado por": "Criado por",
    "Criado em": "Data de criação",
    "Data de início": "Data de início",
    "Data de conclusão": "Data de entrega",
    "Concluído em": "Data de conclusão",
}

def load_and_rename(input_path: Path) -> pd.DataFrame:
    """Carrega o Excel e aplica ``RENAME_MAP`` às colunas."""

    df = pd.read_excel(input_path)
    logger.debug("colunas originais: %s", df.columns.tolist())

    df = df.rename(columns=RENAME_MAP)
    logger.debug("colunas renomeadas: %s", df.columns.tolist())

    return df


def mask_bucket_dates(
    df: pd.DataFrame, bucket_col: str, date_cols: list[str]
) -> pd.DataFrame:
    """Zera as ``date_cols`` quando o bucket for 'backlog' ou 'a fazer'."""

    mask = df[bucket_col].str.strip().str.lower().isin(["backlog", "a fazer"])
    for col in date_cols:
        df.loc[mask, col] = pd.NaT
    return df


def main(input_path: Path, output_dir: Path) -> None:
    """Executa o pipeline de processamento das tarefas."""

    df = load_and_rename(input_path)

    if "Data de entrega" in df.columns:
        df["Due_raw"] = df["Data de entrega"].astype(str)
    else:
        raise KeyError("Não encontrei a coluna 'Data de entrega' após o rename!")

    mask_exec_before = df["Bucket"].str.strip().str.lower() == "execução"
    logger.debug("Total Execução ANTES do parse: %s", mask_exec_before.sum())
    logger.debug(
        "\n%s", df.loc[mask_exec_before, ["Título", "Bucket", "Data de entrega"]].head()
    )

    df = (
        df.fillna({"Atribuído": "Michael Oneil; Artur Almeida"})
        .pipe(
            mask_bucket_dates,
            bucket_col="Bucket",
            date_cols=["Data de início", "Data de entrega", "Data de conclusão"],
        )
        .pipe(
            parse_dates,
            date_cols=[
                "Data de criação",
                "Data de início",
                "Data de entrega",
                "Data de conclusão",
            ],
        )
        .pipe(
            debug_missing_due,
            bucket_col="Bucket",
            due_raw_col="Due_raw",
            due_parsed_col="Data de entrega",
        )
    )

    outfile = output_dir / f"TarefasPlanner_{pd.Timestamp.now():%Y%m%d_%H%M%S}.xlsx"
    df.to_excel(outfile, index=False)
    logger.info("Arquivo salvo em: %s", outfile)

