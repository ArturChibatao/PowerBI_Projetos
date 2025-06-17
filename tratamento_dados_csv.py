from pathlib import Path
import pandas as pd


class YearlyDataMerger:
    """
    Carrega múltiplos CSVs, converte uma coluna de data e concatena só as linhas
    de um ano específico.
    """

    def __init__(
        self,
        file_paths: list[Path],
        date_col: str = "Data de abertura",
        sep: str = ";",
        date_format: str = "%d-%m-%Y %H:%M",
    ):
        """
        :param file_paths: lista de Paths para os CSVs de entrada
        :param date_col: nome da coluna de data
        :param sep: delimitador do CSV
        :param date_format: formato de parse do pandas.to_datetime
        """
        self.file_paths = file_paths
        self.date_col = date_col
        self.sep = sep
        self.date_format = date_format

    def _load_and_prepare(self, path: Path) -> pd.DataFrame:
        """Lê o CSV, converte a coluna de data e retorna o DataFrame."""
        df = pd.read_csv(path, sep=self.sep, engine="python")
        df[self.date_col] = pd.to_datetime(
            df[self.date_col],
            format=self.date_format,
            dayfirst=True,
        )
        return df

    def merge_year(self, year: int) -> pd.DataFrame:
        """
        Carrega cada CSV, filtra pelo ano que deseja e concatena tudo num só DF.
        :param year: ano a filtrar (ex: 2025)
        :return: DataFrame único com todas as entradas do ano
        """
        dfs_filtered = []
        for path in self.file_paths:
            df = self._load_and_prepare(path)
            dfs_filtered.append(df[df[self.date_col].dt.year == year])

        return pd.concat(dfs_filtered, ignore_index=True)


def main():

    base_dir = Path.home() / "Documents" / "progs" / "PowerBi_Projetos" / "inputs" / "dados_gerais"
    files = [base_dir / f"dados_1000-{i}.csv" for i in (1, 2)]
    merger = YearlyDataMerger(file_paths=files)
    df_2025 = merger.merge_year(2025)
    
    output_dir = base_dir.parent
    output_file = output_dir / "dados_2025.csv"
    df_2025.to_csv(output_file, sep=';', encoding='utf-8-sig', index=False)

    print(f"Arquivo com dados de 2025 salvo em: {output_file}")

    # Verifica quantos campos vazios possui na coluna 'Atribuído - Técnico'
    ser = df_2025['Atribuído - Técnico']
    n_nans = ser.isna().sum()
    n_blanks = ser.fillna("").astype(str).str.strip().eq("").sum()
    n_empty = n_nans + n_blanks
    print(f"Total de campos vazios em 'Atribuído - Técnico': {n_empty}")

if __name__ == "__main__":
    main()
