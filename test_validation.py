"""
Módulo de validação e teste para pipeline do Planner.

Este módulo compara arquivos de entrada e saída para identificar:
- Perda de dados importantes (datas, IDs, etc.)
- Mudanças inesperadas nos dados
- Integridade do processamento
- Estatísticas de transformação
"""

import logging
import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass
import numpy as np


@dataclass
class ValidationResult:
    """Resultado de uma validação específica."""
    test_name: str
    passed: bool
    message: str
    details: Dict[str, Any] = None
    severity: str = "INFO"


class PlannerValidator:
    """Classe principal para validação do pipeline do Planner."""
    
    def __init__(self, input_file: Path, output_file: Path):
        """
        Inicializa o validador.
        
        Args:
            input_file: Caminho do arquivo de entrada
            output_file: Caminho do arquivo de saída
        """
        self.input_file = input_file
        self.output_file = output_file
        self.input_df = None
        self.output_df = None
        self.results: List[ValidationResult] = []
        
    def load_files(self) -> bool:
        """
        Carrega os arquivos de entrada e saída.
        
        Returns:
            True se ambos os arquivos foram carregados com sucesso
        """
        try:
            logging.info("Carregando arquivos para validação...")
            
            # Carregar arquivo de entrada
            self.input_df = pd.read_excel(self.input_file)
            logging.info(f"Arquivo de entrada: {len(self.input_df)} linhas")
            
            # Carregar arquivo de saída
            self.output_df = pd.read_excel(self.output_file)
            logging.info(f"Arquivo de saída: {len(self.output_df)} linhas")
            
            return True
            
        except Exception as e:
            logging.error(f"Erro ao carregar arquivos: {e}")
            return False
    
    def validate_row_count(self) -> ValidationResult:
        """Valida se o número de linhas foi preservado."""
        input_rows = len(self.input_df)
        output_rows = len(self.output_df)
        
        if input_rows == output_rows:
            return ValidationResult(
                test_name="Contagem de Linhas",
                passed=True,
                message=f"✅ Número de linhas preservado: {input_rows}",
                details={"input_rows": input_rows, "output_rows": output_rows}
            )
        else:
            return ValidationResult(
                test_name="Contagem de Linhas",
                passed=False,
                message=f"❌ Linhas perdidas: {input_rows} → {output_rows} (diferença: {input_rows - output_rows})",
                details={"input_rows": input_rows, "output_rows": output_rows, "difference": input_rows - output_rows},
                severity="ERROR"
            )
    
    def validate_unique_ids(self) -> ValidationResult:
        """Valida se IDs únicos foram preservados."""
        # Tentar encontrar coluna de ID
        id_columns = ['ID da Tarefa', 'Identificação da tarefa', 'ID', 'Task ID']
        id_col = None
        
        for col in id_columns:
            if col in self.input_df.columns:
                id_col = col
                break
        
        if not id_col:
            return ValidationResult(
                test_name="IDs Únicos",
                passed=True,
                message="⚠️ Nenhuma coluna de ID encontrada para validar",
                severity="WARNING"
            )
        
        # Mapear coluna no output (pode ter sido renomeada)
        output_id_col = 'ID da Tarefa' if 'ID da Tarefa' in self.output_df.columns else id_col
        
        if output_id_col not in self.output_df.columns:
            return ValidationResult(
                test_name="IDs Únicos",
                passed=False,
                message=f"❌ Coluna de ID '{output_id_col}' não encontrada no arquivo de saída",
                severity="ERROR"
            )
        
        input_ids = set(self.input_df[id_col].dropna().astype(str))
        output_ids = set(self.output_df[output_id_col].dropna().astype(str))
        
        missing_ids = input_ids - output_ids
        extra_ids = output_ids - input_ids
        
        if not missing_ids and not extra_ids:
            return ValidationResult(
                test_name="IDs Únicos",
                passed=True,
                message=f"✅ Todos os {len(input_ids)} IDs preservados",
                details={"total_ids": len(input_ids)}
            )
        else:
            issues = []
            if missing_ids:
                issues.append(f"{len(missing_ids)} IDs perdidos")
            if extra_ids:
                issues.append(f"{len(extra_ids)} IDs novos")
            
            return ValidationResult(
                test_name="IDs Únicos",
                passed=False,
                message=f"❌ Problemas com IDs: {', '.join(issues)}",
                details={
                    "missing_ids": list(missing_ids)[:10],  # Primeiros 10
                    "extra_ids": list(extra_ids)[:10],
                    "missing_count": len(missing_ids),
                    "extra_count": len(extra_ids)
                },
                severity="ERROR"
            )
    
    def validate_critical_dates(self) -> ValidationResult:
        """Valida perda de datas importantes."""
        date_columns = {
            'Data de entrega': ['Data de conclusão', 'Data de entrega', 'Due Date'],
            'Data de início': ['Data de início', 'Start Date'],
            'Data de criação': ['Data de criação', 'Criado em', 'Created Date']
        }
        
        issues = []
        date_stats = {}
        
        for output_col, possible_input_cols in date_columns.items():
            # Encontrar coluna correspondente no input
            input_col = None
            for col in possible_input_cols:
                if col in self.input_df.columns:
                    input_col = col
                    break
            
            if not input_col or output_col not in self.output_df.columns:
                continue
            
            # Contar datas válidas
            input_valid = self.input_df[input_col].notna().sum()
            output_valid = self.output_df[output_col].notna().sum()
            
            date_stats[output_col] = {
                'input_valid': input_valid,
                'output_valid': output_valid,
                'difference': input_valid - output_valid
            }
            
            # Verificar perda significativa de datas
            if input_valid > 0:
                loss_percentage = ((input_valid - output_valid) / input_valid) * 100
                
                if output_col == 'Data de entrega' and loss_percentage > 5:
                    # Data de entrega é crítica - tolerância baixa
                    issues.append(f"Data de entrega: {loss_percentage:.1f}% perdida ({input_valid}→{output_valid})")
                elif loss_percentage > 20:
                    # Outras datas - tolerância maior
                    issues.append(f"{output_col}: {loss_percentage:.1f}% perdida ({input_valid}→{output_valid})")
        
        if issues:
            return ValidationResult(
                test_name="Datas Críticas",
                passed=False,
                message=f"❌ Perda significativa de datas: {'; '.join(issues)}",
                details=date_stats,
                severity="ERROR"
            )
        else:
            return ValidationResult(
                test_name="Datas Críticas",
                passed=True,
                message="✅ Datas importantes preservadas adequadamente",
                details=date_stats
            )
    
    def validate_bucket_rules(self) -> ValidationResult:
        """Valida se as regras de bucket foram aplicadas corretamente."""
        if 'Bucket' not in self.output_df.columns:
            return ValidationResult(
                test_name="Regras de Bucket",
                passed=True,
                message="⚠️ Coluna Bucket não encontrada",
                severity="WARNING"
            )
        
        # Buckets que NÃO devem ter datas de início/entrega
        invalid_buckets = ['backlog', 'a fazer']
        # Buckets que DEVEM poder ter datas
        valid_buckets = ['execução', 'aguardando validação', 'concluídos']
        
        bucket_issues = []
        
        # Verificar buckets inválidos com datas
        for bucket in invalid_buckets:
            mask = self.output_df['Bucket'].str.strip().str.lower() == bucket
            bucket_data = self.output_df[mask]
            
            if len(bucket_data) > 0:
                invalid_start = bucket_data['Data de início'].notna().sum()
                invalid_due = bucket_data['Data de entrega'].notna().sum()
                
                if invalid_start > 0:
                    bucket_issues.append(f"'{bucket}' tem {invalid_start} datas de início")
                if invalid_due > 0:
                    bucket_issues.append(f"'{bucket}' tem {invalid_due} datas de entrega")
        
        # Verificar se buckets válidos não perderam datas importantes
        exec_mask = self.output_df['Bucket'].str.strip().str.lower() == 'execução'
        exec_data = self.output_df[exec_mask]
        
        if len(exec_data) > 0:
            missing_due = exec_data['Data de entrega'].isna().sum()
            total_exec = len(exec_data)
            
            if missing_due > total_exec * 0.3:  # Mais de 30% sem data de entrega
                bucket_issues.append(f"Execução: {missing_due}/{total_exec} sem data de entrega ({missing_due/total_exec*100:.1f}%)")
        
        if bucket_issues:
            return ValidationResult(
                test_name="Regras de Bucket",
                passed=False,
                message=f"❌ Problemas com regras de bucket: {'; '.join(bucket_issues)}",
                details={"issues": bucket_issues},
                severity="ERROR"
            )
        else:
            return ValidationResult(
                test_name="Regras de Bucket",
                passed=True,
                message="✅ Regras de bucket aplicadas corretamente"
            )
    
    def validate_data_consistency(self) -> ValidationResult:
        """Valida consistência dos dados (datas lógicas, etc.)."""
        issues = []
        
        # Verificar datas de início após datas de entrega
        if 'Data de início' in self.output_df.columns and 'Data de entrega' in self.output_df.columns:
            both_dates = self.output_df[
                self.output_df['Data de início'].notna() & 
                self.output_df['Data de entrega'].notna()
            ]
            
            invalid_dates = both_dates[
                both_dates['Data de início'] > both_dates['Data de entrega']
            ]
            
            if len(invalid_dates) > 0:
                issues.append(f"{len(invalid_dates)} registros com data de início após data de entrega")
        
        # Verificar datas muito antigas ou futuras
        date_cols = ['Data de início', 'Data de entrega', 'Data de conclusão']
        current_year = pd.Timestamp.now().year
        
        for col in date_cols:
            if col in self.output_df.columns:
                dates = self.output_df[col].dropna()
                if len(dates) > 0:
                    old_dates = dates[dates.dt.year < current_year - 2].count()
                    future_dates = dates[dates.dt.year > current_year + 2].count()
                    
                    if old_dates > 0:
                        issues.append(f"{col}: {old_dates} datas muito antigas (>{current_year-2})")
                    if future_dates > 0:
                        issues.append(f"{col}: {future_dates} datas muito futuras (<{current_year+2})")
        
        if issues:
            return ValidationResult(
                test_name="Consistência de Dados",
                passed=False,
                message=f"⚠️ Problemas de consistência: {'; '.join(issues)}",
                details={"issues": issues},
                severity="WARNING"
            )
        else:
            return ValidationResult(
                test_name="Consistência de Dados",
                passed=True,
                message="✅ Dados consistentes"
            )
    
    def generate_statistics(self) -> Dict[str, Any]:
        """Gera estatísticas comparativas detalhadas."""
        stats = {
            "input_summary": {
                "rows": len(self.input_df),
                "columns": len(self.input_df.columns)
            },
            "output_summary": {
                "rows": len(self.output_df),
                "columns": len(self.output_df.columns)
            }
        }
        
        # Estatísticas de datas
        date_columns = ['Data de início', 'Data de entrega', 'Data de conclusão']
        stats["date_statistics"] = {}
        
        for col in date_columns:
            if col in self.output_df.columns:
                filled = self.output_df[col].notna().sum()
                total = len(self.output_df)
                stats["date_statistics"][col] = {
                    "filled": filled,
                    "total": total,
                    "percentage": (filled / total * 100) if total > 0 else 0
                }
        
        # Distribuição por bucket
        if 'Bucket' in self.output_df.columns:
            bucket_dist = self.output_df['Bucket'].value_counts().to_dict()
            stats["bucket_distribution"] = bucket_dist
        
        return stats
    
    def run_all_validations(self) -> bool:
        """
        Executa todas as validações.
        
        Returns:
            True se todas as validações críticas passaram
        """
        if not self.load_files():
            return False
        
        logging.info("Executando validações...")
        
        # Lista de validações a executar
        validations = [
            self.validate_row_count,
            self.validate_unique_ids,
            self.validate_critical_dates,
            self.validate_bucket_rules,
            self.validate_data_consistency
        ]
        
        # Executar cada validação
        for validation_func in validations:
            try:
                result = validation_func()
                self.results.append(result)
            except Exception as e:
                error_result = ValidationResult(
                    test_name=validation_func.__name__,
                    passed=False,
                    message=f"❌ Erro durante validação: {e}",
                    severity="ERROR"
                )
                self.results.append(error_result)
        
        # Gerar estatísticas
        stats = self.generate_statistics()
        
        # Exibir resultados
        self._display_results(stats)
        
        # Retornar se todas as validações críticas passaram
        critical_failures = [r for r in self.results if not r.passed and r.severity in ["ERROR", "CRITICAL"]]
        return len(critical_failures) == 0
    
    def _display_results(self, stats: Dict[str, Any]) -> None:
        """Exibe os resultados das validações."""
        logging.info("="*60)
        logging.info("RELATÓRIO DE VALIDAÇÃO")
        logging.info("="*60)
        
        # Resumo dos testes
        total_tests = len(self.results)
        passed_tests = len([r for r in self.results if r.passed])
        
        logging.info(f"Testes executados: {total_tests}")
        logging.info(f"Testes aprovados: {passed_tests}")
        logging.info(f"Taxa de sucesso: {passed_tests/total_tests*100:.1f}%")
        logging.info("")
        
        # Resultados detalhados
        for result in self.results:
            logging.info(f"{result.test_name}: {result.message}")
        
        logging.info("")
        logging.info("ESTATÍSTICAS:")
        
        # Estatísticas de datas
        if "date_statistics" in stats:
            logging.info("Preenchimento de datas:")
            for col, data in stats["date_statistics"].items():
                logging.info(f"  {col}: {data['filled']}/{data['total']} ({data['percentage']:.1f}%)")
        
        # Distribuição por bucket
        if "bucket_distribution" in stats:
            logging.info("Distribuição por bucket:")
            for bucket, count in stats["bucket_distribution"].items():
                logging.info(f"  {bucket}: {count} registros")
        
        logging.info("="*60)


def main(input_file: Path, output_file: Path) -> bool:
    """
    Função principal de validação.
    
    Args:
        input_file: Arquivo de entrada
        output_file: Arquivo de saída
        
    Returns:
        True se validação passou
    """
    validator = PlannerValidator(input_file, output_file)
    return validator.run_all_validations()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 3:
        print("Uso: python test_validation.py <arquivo_entrada> <arquivo_saida>")
        sys.exit(1)
    
    input_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2])
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    success = main(input_path, output_path)
    sys.exit(0 if success else 1)