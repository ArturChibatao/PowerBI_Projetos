#!/usr/bin/env python3
"""
Teste específico para validação de datas.
"""

import pandas as pd
from pathlib import Path
import sys
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def test_date_validation(file_path: Path):
    """Testa a validação de datas especificamente."""
    
    print("="*60)
    print("TESTE DE VALIDAÇÃO DE DATAS")
    print("="*60)
    
    try:
        # Carregar arquivo
        df = pd.read_excel(file_path)
        print(f"Arquivo carregado: {len(df)} linhas")
        
        # Verificar se as colunas existem
        required_cols = ['Data de criação', 'Data de início']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            print(f"❌ Colunas faltando: {missing_cols}")
            print("Colunas disponíveis:")
            for col in df.columns:
                print(f"  - {col}")
            return
        
        print("✅ Colunas encontradas")
        
        # Converter para datetime se necessário
        for col in required_cols:
            if not pd.api.types.is_datetime64_any_dtype(df[col]):
                print(f"Convertendo {col} para datetime...")
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Encontrar registros com ambas as datas
        mask_both = df['Data de criação'].notna() & df['Data de início'].notna()
        both_count = mask_both.sum()
        
        print(f"\nRegistros com ambas as datas: {both_count}")
        
        if both_count == 0:
            print("Nenhum registro para testar")
            return
        
        # Verificar datas problemáticas
        mask_problem = (
            mask_both & 
            (df['Data de início'] < df['Data de criação'])
        )
        
        problem_count = mask_problem.sum()
        
        print(f"Registros com problema (início < criação): {problem_count}")
        
        if problem_count > 0:
            print("\nExemplos de problemas:")
            for idx in df[mask_problem].index[:5]:
                title = df.loc[idx, 'Título'] if 'Título' in df.columns else f"Linha {idx}"
                creation = df.loc[idx, 'Data de criação'].strftime('%d/%m/%Y %H:%M')
                start = df.loc[idx, 'Data de início'].strftime('%d/%m/%Y %H:%M')
                print(f"  {title}")
                print(f"    Criação: {creation}")
                print(f"    Início:  {start} ❌")
                print()
            
            # Aplicar correção
            print("Aplicando correção...")
            df_corrected = df.copy()
            # CORREÇÃO: copiar data de INÍCIO para data de CRIAÇÃO
            df_corrected.loc[mask_problem, 'Data de criação'] = df_corrected.loc[mask_problem, 'Data de início']
            
            print("Exemplos após correção:")
            for idx in df[mask_problem].index[:3]:
                title = df.loc[idx, 'Título'] if 'Título' in df.columns else f"Linha {idx}"
                creation = df_corrected.loc[idx, 'Data de criação'].strftime('%d/%m/%Y %H:%M')  # Agora corrigida
                start = df_corrected.loc[idx, 'Data de início'].strftime('%d/%m/%Y %H:%M')
                print(f"  {title}")
                print(f"    Criação: {creation} ✅ (AJUSTADA)")
                print(f"    Início:  {start}")
                print()
            
            # Verificar se correção funcionou
            remaining = (
                (df_corrected['Data de criação'].notna() & df_corrected['Data de início'].notna()) &
                (df_corrected['Data de início'] < df_corrected['Data de criação'])
            ).sum()
            
            if remaining == 0:
                print(f"✅ Correção bem-sucedida! {problem_count} registros corrigidos")
            else:
                print(f"❌ Ainda restam {remaining} problemas após correção")
        else:
            print("✅ Todas as datas estão corretas!")
            
    except Exception as e:
        print(f"Erro: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python test_date_validation.py <arquivo.xlsx>")
        sys.exit(1)
    
    file_path = Path(sys.argv[1])
    if not file_path.exists():
        print(f"Arquivo não encontrado: {file_path}")
        sys.exit(1)
    
    test_date_validation(file_path)