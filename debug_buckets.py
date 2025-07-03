#!/usr/bin/env python3
"""
Script de debug para investigar problemas com buckets.
"""

import pandas as pd
from pathlib import Path
import sys

def debug_buckets(file_path: Path):
    """Debug dos buckets encontrados no arquivo."""
    
    print("="*60)
    print("DEBUG DE BUCKETS")
    print("="*60)
    
    # Carregar arquivo
    df = pd.read_excel(file_path)
    print(f"Arquivo carregado: {len(df)} linhas")
    print()
    
    # Encontrar coluna de bucket
    bucket_cols = [col for col in df.columns if 'bucket' in col.lower()]
    if not bucket_cols:
        bucket_cols = [col for col in df.columns if 'status' in col.lower()]
    
    if not bucket_cols:
        print("❌ Nenhuma coluna de bucket/status encontrada!")
        print("Colunas disponíveis:")
        for col in df.columns:
            print(f"  - {col}")
        return
    
    bucket_col = bucket_cols[0]
    print(f"Usando coluna: '{bucket_col}'")
    print()
    
    # Valores únicos
    unique_values = df[bucket_col].dropna().unique()
    print("VALORES ÚNICOS ENCONTRADOS:")
    for i, value in enumerate(unique_values, 1):
        count = (df[bucket_col] == value).sum()
        print(f"{i:2d}. '{value}' - {count} registros")
    print()
    
    # Normalização
    print("NORMALIZAÇÃO:")
    normalized_map = {}
    for value in unique_values:
        if pd.notna(value):
            normalized = (str(value).strip().lower()
                         .replace('ç', 'c')
                         .replace('ã', 'a')
                         .replace('í', 'i')
                         .replace('ú', 'u')
                         .replace('õ', 'o'))
            normalized_map[value] = normalized
            print(f"'{value}' -> '{normalized}'")
    print()
    
    # Buckets permitidos
    allowed = ['execução', 'aguardando validação', 'concluídos', 'concluido', 'concluida', 'concluidas']
    allowed_normalized = []
    for bucket in allowed:
        norm = (bucket.lower()
               .replace('ç', 'c')
               .replace('ã', 'a')
               .replace('í', 'i')
               .replace('ú', 'u')
               .replace('õ', 'o')
               .replace('ê', 'e')
               .replace('é', 'e')
               .replace('á', 'a')
               .replace('ó', 'o'))
        allowed_normalized.append(norm)
    
    print("BUCKETS PERMITIDOS (normalizados):")
    for bucket in allowed_normalized:
        print(f"  - '{bucket}'")
    print()
    
    # Verificar compatibilidade
    print("COMPATIBILIDADE:")
    for original, normalized in normalized_map.items():
        if normalized in allowed_normalized:
            status = "✅ PERMITIDO"
        else:
            status = "❌ SERÁ REMOVIDO"
        print(f"'{original}' -> {status}")
    print()
    
    # Verificar datas
    date_cols = [col for col in df.columns if 'data' in col.lower() and ('início' in col.lower() or 'entrega' in col.lower())]
    if date_cols:
        print("ANÁLISE DE DATAS POR BUCKET:")
        for original in unique_values:
            if pd.notna(original):
                bucket_data = df[df[bucket_col] == original]
                normalized = normalized_map[original]
                will_keep = normalized in allowed_normalized
                
                print(f"\nBucket: '{original}' ({len(bucket_data)} registros)")
                print(f"Normalizado: '{normalized}'")
                print(f"Status: {'✅ Datas mantidas' if will_keep else '❌ Datas serão removidas'}")
                
                for date_col in date_cols:
                    if date_col in df.columns:
                        filled = bucket_data[date_col].notna().sum()
                        print(f"  {date_col}: {filled}/{len(bucket_data)} preenchidas")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python debug_buckets.py <arquivo.xlsx>")
        sys.exit(1)
    
    file_path = Path(sys.argv[1])
    if not file_path.exists():
        print(f"Arquivo não encontrado: {file_path}")
        sys.exit(1)
    
    debug_buckets(file_path)