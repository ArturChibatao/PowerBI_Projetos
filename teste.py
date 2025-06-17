import pandas as pd

file_path = 'C:\\Users\\artur.almeida\\Documents\\progs\\PowerBi_Porto\\inputs\\glpi.csv'
df = pd.read_csv(file_path)
print(df.head(10))
