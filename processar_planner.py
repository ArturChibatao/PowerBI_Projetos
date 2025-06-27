# processar_planner.py
from pathlib import Path
import pipeline

if __name__ == "__main__":
    BASE_DIR    = Path(r"C:\Users\artur.almeida\Documents\progs\PowerBi_Projetos")
    INPUT_DIR   = BASE_DIR / "inputs"
    OUTPUT_DIR  = BASE_DIR / "outputs"
    INPUT_PATH  = INPUT_DIR / "TarefasPlanner_Junho.xlsx"
    OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

    pipeline.main(input_path=INPUT_PATH, output_dir=OUTPUT_DIR)
