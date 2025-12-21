from bootcamp_data.config import make_paths
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

print(make_paths(ROOT))