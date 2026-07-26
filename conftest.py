import sys
from pathlib import Path

# Workspace FS does not support __pycache__ dirs.
sys.dont_write_bytecode = True

# Ensure the project root is on sys.path so `notebooks` is importable as a package.
sys.path.insert(0, str(Path(__file__).parent))
