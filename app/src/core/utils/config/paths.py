import os
from dotenv import load_dotenv

def find_project_root():
    marker_file = ".env"
    current_dir = os.path.abspath(os.path.dirname(__file__))

    while current_dir != os.path.dirname(current_dir): 
        if os.path.exists(os.path.join(current_dir, marker_file)):
            return current_dir
        current_dir = os.path.dirname(current_dir)

    raise RuntimeError("Can't find project root.")

load_dotenv()
LOG_DIR = os.getenv("LOG_DIR","")
ROOT_PATH = find_project_root()