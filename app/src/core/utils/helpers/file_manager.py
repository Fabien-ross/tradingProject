
import json
from typing import Any, List, Dict


class FileManager:

    def load_json_file(
        self,
        filepath: str
    ):
        """Load a JSON file and return its content as a Python dict."""
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data