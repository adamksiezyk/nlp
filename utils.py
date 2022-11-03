import functools
import pandas as pd
import os
import re
from typing import Any, Callable


Processor = Callable[[str], Any]

FILES_PATH = "ustawy"


def read_documents() -> dict[str, str]:
    file_names = os.listdir(FILES_PATH)
    return {
        name: _read_document(name, FILES_PATH)
        for name in file_names
        if name.endswith(".txt")
    }


def read_normalized_documents() -> dict[str, str]:
    file_names = os.listdir(FILES_PATH)
    return {
        name: _normalize_document(_read_document(name, FILES_PATH))
        for name in file_names
        if name.endswith(".txt")
    }


def process(*processors: Processor) -> pd.DataFrame:
    file_names = os.listdir(FILES_PATH)
    result = []
    for name in file_names:
        if name.endswith(".txt"):
            result.append([name, *_process_document(_normalize_document(_read_document(name, FILES_PATH)), *processors)])
    return pd.DataFrame(result)


def _read_document(name: str, path: str) -> str:
    with open(os.path.join(path, name), 'r') as f:
        return f.read()


def _normalize_document(document: str) -> str:
    return re.sub(r"\s+", " ", document).lower()


def _process_document(document: str, *processors: Processor) -> list:
    return [p(document) for p in processors]
