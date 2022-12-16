import time
import pandas as pd
import os
import re
import requests
from typing import Any, Callable
from xml.etree import ElementTree as ET


Processor = Callable[[str], Any]

FILES_PATH = "ustawy"
CLARIN_URL = "https://ws.clarin-pl.eu/nlprest2/base/"
CLARIN_USER = "adamksiezyk@student.agh.edu.pl"


def read_documents(path: str = FILES_PATH) -> dict[str, str]:
    file_names = os.listdir(path)
    return {
        name: _read_document(name, path)
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


def parse_analysis(xml_str: str) -> list[str]:
    tags = []
    root = ET.fromstring(xml_str)
    for lex in root.findall("./chunk/sentence/tok"):
        base = lex.find("lex/base").text
        ctag = lex.find("lex/ctag").text.split(':')[0]
        tags.append(f"{base}:{ctag}")
    return tags


def get_base_text(xml_str: str) -> str:
    tags = []
    root = ET.fromstring(xml_str)
    for lex in root.findall("./chunk/sentence/tok"):
        tags.append(lex.find("lex/base").text)
    return ' '.join(tags)


def clarine(text: str, lpmn: str, user: str = CLARIN_USER) -> str:
    res_upload = requests.post(f"{CLARIN_URL}/upload",
                               data=text.encode('utf-8'),
                               headers={'Content-Type': "binary/octet-stream"})
    res_upload.raise_for_status()
    file_id = res_upload.text

    res_task = requests.post(f"{CLARIN_URL}/startTask",
                             json={
                                 'user': user,
                                 'lpmn': lpmn,
                                 'file': file_id
                             })
    res_task.raise_for_status()
    task_id = res_task.text

    while True:
        res_status = requests.get(f"{CLARIN_URL}/getStatus/{task_id}")
        res_status.raise_for_status()
        res_status_json = res_status.json()
        print(res_status_json)
        if res_status_json['status'] == "ERROR":
            raise RuntimeError(res_status_json)
        if res_status_json['status'] == "DONE":
            break
        time.sleep(5)
    output_file_id = res_status_json['value'][0]['fileID']

    res_download = requests.get(f"{CLARIN_URL}/download{output_file_id}")
    res_download.raise_for_status()
    return res_download.text
