from __future__ import annotations
import argparse
import html
import json
import re
import shutil
from pathlib import Path
from typing import Iterable
import pandas as pd

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

def organize_raw_files(base_dir: Path) -> tuple[list[Path], list[Path]]:
    """
    Busca archivos JSON en las carpetas de Bronze y los prepara para procesar.
    base_dir debe ser: /opt/airflow/datalake
    """
    # Definimos dónde están los datos que bajaron los DAGs de Bronze
    api_bronze_dir = base_dir / "bronze" / "api"
    web_bronze_dir = base_dir / "bronze" / "webscraping"

    # Nos aseguramos de que existan para que no truene el script
    api_bronze_dir.mkdir(parents=True, exist_ok=True)
    web_bronze_dir.mkdir(parents=True, exist_ok=True)

    # Buscamos archivos .json directamente en esas carpetas
    api_files = sorted(api_bronze_dir.glob("*.json"))
    web_files = sorted(web_bronze_dir.glob("*.json"))

    return api_files, web_files

def _dedupe_paths(paths: Iterable[Path]) -> list[Path]:
    seen = set()
    deduped = []
    for p in paths:
        if p not in seen:
            deduped.append(p)
            seen.add(p)
    return deduped

def flatten_list_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if not df[col].empty:
            has_list = df[col].apply(lambda x: isinstance(x, list)).any()
            if has_list:
                df[col] = df[col].apply(
                    lambda x: ", ".join(map(str, x)) if isinstance(x, list) else x
                )
    return df

def strip_html_tags(text: str) -> str:
    if not isinstance(text, str): return ""
    if BeautifulSoup is not None:
        return BeautifulSoup(text, "html.parser").get_text(" ", strip=True)
    return re.sub(r"<[^>]+>", " ", text).strip()

def preprocess_api_file(file_path: Path, output_dir: Path) -> Path:
    with file_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    rows = payload.get("results", [])
    if not rows: # Si el JSON está vacío o no tiene resultados
        return None
        
    df = pd.DataFrame(rows)

    # Limpieza básica
    for col in ["preparationMinutes", "cookingMinutes"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(-1).astype("int32")

    df = flatten_list_columns(df)
    if "summary" in df.columns:
        df["summary"] = df["summary"].astype(str).apply(strip_html_tags)

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{file_path.stem}.parquet"
    df.to_parquet(output_path, index=False)
    return output_path

def get_english_stopwords() -> set[str]:
    import nltk
    from nltk.corpus import stopwords
    try:
        return set(stopwords.words("english"))
    except LookupError:
        nltk.download("stopwords", quiet=True)
        return set(stopwords.words("english"))

def clean_nlp_text(text: str, stop_words: set[str]) -> str:
    if not isinstance(text, str): return ""
    decoded = html.unescape(text)
    lowered = decoded.lower()
    no_punct = re.sub(r"[^\w\s]", " ", lowered)
    normalized_ws = re.sub(r"\s+", " ", no_punct).strip()
    filtered_tokens = [tok for tok in normalized_ws.split() if tok not in stop_words]
    return " ".join(filtered_tokens)

def preprocess_webscraping_file(file_path: Path, output_dir: Path, stop_words: set[str]) -> Path:
    with file_path.open("r", encoding="utf-8") as f:
        rows = json.load(f)

    df = pd.DataFrame(rows)
    if "article_summary" in df.columns:
        df["article_summary_clean"] = df["article_summary"].astype(str).apply(
            lambda txt: clean_nlp_text(txt, stop_words)
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{file_path.stem}.parquet"
    df.to_parquet(output_path, index=False)
    return output_path

def main() -> None:
    parser = argparse.ArgumentParser(description="Procesamiento Capa Silver")
    parser.add_argument("--base-dir", default="/opt/airflow/datalake", help="Datalake root")
    args, unknown = parser.parse_known_args()

    base_dir = Path(args.base_dir).resolve()

    # 1. Localizar archivos en BRONZE
    api_files, web_files = organize_raw_files(base_dir)

    # 2. Definir carpetas de SILVER
    api_output_dir = base_dir / "silver" / "api"
    web_output_dir = base_dir / "silver" / "webscraping"

    # 3. Procesar API
    print(f"Procesando {len(api_files)} archivos de API...")
    for path in api_files:
        out = preprocess_api_file(path, api_output_dir)
        if out: print(f"- Creado: {out.name}")

    # 4. Procesar WebScraping
    stop_words = get_english_stopwords()
    print(f"Procesando {len(web_files)} archivos de WebScraping...")
    for path in web_files:
        out = preprocess_webscraping_file(path, web_output_dir, stop_words)
        if out: print(f"- Creado: {out.name}")

if __name__ == "__main__":
    main()