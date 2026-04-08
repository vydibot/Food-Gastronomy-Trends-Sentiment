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
except ImportError:  # pragma: no cover
    BeautifulSoup = None


def organize_raw_files(base_dir: Path) -> tuple[list[Path], list[Path]]:
    """Move raw JSON files into source-specific folders and return moved paths."""
    api_raw_dir = base_dir / "data" / "api" / "raw"
    web_raw_dir = base_dir / "data" / "webscraping" / "raw"

    api_raw_dir.mkdir(parents=True, exist_ok=True)
    web_raw_dir.mkdir(parents=True, exist_ok=True)

    api_files: list[Path] = []
    web_files: list[Path] = []

    for file_path in base_dir.glob("*.json"):
        name_lower = file_path.name.lower()

        if name_lower.startswith("artichoke_"):
            destination = api_raw_dir / file_path.name
            shutil.move(str(file_path), str(destination))
            api_files.append(destination)
        elif name_lower.startswith("foodblog_nycfoodtrends_"):
            destination = web_raw_dir / file_path.name
            shutil.move(str(file_path), str(destination))
            web_files.append(destination)

    # Also include files already organized in raw folders.
    api_files.extend(sorted(api_raw_dir.glob("*.json")))
    web_files.extend(sorted(web_raw_dir.glob("*.json")))

    return _dedupe_paths(api_files), _dedupe_paths(web_files)


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
        has_list = df[col].apply(lambda x: isinstance(x, list)).any()
        if has_list:
            df[col] = df[col].apply(
                lambda x: ", ".join(map(str, x)) if isinstance(x, list) else x
            )
    return df


def strip_html_tags(text: str) -> str:
    if not isinstance(text, str):
        return ""

    if BeautifulSoup is not None:
        return BeautifulSoup(text, "html.parser").get_text(" ", strip=True)

    return re.sub(r"<[^>]+>", " ", text).strip()


def preprocess_api_file(file_path: Path, output_dir: Path) -> Path:
    with file_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    rows = payload.get("results", [])
    df = pd.DataFrame(rows)

    # Null handling for timing fields; keep numeric type.
    for col in ["preparationMinutes", "cookingMinutes"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(-1).astype("int32")

    if "license" in df.columns:
        df = df.drop(columns=["license"])

    int32_cols = ["id", "readyInMinutes", "servings", "weightWatcherSmartPoints"]
    float32_cols = ["healthScore", "pricePerServing", "spoonacularScore"]

    for col in int32_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(-1).astype("int32")

    for col in float32_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")

    df = flatten_list_columns(df)

    if "summary" in df.columns:
        df["summary"] = df["summary"].astype(str).apply(strip_html_tags)

    # IQR outlier flag on logistics/economic variables.
    flag_columns = [c for c in ["readyInMinutes", "pricePerServing"] if c in df.columns]
    df["is_outlier"] = False

    for col in flag_columns:
        series = pd.to_numeric(df[col], errors="coerce")
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        col_outlier = (series < lower) | (series > upper)
        df["is_outlier"] = df["is_outlier"] | col_outlier.fillna(False)

    if "id" in df.columns:
        df = df.drop_duplicates(subset=["id"], keep="last")

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{file_path.stem}_cleaned.parquet"
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
    if not isinstance(text, str):
        return ""

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

    # Drop rows without core NLP fields.
    def is_non_empty(value: object) -> bool:
        return isinstance(value, str) and value.strip() != ""

    if "article_title" in df.columns and "article_summary" in df.columns:
        df = df[
            df["article_title"].apply(is_non_empty)
            & df["article_summary"].apply(is_non_empty)
        ]

    if "published_date" in df.columns:
        df["published_date"] = pd.to_datetime(df["published_date"], utc=True, errors="coerce")

    if "categories" in df.columns:
        df["categories"] = df["categories"].apply(
            lambda x: ", ".join(map(str, x)) if isinstance(x, list) else x
        )

    if "article_summary" in df.columns:
        df["article_summary_clean"] = df["article_summary"].astype(str).apply(
            lambda txt: clean_nlp_text(txt, stop_words)
        )

    dedupe_col = "article_url" if "article_url" in df.columns else "article_id"
    if dedupe_col in df.columns:
        df = df.drop_duplicates(subset=[dedupe_col], keep="last")

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{file_path.stem}_cleaned.parquet"
    df.to_parquet(output_path, index=False)
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Organize and preprocess API and web scraping datasets."
    )
    parser.add_argument(
        "--base-dir",
        default=".",
        help="Project base directory containing JSON files and data folders.",
    )
    args = parser.parse_args()

    base_dir = Path(args.base_dir).resolve()

    api_files, web_files = organize_raw_files(base_dir)

    api_output_dir = base_dir / "data" / "api" / "processed"
    web_output_dir = base_dir / "data" / "webscraping" / "processed"

    api_outputs = [preprocess_api_file(path, api_output_dir) for path in api_files]

    stop_words = get_english_stopwords()
    web_outputs = [
        preprocess_webscraping_file(path, web_output_dir, stop_words) for path in web_files
    ]

    print("Processed API files:")
    for p in api_outputs:
        print(f"- {p}")

    print("Processed web scraping files:")
    for p in web_outputs:
        print(f"- {p}")


if __name__ == "__main__":
    main()
