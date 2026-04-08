# Data Organization and Preprocessing

This project separates raw JSON files by source and applies dedicated preprocessing pipelines for:

- Structured recipe data from Spoonacular API.
- Unstructured article data from Eater NY web scraping.

## Folder Layout

- `data/api/raw`: Raw API JSON files (e.g., `artichoke_*.json`).
- `data/api/processed`: Cleaned API outputs in Parquet.
- `data/webscraping/raw`: Raw scraping JSON files (e.g., `foodblog_nycfoodtrends_*.json`).
- `data/webscraping/processed`: Cleaned scraping outputs in Parquet.

## What the Script Does

### API Pipeline (`artichoke_*.json`)

- Fills nulls in `preparationMinutes` and `cookingMinutes` with `-1`.
- Drops `license`.
- Casts numeric columns to `int32` and `float32`.
- Flattens list columns (such as `dishTypes`, `diets`) into comma-separated strings.
- Removes HTML tags from `summary`.
- Flags outliers (`is_outlier`) using IQR on `readyInMinutes` and `pricePerServing`.
- Deduplicates by `id` keeping the last record.

### Web Scraping Pipeline (`foodblog_nycfoodtrends_*.json`)

- Drops records with empty/null `article_title` or `article_summary`.
- Converts `published_date` to UTC datetime.
- Converts `categories` list to comma-separated text.
- Creates `article_summary_clean` with:
  - HTML entity decoding.
  - Lowercasing.
  - Punctuation removal.
  - English stop-word removal using NLTK.
- Deduplicates by `article_url` (or `article_id` fallback).

## Run

1. Install dependencies:

```bash
python -m pip install -r requirements.txt
```

2. Execute the pipeline from project root:

```bash
python preprocess_datasets.py --base-dir .
```

## Notes

- The script automatically moves top-level JSON files into the correct raw folders.
- If files are already in raw folders, they are processed directly.
- Outputs are saved as `*_cleaned.parquet`.
