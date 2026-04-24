import pandas as pd
import re
import os
from schema import UnifiedDocument, SourceType

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Process sales records, handling type traps and duplicates.


def _clean_price(v):
    if pd.isna(v):
        return None
    s = str(v).strip()
    # remove currency symbols and commas
    s2 = re.sub(r"[,$]", "", s)
    # handle $1200 or 1200
    m = re.search(r"(\d+[\d\.]+)", s2)
    if m:
        try:
            return float(m.group(1))
        except Exception:
            return None
    # fallback: try to parse numbers inside words (basic english)
    words_to_num = {"zero":0,"one":1,"two":2,"three":3,"four":4,"five":5,
                    "six":6,"seven":7,"eight":8,"nine":9,"ten":10}
    parts = s2.lower().split()
    total = 0
    found = False
    for p in parts:
        if p in words_to_num:
            total += words_to_num[p]
            found = True
    if found:
        return float(total)
    return None


def process_sales_csv(file_path):
    df = pd.read_csv(file_path)
    if 'id' in df.columns:
        df = df.drop_duplicates(subset=['id'])
    else:
        df = df.drop_duplicates()

    # clean price
    if 'price' in df.columns:
        df['price_clean'] = df['price'].apply(_clean_price)
    else:
        df['price_clean'] = None

    # normalize date_of_sale
    if 'date_of_sale' in df.columns:
        df['date_norm'] = pd.to_datetime(df['date_of_sale'], errors='coerce').dt.strftime('%Y-%m-%d')
    else:
        df['date_norm'] = None

    docs = []
    for idx, row in df.iterrows():
        doc = UnifiedDocument(
            document_id=str(row.get('id') or f'csv-{idx}'),
            title=str(row.get('product') or row.get('name') or ''),
            summary=str(row.to_dict()),
            content=str(row.to_dict()),
            source_type=SourceType.CSV,
            source_metadata={
                'original_file': os.path.basename(file_path),
                'price_clean': row.get('price_clean'),
                'date_norm': row.get('date_norm')
            }
        )
        docs.append(doc)

    return docs

