from bs4 import BeautifulSoup
import os
from schema import UnifiedDocument, SourceType

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Extract product data from the HTML table, ignoring boilerplate.


def _clean_text(cell):
    return ' '.join(cell.get_text(separator=' ', strip=True).split())


def parse_html_catalog(file_path):
    docs = []
    with open(file_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'html.parser')

    table = soup.find('table', id='main-catalog') or soup.find('table')
    if not table:
        return docs

    # find headers
    headers = []
    header_row = table.find('tr')
    if header_row:
        for th in header_row.find_all(['th', 'td']):
            headers.append(_clean_text(th).lower())

    rows = table.find_all('tr')
    for i, row in enumerate(rows[1:], start=1):
        cells = row.find_all(['td', 'th'])
        if not cells:
            continue
        record = {}
        for idx, cell in enumerate(cells):
            key = headers[idx] if idx < len(headers) else f'col_{idx}'
            record[key] = _clean_text(cell)

        price_keys = [k for k in record.keys() if 'price' in k or 'gia' in k]
        price = None
        if price_keys:
            price_raw = record.get(price_keys[0], '')
            if price_raw.lower() in ['n/a', 'liên hệ', 'lien he', '']:
                price = None
            else:
                price = price_raw

        title = record.get('product', record.get('name') or record.get('title') or f'item-{i}')

        doc = UnifiedDocument(
            document_id=f"html-{i}",
            title=title,
            summary=str(record),
            content=' '.join([f"{k}: {v}" for k, v in record.items()]),
            source_type=SourceType.HTML,
            source_metadata={"original_file": os.path.basename(file_path), "price_raw": price},
        )
        docs.append(doc)

    return docs

