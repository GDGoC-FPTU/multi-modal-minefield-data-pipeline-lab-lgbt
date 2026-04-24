import json
import time
import os

# Robust path handling
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "raw_data")


# Import role-specific modules
from schema import UnifiedDocument
from process_pdf import extract_pdf_data
from process_transcript import clean_transcript
from process_html import parse_html_catalog
from process_csv import process_sales_csv
from process_legacy_code import extract_logic_from_code
from quality_check import run_quality_gate

# ==========================================
# ROLE 4: DEVOPS & INTEGRATION SPECIALIST
# ==========================================
# Task: Orchestrate the ingestion pipeline and handle errors/SLA.

def main():
    start_time = time.time()
    final_kb = []
    
    # --- FILE PATH SETUP (Handled for students) ---
    pdf_path = os.path.join(RAW_DATA_DIR, "lecture_notes.pdf")
    trans_path = os.path.join(RAW_DATA_DIR, "demo_transcript.txt")
    html_path = os.path.join(RAW_DATA_DIR, "product_catalog.html")
    csv_path = os.path.join(RAW_DATA_DIR, "sales_records.csv")
    code_path = os.path.join(RAW_DATA_DIR, "legacy_pipeline.py")
    
    output_path = os.path.join(os.path.dirname(SCRIPT_DIR), "processed_knowledge_base.json")
    # ----------------------------------------------

    # --- Run processors ---
    def _serialize(doc):
        if doc is None:
            return None
        try:
            # pydantic model
            d = doc.dict()
        except Exception:
            try:
                d = doc.to_dict()
            except Exception:
                d = dict(doc)
        # normalize timestamp
        ts = d.get('timestamp')
        if ts is not None:
            try:
                d['timestamp'] = ts.isoformat()
            except Exception:
                d['timestamp'] = str(ts)
        return d

    # PDF
    pdf_doc = extract_pdf_data(pdf_path)
    if pdf_doc and run_quality_gate(pdf_doc):
        final_kb.append(_serialize(pdf_doc))

    # Transcript (use price-aware cleaner)
    try:
        from process_transcript import clean_transcript_with_price as clean_transcript_fn
    except Exception:
        from process_transcript import clean_transcript as clean_transcript_fn
    trans_doc = clean_transcript_fn(trans_path)
    if trans_doc and run_quality_gate(trans_doc):
        final_kb.append(_serialize(trans_doc))

    # HTML (may return list)
    html_docs = parse_html_catalog(html_path)
    for h in (html_docs or []):
        if h and run_quality_gate(h):
            final_kb.append(_serialize(h))

    # CSV
    csv_docs = process_sales_csv(csv_path)
    for c in (csv_docs or []):
        if c and run_quality_gate(c):
            final_kb.append(_serialize(c))

    # Legacy code
    code_doc = extract_logic_from_code(code_path)
    if code_doc and run_quality_gate(code_doc):
        final_kb.append(_serialize(code_doc))

    # Save
    try:
        with open(output_path, 'w', encoding='utf-8') as out:
            json.dump(final_kb, out, ensure_ascii=False, indent=2)
        print(f"Wrote processed knowledge base to {output_path}")
    except Exception as e:
        print(f"Failed to write output: {e}")

    end_time = time.time()
    print(f"Pipeline finished in {end_time - start_time:.2f} seconds.")
    print(f"Total valid documents stored: {len(final_kb)}")


if __name__ == "__main__":
    main()
