import google.generativeai as genai
import os
import json
import time
from dotenv import load_dotenv

from schema import UnifiedDocument, SourceType

load_dotenv()
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))


def _safe_json_load(s: str):
    s = s.strip()
    # remove markdown fences
    if s.startswith("```json"):
        s = s[len("```json"):]
    if s.startswith("```"):
        s = s[3:]
    if s.endswith("```"):
        s = s[:-3]
    try:
        return json.loads(s)
    except Exception:
        # try to find first { and last }
        start = s.find('{')
        end = s.rfind('}')
        if start != -1 and end != -1:
            try:
                return json.loads(s[start:end+1])
            except Exception:
                pass
    raise ValueError("Could not parse JSON from model response")


def extract_pdf_data(file_path, max_retries=5, backoff_factor=1.5):
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return None

    model = genai.GenerativeModel('gemini-2.5-flash')

    print(f"Uploading {file_path} to Gemini...")
    try:
        pdf_file = genai.upload_file(path=file_path)
    except Exception as e:
        print(f"Failed to upload file to Gemini: {e}")
        return None

    prompt = (
        "Analyze this document and return a JSON object with: document_id, title, summary, content, "
        "source_type, author, timestamp, and source_metadata (include original filename). "
        "If fields are unknown, set them to null or empty strings."
    )

    attempt = 0
    while attempt <= max_retries:
        try:
            print(f"Generating content from PDF using Gemini (attempt {attempt+1})...")
            response = model.generate_content([pdf_file, prompt])
            content_text = getattr(response, 'text', None) or str(response)
            data = _safe_json_load(content_text)

            # Ensure required fields and map to UnifiedDocument
            doc = UnifiedDocument(
                document_id=data.get('document_id') or os.path.basename(file_path),
                title=data.get('title') or data.get('document_id'),
                summary=data.get('summary') or None,
                content=data.get('content') or data.get('summary') or "",
                source_type=SourceType.PDF,
                author=data.get('author') or "Unknown",
                timestamp=data.get('timestamp'),
                source_metadata=data.get('source_metadata') or {"original_file": os.path.basename(file_path)}
            )
            return doc

        except Exception as e:
            msg = str(e)
            # Detect rate limit
            if '429' in msg or 'rate' in msg.lower() or 'quota' in msg.lower():
                wait = (backoff_factor ** attempt) * 1.0
                print(f"Rate limited by Gemini, sleeping {wait:.1f}s before retry...")
                time.sleep(wait)
                attempt += 1
                continue
            else:
                print(f"Failed to generate content from Gemini: {e}")
                return None

    print("Exceeded max retries for Gemini PDF extraction")
    return None
