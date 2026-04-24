import json

with open('processed_knowledge_base_v2.json', encoding='utf-8') as f:
    data = json.load(f)

for rec in data:
    src = rec.get('source_type','')
    meta = rec.get('source_metadata',{})
    if src and src.lower()=='transcript' or meta.get('original_file','')=='demo_transcript.txt':
        print('--- TRANSCRIPT RECORD ---')
        print('document_id:', rec.get('document_id'))
        print('title:', rec.get('title'))
        print('price_extracted:', meta.get('price_extracted'))
        print('content snippet:\n')
        print(rec.get('content','')[:2000])
        break
