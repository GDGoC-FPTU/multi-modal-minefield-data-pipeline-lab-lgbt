import json
import os
from typing import Dict, Any, List

# Simple migration helper: v1 -> v2
# - rename `summary` -> `abstract`
# - rename `timestamp` -> `created_at`
# - add `schema_version: v2`
# - preserve other fields and add note to source_metadata


def migrate_record_v1_to_v2(rec: Dict[str, Any]) -> Dict[str, Any]:
    r = dict(rec)
    # rename summary -> abstract
    if 'summary' in r:
        r['abstract'] = r.pop('summary')
    # rename timestamp -> created_at
    if 'timestamp' in r:
        r['created_at'] = r.pop('timestamp')
    # mark schema version
    r['schema_version'] = 'v2'
    # add migration flag
    meta = r.get('source_metadata') or {}
    meta['migrated_from'] = meta.get('migrated_from', 'v1')
    r['source_metadata'] = meta
    return r


def migrate_file(input_path: str, output_path: str = None) -> List[Dict[str, Any]]:
    if output_path is None:
        output_path = input_path
    if not os.path.exists(input_path):
        raise FileNotFoundError(input_path)
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    migrated = [migrate_record_v1_to_v2(d) for d in data]
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(migrated, f, ensure_ascii=False, indent=2)
    return migrated


if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2:
        print('Usage: python schema_migration.py processed_knowledge_base.json [out.json]')
        sys.exit(1)
    inp = sys.argv[1]
    out = sys.argv[2] if len(sys.argv) > 2 else inp
    migrate_file(inp, out)
    print(f'Migrated {inp} -> {out}')