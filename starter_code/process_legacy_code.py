import ast
import os
from schema import UnifiedDocument, SourceType

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Extract docstrings and comments from legacy Python code.


def extract_logic_from_code(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        source_code = f.read()

    tree = ast.parse(source_code)
    docs = []

    # module docstring
    mod_doc = ast.get_docstring(tree)
    if mod_doc:
        docs.append(('module', mod_doc))

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            name = getattr(node, 'name', '<anon>')
            doc = ast.get_docstring(node)
            if doc:
                docs.append((name, doc))

    # also scan for business-rule comments
    business_comments = []
    for line in source_code.splitlines():
        if '# Business Logic' in line or '# BusinessRule' in line or 'Business Logic Rule' in line:
            business_comments.append(line.strip())

    combined = '\n\n'.join([f"{n}: {d}" for n, d in docs])
    if business_comments:
        combined += '\n\n' + '\n'.join(business_comments)

    doc = UnifiedDocument(
        document_id=os.path.basename(file_path),
        title=f"legacy-{os.path.basename(file_path)}",
        summary=(combined[:300] + '...') if len(combined) > 300 else combined,
        content=combined,
        source_type=SourceType.CODE,
        source_metadata={"original_file": os.path.basename(file_path)}
    )

    return doc

