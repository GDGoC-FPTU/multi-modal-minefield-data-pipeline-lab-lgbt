# ==========================================
# ROLE 3: OBSERVABILITY & QA ENGINEER
# ==========================================
# Task: Implement quality gates to reject corrupt data or logic discrepancies.

TOXIC_PATTERNS = [
    'null pointer exception',
    'segmentation fault',
    'error',
    'traceback',
    'access denied'
]


def run_quality_gate(document):
    """Return True if document passes quality gates, False otherwise.

    `document` may be a dict or a UnifiedDocument instance.
    """
    if document is None:
        return False

    # support dict input
    try:
        content = document.content if hasattr(document, 'content') else document.get('content', '')
    except Exception:
        content = ''

    if not content or len(content.strip()) < 20:
        return False

    lowered = content.lower()
    for p in TOXIC_PATTERNS:
        if p in lowered:
            return False

    return True
