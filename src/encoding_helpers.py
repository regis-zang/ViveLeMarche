# src/encoding_helpers.py
def fix_mojibake(s: str) -> str:
    """Conserta strings UTF-8 lidas por engano como latin-1."""
    if not isinstance(s, str):
        return s
    try:
        return s.encode("latin1").decode("utf-8")
    except Exception:
        return s
