# schema_registry/summary.py
from typing import Dict, Any, List, Tuple, Optional
from deepdiff import DeepDiff


def _top_level_keys(schema: Dict[str, Any]) -> List[str]:
    """
    Return top-level property keys from a JSON Schema produced by genson/custom builder.
    If schema appears to be a direct 'properties' dict, handle that.
    """
    if not schema:
        return []
    props = schema.get("properties") if isinstance(schema, dict) else None
    if not props and isinstance(schema, dict):
        # Maybe schema itself is the properties dict (defensive)
        props = schema
    if not isinstance(props, dict):
        return []
    return list(props.keys())


def _count_properties(schema_section: Dict[str, Any]) -> int:
    """Return a heuristic 'field count' for a schema section (properties length)."""
    if not isinstance(schema_section, dict):
        return 0
    # If section has 'properties'
    if "properties" in schema_section and isinstance(schema_section["properties"], dict):
        return len(schema_section["properties"])
    # If section is an array with 'items' providing properties
    if "items" in schema_section and isinstance(schema_section["items"], dict):
        if "properties" in schema_section["items"]:
            return len(schema_section["items"]["properties"])
    # Fallback: treat section as dict of fields
    return len(schema_section)


def _lapcol_count(schema: Dict[str, Any]) -> int:
    """
    Heuristic: count keys that start with 'lapcol__' inside 'schema_shape' or top-level properties.
    This is used to measure how many lap columns are represented in the structural key map.
    """
    props = schema.get("properties", {}) if isinstance(schema, dict) else {}
    # check schema_shape
    if "schema_shape" in props and isinstance(props["schema_shape"], dict):
        ss_props = props["schema_shape"].get("properties", {}) or {}
        return len([k for k in ss_props.keys() if k.startswith("lapcol__")])
    # fallback: count any top-level keys that start with lapcol__
    return len([k for k in props.keys() if k.startswith("lapcol__")])


def summarize_schema_diff(schema_a: Dict[str, Any], schema_b: Dict[str, Any]) -> Dict[str, Any]:
    """
    Produce a concise, human-friendly summary describing the major structural differences
    between two JSON Schemas.
    Returns a dict with:
      - added_keys, removed_keys (top-level)
      - counts: per-section field counts and diffs (laps, weather, results, schema_shape)
      - lap_column_counts: a numeric comparison
      - quick_lines: 3-6 human-readable bullets summarizing the most important changes
      - small_deepdiff: a small DeepDiff (trimmed) for the top-level keys if useful
    """
    a_keys = set(_top_level_keys(schema_a))
    b_keys = set(_top_level_keys(schema_b))

    added_keys = sorted(list(b_keys - a_keys))
    removed_keys = sorted(list(a_keys - b_keys))
    common_keys = sorted(list(a_keys & b_keys))

    # Heuristic counts for several likely important sections
    sections = ["laps", "weather", "results", "schema_shape", "session_details", "event_info", "schema_signature"]
    counts = {}
    for sec in sections:
        sec_a = schema_a.get("properties", {}).get(sec, {}) if isinstance(schema_a, dict) else {}
        sec_b = schema_b.get("properties", {}).get(sec, {}) if isinstance(schema_b, dict) else {}
        ca = _count_properties(sec_a)
        cb = _count_properties(sec_b)
        counts[sec] = {"a": ca, "b": cb, "delta": cb - ca}

    # lapcol count (structural key trick)
    lapcols_a = _lapcol_count(schema_a)
    lapcols_b = _lapcol_count(schema_b)

    # Use DeepDiff but keep only top-level keys changes to avoid enormous output
    try:
        dd = DeepDiff(schema_a, schema_b, ignore_order=True, max_list_length=10)
        dd_dict = dd.to_dict()
    except Exception:
        dd_dict = {"error": "deepdiff-failed"}

    # Trim deep-diff to only top-level diffs (if present)
    small_dd = {}
    for key in ["dictionary_item_added", "dictionary_item_removed", "type_changes", "values_changed"]:
        if key in dd_dict:
            # take first 10 entries from each (if large)
            items = list(dd_dict[key].items()) if isinstance(dd_dict[key], dict) else dd_dict[key]
            small_dd[key] = dict(items[:10]) if isinstance(dd_dict[key], dict) else (items[:10] if isinstance(items, list) else dd_dict[key])

    # Compose human-friendly bullets
    lines = []
    if added_keys:
        lines.append(f"+ Added top-level keys: {', '.join(added_keys[:6])}{'...' if len(added_keys)>6 else ''}")
    if removed_keys:
        lines.append(f"- Removed top-level keys: {', '.join(removed_keys[:6])}{'...' if len(removed_keys)>6 else ''}")

    # Significant numeric changes
    if lapcols_a != lapcols_b:
        lines.append(f"Δ lap columns: {lapcols_a} → {lapcols_b} ({'+' if lapcols_b>lapcols_a else ''}{lapcols_b - lapcols_a})")

    # key section deltas
    for sec in ["laps", "weather", "results", "schema_shape"]:
        d = counts.get(sec, {})
        if d and abs(d["delta"]) > 0:
            lines.append(f"Δ {sec} fields: {d['a']} → {d['b']} ({'+' if d['delta']>0 else ''}{d['delta']})")

    # If nothing significant detected, say so
    if not lines:
        lines.append("No obvious structural differences detected at the top-level (see full diff for details).")

    summary = {
        "added_keys": added_keys,
        "removed_keys": removed_keys,
        "lapcol_count": {"a": lapcols_a, "b": lapcols_b, "delta": lapcols_b - lapcols_a},
        "section_counts": counts,
        "quick_lines": lines,
        "small_deepdiff": small_dd,
    }

    return summary
