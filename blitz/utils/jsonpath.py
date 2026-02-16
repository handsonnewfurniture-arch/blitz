def jsonpath_extract(data, path: str):
    """Extract data using simplified JSONPath notation.

    Supports:
        $.field           — dict key access
        $.field.subfield  — nested access
        $.field[*]        — iterate list items
        $[*].field        — extract field from each item in a list

    Examples:
        jsonpath_extract({"data": {"items": [1,2,3]}}, "$.data.items") -> [1,2,3]
        jsonpath_extract([{"a": 1}, {"a": 2}], "$[*].a") -> [1, 2]
    """
    if not path.startswith("$"):
        raise ValueError(f"JSONPath must start with '$': {path}")

    # Remove leading $. or $
    remainder = path[1:]
    if remainder.startswith("."):
        remainder = remainder[1:]

    if not remainder:
        return data

    parts = _split_path(remainder)
    current = data

    for part in parts:
        if current is None:
            return None

        if part == "*" or part == "[*]":
            if not isinstance(current, list):
                return None
            continue

        if isinstance(current, list):
            # Apply field extraction to each item in the list
            current = [
                item.get(part) if isinstance(item, dict) else None
                for item in current
            ]
            # Flatten nested lists
            if current and isinstance(current[0], list):
                current = [x for sublist in current if sublist for x in sublist]
        elif isinstance(current, dict):
            current = current.get(part)
        else:
            return None

    return current


def _split_path(path: str) -> list[str]:
    """Split a JSONPath remainder into parts, handling [*] notation."""
    parts = []
    current = ""
    i = 0
    while i < len(path):
        if path[i] == ".":
            if current:
                parts.append(current)
                current = ""
        elif path[i] == "[":
            if current:
                parts.append(current)
                current = ""
            end = path.index("]", i)
            bracket_content = path[i : end + 1]
            parts.append(bracket_content)
            i = end
        else:
            current += path[i]
        i += 1

    if current:
        parts.append(current)

    return parts
