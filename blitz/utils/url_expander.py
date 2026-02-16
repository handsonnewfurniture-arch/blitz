import re


def expand_url_pattern(pattern: str) -> list[str]:
    """Expand {start..end} range patterns in URL strings.

    Examples:
        "https://api.com/page/{1..5}" -> 5 URLs
        "https://api.com/{a,b,c}/data" -> 3 URLs
        "https://api.com/static" -> ["https://api.com/static"]
    """
    # Handle {start..end} range patterns
    range_match = re.search(r"\{(\d+)\.\.(\d+)\}", pattern)
    if range_match:
        start = int(range_match.group(1))
        end = int(range_match.group(2))
        prefix = pattern[: range_match.start()]
        suffix = pattern[range_match.end() :]
        return [f"{prefix}{i}{suffix}" for i in range(start, end + 1)]

    # Handle {a,b,c} list patterns
    list_match = re.search(r"\{([^}]+)\}", pattern)
    if list_match and "," in list_match.group(1):
        items = [item.strip() for item in list_match.group(1).split(",")]
        prefix = pattern[: list_match.start()]
        suffix = pattern[list_match.end() :]
        return [f"{prefix}{item}{suffix}" for item in items]

    return [pattern]


def expand_vars(text: str, variables: dict) -> str:
    """Replace {var_name} placeholders with values from variables dict.

    Also expands $ENV_VAR and ${ENV_VAR} from environment.
    """
    import os

    # First expand environment variables
    result = os.path.expandvars(text)

    # Then expand pipeline variables (skip range/list patterns)
    for key, value in variables.items():
        result = result.replace(f"{{{key}}}", str(value))

    return result
