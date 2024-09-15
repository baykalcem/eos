from typing import Any

import pandas as pd


def flatten_dict(d: dict[str, Any], sep: str = ".") -> dict[str, Any]:
    """
    Flatten a nested dictionary, concatenating keys with a separator.
    Works for arbitrary levels of nesting.
    """

    def _flatten(current: dict[str, Any], parent_key: str = "") -> dict[str, Any]:
        items = {}
        for k, v in current.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.update(_flatten(v, new_key))
            else:
                items[new_key] = v
        return items

    return _flatten(d)


def unflatten_dict(d: dict[str, Any], sep: str = ".") -> dict[str, Any]:
    """
    Unflatten a dictionary with concatenated keys into a nested dictionary.
    Works for arbitrary levels of nesting.
    """
    unflattened = {}
    for key, value in d.items():
        parts = key.split(sep)
        current = unflattened
        for part in parts[:-1]:
            current = current.setdefault(part, {})
        current[parts[-1]] = value
    return unflattened


def dicts_to_dfs(dicts: list[dict[str, Any]], sep: str = ".") -> pd.DataFrame:
    """
    Convert a list of nested dictionaries to a pandas DataFrame.
    Each nested dictionary is flattened, and the resulting DataFrame is created.
    """
    flattened_dicts = [flatten_dict(d, sep) for d in dicts]
    return pd.DataFrame(flattened_dicts)


def df_to_dicts(df: pd.DataFrame, sep: str = ".") -> list[dict[str, Any]]:
    """
    Convert a pandas DataFrame back to a list of nested dictionaries.
    """
    return [unflatten_dict(row.dropna().to_dict(), sep) for _, row in df.iterrows()]
