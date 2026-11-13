"""Shared ordering and grouping for the EMPAREJA2 character catalog."""
from decimal import Decimal


def group_id(item):
    value = item.get("pairGroupId") or item.get("pairNumericId")
    if value is None:
        return None
    if isinstance(value, Decimal):
        return str(int(value))
    return str(value).strip() or None


def character_id(item):
    try:
        return int(item.get("characterId"))
    except (TypeError, ValueError, OverflowError):
        return None


def group_sort_key(group):
    if group.startswith("CONTRIB#"):
        return (2, group)
    return (0, int(group), group) if group.isdigit() else (1, group)


def group_items(items):
    groups = {}
    for item in items:
        group = group_id(item)
        if group is not None and character_id(item) is not None:
            groups.setdefault(group, []).append(item)
    return [(group, groups[group]) for group in sorted(groups, key=group_sort_key)]
