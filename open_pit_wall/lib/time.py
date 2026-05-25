"""Time parsing helpers."""


def parse_time_string(time_string):
    """Convert a pandas/FastF1 time string into total seconds."""
    normalized = str(time_string).strip()
    if not normalized or normalized in {"NaT", "None"}:
        raise ValueError(f"Unsupported time string: {time_string}")

    if "days" in normalized:
        day_part, normalized = normalized.split("days", maxsplit=1)
        days = int(day_part.strip())
        normalized = normalized.strip()
    else:
        days = 0

    parts = normalized.split(":")
    if len(parts) == 3:
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = float(parts[2])
    elif len(parts) == 2:
        hours = 0
        minutes = int(parts[0])
        seconds = float(parts[1])
    else:
        hours = 0
        minutes = 0
        seconds = float(parts[0])

    return days * 86400 + hours * 3600 + minutes * 60 + seconds
