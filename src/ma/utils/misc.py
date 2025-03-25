def truncate_string(input_string: str, max_length: int = 30, suffix: str = "...") -> str:
    if len(input_string) > max_length:
        return input_string[: max_length - len(suffix)] + suffix
    return input_string
