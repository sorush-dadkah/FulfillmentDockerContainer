def to_camel_case(raw_str: str, divider: str) -> str:
    components = raw_str.split(divider)
    return components[0].lower() + "".join(x.title() for x in components[1:])
