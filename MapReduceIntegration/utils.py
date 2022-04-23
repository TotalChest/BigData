SPLIT_SYMBOL = '|'


def _get_by_json_path(json_object, json_path):
    for key in json_path.split('.'):
        if isinstance(json_object, list):
            try:
                json_object = json_object[int(key)]
            except IndexError:
                return None
        elif isinstance(json_object, dict):
            try:
                json_object = json_object[key]
            except KeyError:
                return None
        else:
            raise Exception()

    return json_object


def get_value_by_json_path(json_object, source_path):
    result = {}

    for field in source_path.split(SPLIT_SYMBOL):
        value = _get_by_json_path(json_object.copy(), field)
        if field is not None:
            result[field] = value

    if len(result) == 0:
        return None
    elif len(result) == 1:
        return next(iter(result.values()))
    else:
        return SPLIT_SYMBOL.join(
            f'{key}: {value}' for key, value in result.items()
        )
