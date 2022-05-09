import json
import logging

from functools import partial, reduce, wraps
from statistics import mean
import typing as tp

from mrjob.job import MRJob


logger = logging.getLogger(__name__)


def params(func):
    @wraps(func)
    def wrapper(values=None, **kwargs):
        if not kwargs:
            return func(values)
        return partial(func, **kwargs)
    return wrapper


@params
def concat(values: tp.List[str], **kwargs):
    return kwargs.get('delimiter', ', ').join(
        {value for value in values if value}
    )


@params
def min_length_str(values: tp.List[str], **kwargs):
    min_len = 1e10
    result = values[0]
    for value in values:
        if value is not None and len(value) < min_len:
            min_len = len(value)
            result = value
    return result


@params
def coalesce(values: tp.List[str], **kwargs):
    return reduce(lambda x, y: x if x is not None else y, values)


@params
def mean_value(values: tp.List[str], **kwargs):
    delimiter = kwargs.get('delimiter', ' ')
    int_values = [int(value.split(delimiter)[0]) for value in values if value]
    return str(mean(int_values)) if int_values else None


FIELD_TO_RESOLUTION_FUNC = {
    'color': concat(delimiter=', '),
    'model': min_length_str,
    'os_version': min_length_str,
    'warranty': coalesce,
    'protect': coalesce,
    'screen_diagonal': coalesce,
    'screen_resolution': coalesce,
    'screen_type': coalesce,
    'wireless_interfaces': min_length_str,
    'connection_standard': min_length_str,
    'processor': min_length_str,
    'memory': concat(delimiter=', '),
    'sim_type': coalesce,
    'sim_count': concat(delimiter=', '),
    'music_time': mean_value(delimiter=' '),
    'camera': coalesce,
    'front_camera': coalesce,
    'weight': coalesce,
    'height': coalesce,
    'thickness': coalesce,
    'width': coalesce
}


def fuse(items):
    result = {}
    for field, resolution_func in FIELD_TO_RESOLUTION_FUNC.items():
        values = list(map(lambda obj: obj.get(field), items))
        result[field] = resolution_func(values)

    return result


class Fusion(MRJob):
    def mapper(self, _, line):
        values = json.loads(line)
        yield None, fuse(values)


if __name__ == '__main__':
    Fusion.run()
