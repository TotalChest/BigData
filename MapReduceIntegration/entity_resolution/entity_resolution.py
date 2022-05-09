import collections
import logging
import json
import typing

from mrjob.job import MRJob


logger = logging.getLogger(__name__)


SENTINEL_KEY = 'OTHERS'
THRESHOLD_EQUAL = 3


def _get_model(json_object: typing.Dict) -> str:
    model = json_object.get('model')
    if model in (None, 'null'):
        return SENTINEL_KEY

    model_words = model.lower().split()
    for i, world in enumerate(model_words):
        if world == 'iphone':
            word_iphone_index = i
            break
    else:
        logger.info('No "iphone" word')
        return SENTINEL_KEY

    if len(model_words) > word_iphone_index + 1:
        return model_words[word_iphone_index + 1]
    else:
        logger.info('No words after "iphone"')
        return SENTINEL_KEY


def _get_width(json_object: typing.Dict) -> str:
    width = json_object.get('width')
    if width in (None, 'null'):
        return SENTINEL_KEY

    return width.lower()


def _get_processor(json_object: typing.Dict) -> str:
    processor = json_object.get('processor')
    if processor in (None, 'null'):
        return SENTINEL_KEY

    return processor.lower()


def _get_memory(json_object: typing.Dict) -> str:
    memory = json_object.get('memory')
    if memory in (None, 'null'):
        return SENTINEL_KEY

    first_memory_word = memory.split()[0]
    return first_memory_word.lower()


def _get_color(json_object: typing.Dict) -> str:
    color = json_object.get('color')
    if color in (None, 'null'):
        return SENTINEL_KEY

    return color.lower()


def _get_protect(json_object: typing.Dict) -> str:
    protect = json_object.get('protect')
    if protect in (None, 'null'):
        return SENTINEL_KEY

    return protect.lower()


def _get_os_version(json_object: typing.Dict) -> str:
    os_version = json_object.get('os_version')
    if os_version in (None, 'null'):
        return SENTINEL_KEY

    return os_version.lower()


def _object_with_key(
        keys: typing.List[str],
        json_object: typing.Dict,
) -> typing.Generator[str, None, None]:
    key = '|'.join(keys)
    yield key, json_object


def _is_equal_params(param_1: str, param_2: str, scores=(3, 2, 0)) -> int:
    if param_1 != SENTINEL_KEY and param_2 != SENTINEL_KEY:
        if param_1 == param_2:
            return scores[0]
        else:
            return scores[2]

    return scores[1]


def _is_equal_objects(object_1, object_2):
    model_1 = _get_model(json_object=object_1)
    model_2 = _get_model(json_object=object_2)

    processor_1 = _get_processor(json_object=object_1)
    processor_2 = _get_processor(json_object=object_2)

    protect_1 = _get_protect(json_object=object_1)
    protect_2 = _get_protect(json_object=object_2)

    os_version_1 = _get_os_version(json_object=object_1)
    os_version_2 = _get_os_version(json_object=object_2)

    return sum(
        [
            _is_equal_params(model_1, model_2, (3, 2, 0)),
            _is_equal_params(processor_1, processor_2, (2, 1, 0)),
            _is_equal_params(protect_1, protect_2, (2, 1, 0)),
            _is_equal_params(os_version_1, os_version_2, (2, 1, 0)),
        ]
    ) > THRESHOLD_EQUAL


class EntityResolution(MRJob):
    def mapper(self, _, line):
        json_object = json.loads(line)

        key_by_width = _get_width(json_object=json_object)

        yield from _object_with_key(
            keys=[key_by_width],
            json_object=json_object,
        )

    def reducer(self, key, objects):
        objects = list(objects)

        group_number = 0
        group_to_objects = collections.defaultdict(list)
        touched_indices = set()

        for index_1, object_1 in enumerate(objects):
            if index_1 in touched_indices:
                continue

            group_number += 1
            group_to_objects[group_number].append(object_1)
            touched_indices.add(index_1)

            for index_2, object_2 in enumerate(objects):
                if index_2 in touched_indices:
                    continue

                if _is_equal_objects(object_1, object_2):
                    group_to_objects[group_number].append(object_2)
                    touched_indices.add(index_2)

        for group in group_to_objects.values():
            yield None, group


if __name__ == '__main__':
    EntityResolution.run()
