import logging
import json

from mrjob.job import MRJob

from utils import get_value_by_json_path


logger = logging.getLogger(__name__)


WB_SCHEMA_TO_CANON_SCHEMA = {
    'nm_colors_names': 'color',
}

WB_OPTION_TO_CANON_SCHEMA = {
    'Модель': 'model',
    'Версия операционной системы': 'os_version',
    'Гарантийный срок': 'warranty',
    'Степень пылевлагозащиты': 'protect',
    'Диагональ экрана': 'screen_diagonal',
    'Разрешение экрана': 'screen_resolution',
    'Тип дисплея/экрана': 'screen_type',
    'Беспроводные интерфейсы': 'wireless_interfaces',
    'Стандарт связи': 'connection_standard',
    'Процессор': 'processor',
    'Объем встроенной памяти (Гб)': 'memory',
    'Тип SIM карты': 'sim_type',
    'Количество SIM карт': 'sim_count',
    'Время работы при прослушивании музыки': 'music_time',
    'Количество мп основной камеры': 'camera',
    'Фронтальная камера (млн. пикс.)': 'front_camera',
    'Вес товара без упаковки (г)': 'weight',
    'Высота предмета': 'height',
    'Толщина предмета': 'thickness',
    'Ширина предмета': 'width',
}


class Prepare(MRJob):
    def mapper(self, _, line):
        canon_schema_object = {}
        wb_schema_object = json.loads(line)

        for source_path, new_key in WB_SCHEMA_TO_CANON_SCHEMA.items():
            canon_schema_object[new_key] = get_value_by_json_path(
                wb_schema_object, source_path,
            )

        for option in wb_schema_object.get('options', []):
            canon_field = WB_OPTION_TO_CANON_SCHEMA.get(option.get('name'))
            if canon_field is not None:
                canon_schema_object[canon_field] = option.get('value')

        yield None, canon_schema_object


if __name__ == '__main__':
    Prepare.run()
