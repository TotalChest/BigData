import logging
import json

from mrjob.job import MRJob

from utils import get_value_by_json_path
from utils import SPLIT_SYMBOL


logger = logging.getLogger(__name__)


DNS_SCHEMA_TO_CANON_SCHEMA = {
    'Цвет, заявленный производителем': 'color',
    'Модель': 'model',
    'Версия ОС': 'os_version',
    'Гарантия': 'warranty',
    'Степень защиты IP': 'protect',
    'Диагональ экрана (дюйм)': 'screen_diagonal',
    'Разрешение экрана': 'screen_resolution',
    'Технология изготовления экрана': 'screen_type',
    f'Версия Bluetooth{SPLIT_SYMBOL}Стандарт Wi-Fi{SPLIT_SYMBOL}NFC': (
        'wireless_interfaces'
    ),
    f'Поддержка сетей 2G{SPLIT_SYMBOL}Поддержка сетей 3G{SPLIT_SYMBOL}Поддержк'
    f'а сетей 4G (LTE)': (
        'connection_standard'
    ),
    'Модель процессора': 'processor',
    'Объем встроенной памяти': 'memory',
    'Формат SIM-карт': 'sim_type',
    'Количество SIM-карт': 'sim_count',
    'Время работы при прослушивании музыки': 'music_time',
    'Количество мегапикселей основной камеры': 'camera',
    'Количество мегапикселей фронтальной камеры': 'front_camera',
    'Вес': 'weight',
    'Высота': 'height',
    'Толщина': 'thickness',
    'Ширина': 'width',
}


class Prepare(MRJob):
    def mapper(self, _, line):
        canon_schema_object = {}
        wb_schema_object = json.loads(line)

        for source_path, new_key in DNS_SCHEMA_TO_CANON_SCHEMA.items():
            canon_schema_object[new_key] = get_value_by_json_path(
                wb_schema_object, source_path,
            )
        yield '', canon_schema_object


if __name__ == '__main__':
    Prepare.run()

