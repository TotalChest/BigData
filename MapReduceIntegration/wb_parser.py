import json
import requests


WB_BASE_URL = (
    'https://wbxcatalog-ru.wildberries.ru/brands/a/catalog?stores=125238,12523'
    '9,125240,117289,507,120602,6158,3158,117501,120762,121709,124731,159402,2'
    '737,117986,1733,686,132043&locale=ru&lang=ru&curr=rub&dest=-1029256,-10226'
    '9,-1252558,-445284&subject=515&brand=6049'
)
WB_PRODUCT_URL = 'https://wbx-content-v2.wbstatic.net/ru/{product_id}.json'

RESULT_FILE = 'raw_data/WB_raw_data.txt'


def main():
    result = []

    response_json = requests.get(WB_BASE_URL).json()
    for product in response_json['data']['products']:
        product_id = product['id']
        response_json = requests.get(WB_PRODUCT_URL.format(
            product_id=product_id,
        )).json()
        result.append(response_json)

    with open(RESULT_FILE, 'w') as f:
        for json_data in result:
            json.dump(json_data, f, ensure_ascii=False)
            f.write('\n')


if __name__ == '__main__':
    main()
