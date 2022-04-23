import json
from selenium import webdriver
from bs4 import BeautifulSoup


DNS_BASE_URL = 'https://www.dns-shop.ru/search/?q=iphone&p={page}'
PRODUCT_URL = 'https://www.dns-shop.ru{product_url}'

CHARACTERISTIC_TITLE = 'product-characteristics__spec-title'
CHARACTERISTIC_VALUE = 'product-characteristics__spec-value'

RESULT_FILE = 'raw_data/DNS_raw_data.txt'


def main():
    product_urls = set()
    result = []
    with webdriver.Safari() as driver:
        for page in range(15):
            driver.get(DNS_BASE_URL.format(page=page))
            html_doc = driver.page_source
            soup = BeautifulSoup(html_doc, 'html.parser')

            for a_tag in soup.find_all('a', href=True):
                if (
                        a_tag['href'].startswith('/product/')
                        and len(a_tag['href'].split('/')) == 5
                ):
                    product_urls.add(a_tag['href'])

        for product_url in product_urls:
            driver.get(PRODUCT_URL.format(product_url=product_url))
            html_doc = driver.page_source
            soup = BeautifulSoup(html_doc, 'html.parser')

            product_dict = {}
            for key, value in zip(
                    soup.find_all('div', {'class': CHARACTERISTIC_TITLE}),
                    soup.find_all('div', {'class': CHARACTERISTIC_VALUE}),
            ):
                if not len(key.contents) or not len(value.contents):
                    continue

                characteristic = str(key.contents[0]).strip()

                while not isinstance(value.contents[0], str):
                    value = value.contents[0]
                characteristic_value = str(value.contents[0]).strip()

                product_dict[characteristic] = characteristic_value

            result.append(product_dict)

    with open(RESULT_FILE, 'w') as f:
        for json_data in result:
            json.dump(json_data, f, ensure_ascii=False)
            f.write('\n')


if __name__ == '__main__':
    main()
