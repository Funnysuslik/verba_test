from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import httpx

sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))

from aggregator import CatalogAggregator
from loader import XlsxLoader


class WBDefaults:
    SEARCH_WORD = "пальто из натуральной шерсти"
    PAGE_SIZE = 1000
    BASE_URLS = [
        "https://search.wb.ru/exactmatch/ru/common/v5/search",
        "https://www.wildberries.ru/__internal/u-search/exactmatch/ru/common/v18/search",
    ]
    OUTPUT_CATALOG = "output/catalog.xlsx"
    OUTPUT_FILTERED = "output/catalog_filtered.xlsx"
    PRODUCT_URL = "https://www.wildberries.ru/catalog/{product_id}/detail.aspx"
    SELLER_URL = "https://www.wildberries.ru/seller/{seller_id}"
    IMAGES_URL = "https://mow-basket-cdn-31.geobasket.ru/vol{vol}/part{part}/{product_id}/images/c516x688/{image_id}.webp"

class CrowlerDefaults:
    TIMEOUT = 60
    RETRIES = 3
    RUN_HTTP = True


class WildberriesCrawler:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.client: httpx.Client | None = None
        self.last_request_url: str | None = None

    def init_session(self) -> None:
        self.client = httpx.Client(
            timeout=self.config.get("timeout", CrowlerDefaults.TIMEOUT),
            follow_redirects=True,
        )

    def close(self) -> None:
        if self.client is not None:
            self.client.close()
            self.client = None

    def crawl(self) -> dict[str, Any]:
        if not self.config.get("run_http", False):
            return {}

        if self.client is None:
            self.init_session()

        if self.client is None:
            raise RuntimeError("HTTP client is not initialized")

        retries = int(self.config.get("retries", CrowlerDefaults.RETRIES))
        last_error: Exception | None = None

        for url in self.config.get("base_urls", WBDefaults.BASE_URLS):
            for attempt in range(1, retries + 1):
                try:
                    req_url, params, headers = self._build_search_request(url)
                    self.last_request_url = str(httpx.URL(req_url, params=params))
                    response = self.client.get(req_url, params=params, headers=headers)
                    if response.status_code != 200:
                        raise RuntimeError(f"WB request failed with status {response.status_code}")
                    return response.json()
                except (httpx.TimeoutException, httpx.HTTPError, ValueError, RuntimeError) as exc:
                    last_error = exc
                    if attempt < retries:
                        continue

        if last_error is not None:
            raise RuntimeError(f"WB request failed after retries: {last_error}") from last_error
        raise RuntimeError("WB request failed: unknown error")

    def _build_search_request(self, base_url: str) -> tuple[str, dict[str, Any], dict[str, str]]:
        query = str(self.config.get("query", WBDefaults.SEARCH_WORD))
        params = {
            'ab_testing': False,
            'appType': 1,
            'curr': 'rub',
            'dest': '-1257786',
            'hide_vflags': '4294967296',
            'inheritFilters': False,
            'lang': 'ru',
            'query': query,
            'resultset': 'catalog',
            'sort': 'popular',
            'spp': self.config.get("page_size", WBDefaults.PAGE_SIZE),
            'suppressSpellcheck': False,
        }
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'application/json, text/plain, */*',
            'Referer': f'https://www.wildberries.ru/catalog/0/search.aspx?search={quote_plus(query)}',
            'Cookie': 'x_wbaas_token=1.1000.6538e61eaef841f28ab9260fbc9dd6ef.MHwyYTAwOjEzNzA6ODE5Yzo0Y2RkOjMyOTY6NDIyYzpiYmE0OjI4ZjN8TW96aWxsYS81LjAgKFgxMTsgTGludXggeDg2XzY0OyBydjoxNDAuMCkgR2Vja28vMjAxMDAxMDEgRmlyZWZveC8xNDAuMHwxNzc3MzkwODQ3fHJldXNhYmxlfDJ8ZXlKb1lYTm9Jam9pSW4wPXwwfDN8MTc3Njc4NjA0N3wx.MEUCIQC2KwJV+lmmyqeYyXe82smBLlLDLK6DRuPCqwCJDzcfGAIgEmZma4OQqT3HOHwr9LRD7gOrQ4/5BQwTpqolMOTMI3w=; _wbauid=7197725391776181249; _cp=1',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0',
            'X-Queryid': 'qid719772539177618124920260414154104',
            'X-Requested-With': 'XMLHttpRequest',
            'X-Spa-Version': '14.5.5',
            'X-Userid': '0',
        }
        return base_url, params, headers


class WildberriesParser:
    def parse(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        if not isinstance(payload, dict):
            return []

        products = []
        raw_products = self._extract_products(payload)
        for product in raw_products:
            products.append(self.parse_product_card(product))
        return products

    def parse_product_card(self, card: dict[str, Any]) -> dict[str, Any]:
        sizes = card.get("sizes") if isinstance(card.get("sizes"), list) else []
        return {
            "product_url": WBDefaults.PRODUCT_URL.format(product_id=card.get("id")),
            "sku": str(card.get("id")) if card.get("id") is not None else None,
            "name": card.get("name"),
            "price": self._extract_price(card),
            # "description": self._pick_first(card, ["description"]), # Не понял что имеется ввиду, продукт контент скачивается отдельно для каждого продукта, к чему он в ранкинге?
            "image_urls": self._build_image_urls(card.get("id"), card.get("pics", 0)),
            # "characteristics": , # тоже самое что и с описанием
            "seller_name": card.get("supplier"),
            "seller_url": WBDefaults.SELLER_URL.format(seller_id=card.get("supplier_id")),
            "sizes": self._extract_sizes(sizes),
            "stock": card.get("totalQuantity"),
            "rating": card.get("reviewRating"),
            "reviews_count": card.get("feedbacks"),
            "production_country": "Россия", # тоже что и с описанием, но нужно для агрегации
        }

    @staticmethod
    def _build_image_urls(product_id: Any, pics_count: Any) -> list[str]:
        if product_id is None:
            return []

        product_id_str = str(product_id)
        vol = product_id_str[:4]
        part = product_id_str[:6]

        try:
            total_images = int(pics_count)
        except (TypeError, ValueError):
            total_images = 0

        if total_images <= 0:
            return []

        return [
            WBDefaults.IMAGES_URL.format(
                vol=vol,
                part=part,
                product_id=product_id_str,
                image_id=image_id,
            )
            for image_id in range(1, total_images + 1)
        ]

    @staticmethod
    def _pick_first(source: dict[str, Any], keys: list[str], default: Any = None) -> Any:
        for key in keys:
            value = source.get(key)
            if value is not None:
                return value
        return default

    @staticmethod
    def _extract_price(card: dict[str, Any]) -> float | None:
        direct_price = card.get("price")
        if isinstance(direct_price, (int, float)):
            return float(direct_price)

        sizes = card.get("sizes")
        if not isinstance(sizes, list):
            return None

        for size in sizes:
            if not isinstance(size, dict):
                continue
            price_block = size.get("price")
            if isinstance(price_block, (int, float)):
                return float(price_block)
            if isinstance(price_block, dict):
                for key in ("total", "product", "basic"):
                    value = price_block.get(key)
                    if isinstance(value, (int, float)):
                        return float(value) / 100 if value > 10000 else float(value)
        return None

    @staticmethod
    def _extract_sizes(sizes: list[Any]) -> list[str]:
        result: list[str] = []
        for size in sizes:
            if isinstance(size, dict):
                value = size.get("name") or size.get("origName")
                if value:
                    result.append(str(value))
            elif size is not None:
                result.append(str(size))
        return result

    @staticmethod
    def _extract_products(payload: dict[str, Any]) -> list[dict[str, Any]]:
        candidates = [
            payload.get("products"),
            payload.get("data", {}).get("products") if isinstance(payload.get("data"), dict) else None,
            payload.get("result", {}).get("products") if isinstance(payload.get("result"), dict) else None,
        ]
        for candidate in candidates:
            if isinstance(candidate, list):
                return [item for item in candidate if isinstance(item, dict)]
        return []


def main() -> None:
    args = _parse_args()
    config = {
        "query": args.query,
        "page_size": args.page_size,
        "timeout": args.timeout,
        "retries": args.retries,
        "run_http": args.run_http,
        "base_urls": WBDefaults.BASE_URLS,
    }
    crawler = WildberriesCrawler(config=config)
    parser = WildberriesParser()
    loader = XlsxLoader()
    aggregator = CatalogAggregator()

    try:
        payload = crawler.crawl()
    finally:
        crawler.close()

    if crawler.last_request_url:
        print(f"Request URL: {crawler.last_request_url}")

    products = parser.parse(payload)
    if not products:
        raise RuntimeError("No products parsed. Check request params/headers and parser key paths.")
    filtered_catalog = aggregator.filter_for_assignment(products)

    loader.save(products, WBDefaults.OUTPUT_CATALOG, WBDefaults.SEARCH_WORD)
    loader.save(filtered_catalog, WBDefaults.OUTPUT_FILTERED, WBDefaults.SEARCH_WORD)
    print(
        "WB scaffold finished. "
        f"Saved {len(products)} rows to {WBDefaults.OUTPUT_CATALOG} and "
        f"{len(filtered_catalog)} rows to {WBDefaults.OUTPUT_FILTERED}."
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="WB catalog scaffold")
    parser.add_argument("--query", default=WBDefaults.SEARCH_WORD, help="Search query")
    parser.add_argument("--page-size", type=int, default=WBDefaults.PAGE_SIZE, help="WB spp/page size")
    parser.add_argument("--timeout", type=int, default=CrowlerDefaults.TIMEOUT, help="Request timeout (seconds)")
    parser.add_argument("--retries", type=int, default=CrowlerDefaults.RETRIES, help="Retries per endpoint")
    parser.add_argument(
        "--run-http",
        action=argparse.BooleanOptionalAction,
        default=CrowlerDefaults.RUN_HTTP,
        help="Enable real HTTP requests",
    )
    return parser.parse_args()


if __name__ == "__main__":
    main()
