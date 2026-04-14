# WB parser (verba test)

Текущий этап: только абстракции, без рабочей логики парсинга/краулинга.

Структура максимально приближена к подходу `profitero`:
- отдельный контракт краулера (`init_session`, `crawl`, `post_crawl`)
- отдельный контракт парсера (`parse` + обработчики статусов)
- отдельный контракт лоадера

## Структура

- `src/crawler.py` — базовый crawler-интерфейс
- `src/parser.py` — базовый parser-интерфейс
- `src/loader.py` — базовый loader-интерфейс + схема `ProductRecord` + заготовка `XlsxLoader`
- `shops/wb.py` — WB-специфичные классы, константы (`WBDefaults`) и entrypoint

## Запуск

```bash
python shops/wb.py
```