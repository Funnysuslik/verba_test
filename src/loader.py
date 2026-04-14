from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from openpyxl import Workbook
from pydantic import BaseModel, ConfigDict, Field, ValidationError


REQUIRED_COLUMNS = [
    "product_url",
    "sku",
    "name",
    "price",
    "description",
    "image_urls",
    "characteristics",
    "seller_name",
    "seller_url",
    "sizes",
    "stock",
    "rating",
    "reviews_count",
    "production_country",
]


class ProductRecord(BaseModel):
    model_config = ConfigDict(strict=False, extra="ignore")

    product_url: str
    sku: str
    name: str
    price: float

    description: str | None = None
    image_urls: list[str] = Field(default_factory=list)
    characteristics: dict[str, Any] | str = Field(default_factory=dict)
    seller_name: str | None = None
    seller_url: str | None = None
    sizes: list[str] = Field(default_factory=list)
    stock: int | None = None
    rating: float | None = None
    reviews_count: int | None = None
    production_country: str | None = None


class XlsxLoader:
    """
    Simple XLSX writer.
    Keeps field names explicit so parser and aggregator stay decoupled.
    """

    def normalize_records(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for record in records:
            validated = self._validate_record(record)
            row: dict[str, Any] = {}
            for key in REQUIRED_COLUMNS:
                row[key] = getattr(validated, key)

            row["description"] = validated.description
            row["image_urls"] = ", ".join(self._to_str_list(validated.image_urls))
            row["characteristics"] = self._dump_characteristics(validated.characteristics)
            row["seller_name"] = validated.seller_name
            row["seller_url"] = validated.seller_url
            row["sizes"] = ", ".join(self._to_str_list(validated.sizes))
            row["stock"] = validated.stock
            row["rating"] = validated.rating
            row["reviews_count"] = validated.reviews_count
            row["production_country"] = validated.production_country
            normalized.append(row)
        return normalized

    def save(self, records: list[dict[str, Any]], output_path: str | Path, search_word: str) -> None:
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)

        workbook = Workbook()
        sheet = workbook.active
        sheet.title = f"catalog_{search_word}"
        sheet.append(REQUIRED_COLUMNS)

        for row in self.normalize_records(records):
            sheet.append([row.get(column) for column in REQUIRED_COLUMNS])

        workbook.save(output)

    @staticmethod
    def _validate_record(record: dict[str, Any]) -> ProductRecord:
        prepared = dict(record)
        if "name" not in prepared and "title" in prepared:
            prepared["name"] = prepared.get("title")

        try:
            return ProductRecord.model_validate(prepared)
        except ValidationError as exc:
            errors = "; ".join(
                f"{'.'.join(str(part) for part in err['loc'])}: {err['msg']}"
                for err in exc.errors()
            )
            raise ValueError(f"Invalid record for export: {errors}") from exc

    @staticmethod
    def _to_str_list(value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item) for item in value if item is not None]
        return [str(value)]

    @staticmethod
    def _dump_characteristics(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        if isinstance(value, dict):
            return json.dumps(value, ensure_ascii=False)
        return str(value)
