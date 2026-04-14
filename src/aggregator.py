from __future__ import annotations

from typing import Any


class CatalogAggregator:
    """
    Aggregation/filtering rules for the second XLSX.
    """

    def filter_for_assignment(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for record in records:
            if not self._has_min_rating(record, 4.5):
                continue
            if not self._has_max_price(record, 10000):
                continue
            if not self._is_country(record, "россия"):
                continue
            result.append(record)
        return result

    @staticmethod
    def _has_min_rating(record: dict[str, Any], threshold: float) -> bool:
        value = record.get("rating")
        if value is None:
            return False
        try:
            return float(value) >= threshold
        except (TypeError, ValueError):
            return False

    @staticmethod
    def _has_max_price(record: dict[str, Any], ceiling: float) -> bool:
        value = record.get("price")
        if value is None:
            return False
        try:
            return float(value) <= ceiling
        except (TypeError, ValueError):
            return False

    @staticmethod
    def _is_country(record: dict[str, Any], expected_country: str) -> bool:
        value = record.get("production_country")
        if not value:
            return False
        return str(value).strip().lower() == expected_country
