"""
Extract-парсинг JSON-поля context_json.

Часть пайплайна mysql_replica → local-postgres.
Используется в etl_pipeline.ResumeETL перед загрузкой батча в staging.

Роль в пайплайне:
    - из большого JSON-поля context_json вытаскиваются только нужные поля:
      SKU (positionId + productSKU) и coupon_rule_id (ruleId);
    - используется предкомпилированный regex вместо json.loads,
      потому что полная десериализация дорога, а нам нужны 2 поля;
    - regex-подход не падает на невалидном JSON.
"""
import re


class SimpleParser:
    """Упрощённый парсер: только SKU и купоны."""

    # Предкомпилированные regex для скорости.
    # DOTALL — чтобы .* переходил через переводы строк внутри JSON.
    SKU_PATTERN = re.compile(
        r'"positionId":\s*\d+.*?"productSKU":\s*"(\d+)"',
        re.DOTALL,
    )
    COUPON_PATTERN = re.compile(r'"ruleId"\s*:\s*"?(\d+)"?')

    @staticmethod
    def parse_skus(json_text):
        """Быстрый парсинг SKU через предкомпилированный regex."""
        if not json_text:
            return ''

        matches = SimpleParser.SKU_PATTERN.findall(json_text)
        return '|'.join(sorted(matches)) if matches else ''

    @staticmethod
    def parse_coupon_rule_id(json_text):
        """Быстрый парсинг ruleId (id купонного правила)."""
        if not json_text:
            return None

        match = SimpleParser.COUPON_PATTERN.search(json_text)
        if match:
            try:
                return int(match.group(1))
            except (ValueError, TypeError):
                return None

        return None

    @staticmethod
    def parse_minimal_fields(json_text):
        """Парсит только 2 поля: SKU и coupon_rule_id."""
        return {
            'skus': SimpleParser.parse_skus(json_text),
            'coupon_rule_id': SimpleParser.parse_coupon_rule_id(json_text),
        }