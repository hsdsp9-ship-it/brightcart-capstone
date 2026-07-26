import os
import logging
import pytest


# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------

class TestGetConfig:
    def setup_method(self):
        os.environ.pop("CATALOG", None)
        os.environ.pop("SCHEMA", None)

    def teardown_method(self):
        os.environ.pop("CATALOG", None)
        os.environ.pop("SCHEMA", None)

    def test_returns_all_expected_keys(self):
        from notebooks.config import get_config
        cfg = get_config()
        expected = {
            "catalog", "schema", "raw_volume", "checkpoint_volume",
            "customers_path", "products_path", "orders_path",
            "incoming_orders_dir", "schema_tracking_dir",
        }
        assert expected == set(cfg.keys())

    def test_default_catalog_and_schema(self):
        from notebooks.config import get_config
        cfg = get_config()
        assert cfg["catalog"] == "harpalsingh"
        assert cfg["schema"] == "brightcart"

    def test_env_override(self):
        from notebooks.config import get_config
        os.environ["CATALOG"] = "test_cat"
        os.environ["SCHEMA"] = "test_schema"
        cfg = get_config()
        assert cfg["catalog"] == "test_cat"
        assert cfg["schema"] == "test_schema"

    def test_paths_derived_from_catalog_schema(self):
        from notebooks.config import get_config
        os.environ["CATALOG"] = "mycat"
        os.environ["SCHEMA"] = "myschema"
        cfg = get_config()
        assert cfg["raw_volume"] == "/Volumes/mycat/myschema/raw_data"
        assert cfg["checkpoint_volume"] == "/Volumes/mycat/myschema/checkpoints"
        for key in ("customers_path", "products_path", "orders_path", "incoming_orders_dir"):
            assert cfg[key].startswith(cfg["raw_volume"]), f"{key} does not start with raw_volume"
        assert cfg["schema_tracking_dir"].startswith(cfg["checkpoint_volume"])

    def test_custom_defaults_respected(self):
        from notebooks.config import get_config
        cfg = get_config(catalog_default="custom_cat", schema_default="custom_schema")
        assert cfg["catalog"] == "custom_cat"
        assert cfg["schema"] == "custom_schema"

    def test_env_takes_precedence_over_custom_defaults(self):
        from notebooks.config import get_config
        os.environ["CATALOG"] = "env_cat"
        os.environ["SCHEMA"] = "env_schema"
        cfg = get_config(catalog_default="ignored_cat", schema_default="ignored_schema")
        assert cfg["catalog"] == "env_cat"
        assert cfg["schema"] == "env_schema"


# ---------------------------------------------------------------------------
# logging_helper
# ---------------------------------------------------------------------------

class TestGetLogger:
    def test_returns_logger_instance(self):
        from notebooks.logging_helper import get_logger
        assert isinstance(get_logger("test.instance"), logging.Logger)

    def test_logger_name(self):
        from notebooks.logging_helper import get_logger
        assert get_logger("test.name").name == "test.name"

    def test_has_standard_level_methods(self):
        from notebooks.logging_helper import get_logger
        logger = get_logger("test.levels")
        for method in ("debug", "info", "warning", "error", "critical"):
            assert callable(getattr(logger, method, None)), f"missing method: {method}"

    def test_same_name_returns_same_instance(self):
        from notebooks.logging_helper import get_logger
        assert get_logger("test.singleton") is get_logger("test.singleton")

    def test_log_level_env_var(self):
        os.environ["LOG_LEVEL"] = "DEBUG"
        root = logging.getLogger()
        for h in root.handlers[:]:
            root.removeHandler(h)
        from notebooks.logging_helper import get_logger
        get_logger("test.level_env")
        assert root.level == logging.DEBUG
        os.environ.pop("LOG_LEVEL", None)
