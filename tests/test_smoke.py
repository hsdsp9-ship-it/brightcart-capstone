def test_get_config_defaults():
    import os
    os.environ.pop("CATALOG", None)
    os.environ.pop("SCHEMA", None)
    from notebooks.config import get_config
    cfg = get_config()
    assert "catalog" in cfg and "schema" in cfg


def test_get_logger():
    from notebooks.logging_helper import get_logger
    logger = get_logger("test_smoke")
    assert hasattr(logger, "info")
