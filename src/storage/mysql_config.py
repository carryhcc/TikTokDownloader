from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..config import Parameter

DEFAULT_MYSQL_CONFIG = {
    "mysql_host": "127.0.0.1",
    "mysql_port": 3306,
    "mysql_user": "root",
    "mysql_password": "",
    "mysql_database": "douk_downloader",
    "mysql_table": "douk_records",
    "mysql_comment_table": "comment_data",
}


def resolve_mysql_logger_params(parameter: "Parameter", storage_type: str) -> dict[str, Any]:
    cache = getattr(parameter, "_mysql_logger_config_cache", None)
    if cache is None:
        settings_data = _safe_read_settings(parameter)
        cache = {}
        for key, default in DEFAULT_MYSQL_CONFIG.items():
            if hasattr(parameter, key):
                cache[key] = getattr(parameter, key)
            else:
                cache[key] = settings_data.get(key, default)
        setattr(parameter, "_mysql_logger_config_cache", cache)

    data = cache.copy()
    data["storage_type"] = storage_type
    return data


def _safe_read_settings(parameter: "Parameter") -> dict[str, Any]:
    settings = getattr(parameter, "settings", None)
    if not settings or not hasattr(settings, "read"):
        return {}
    try:
        data = settings.read()
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}
