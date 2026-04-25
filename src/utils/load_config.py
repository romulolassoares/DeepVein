import os
import yaml

from pathlib import Path

from .logger import get_logger

CONFIG_PATH_ENV_VAR = "DEEPVEIN_CONFIG_PATH"
_DEFAULT_CONFIG_PATH = Path("config/config.yml")


logger = get_logger(__name__)


def _try_load_dot_env() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv()


_try_load_dot_env()

_config_path = Path(os.environ.get(CONFIG_PATH_ENV_VAR, _DEFAULT_CONFIG_PATH))
logger.info("Loading configuration from '%s'", _config_path)

try:
    with _config_path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
except FileNotFoundError as e:
    logger.exception("Configuration file not found at '%s'", _config_path)
    raise FileNotFoundError(
        f"Config file not found: {_config_path} "
        f"(set {CONFIG_PATH_ENV_VAR} or create the file at the default path)"
    ) from e
except yaml.YAMLError as e:
    logger.exception("YAML parsing error while loading '%s'", _config_path)
    raise yaml.YAMLError(f"Error loading YAML from {_config_path}") from e
except Exception as e:
    logger.exception("Unexpected error while loading config '%s'", _config_path)
    raise RuntimeError(f"Error loading config file {_config_path}: {e}") from e

logger.info("Configuration file loaded successfully")
