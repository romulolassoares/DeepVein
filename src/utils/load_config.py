import os
from pathlib import Path

import yaml

CONFIG_PATH_ENV_VAR = "DEEPVEIN_CONFIG_PATH"
_DEFAULT_CONFIG_PATH = Path("config/config.yml")


def _try_load_dot_env() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv()


_try_load_dot_env()

_config_path = Path(os.environ.get(CONFIG_PATH_ENV_VAR, _DEFAULT_CONFIG_PATH))

try:
    with _config_path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
except FileNotFoundError as e:
    raise FileNotFoundError(
        f"Config file not found: {_config_path} "
        f"(set {CONFIG_PATH_ENV_VAR} or create the file at the default path)"
    ) from e
except yaml.YAMLError as e:
    raise yaml.YAMLError(f"Error loading YAML from {_config_path}") from e
except Exception as e:
    raise RuntimeError(f"Error loading config file {_config_path}: {e}") from e


print(config)