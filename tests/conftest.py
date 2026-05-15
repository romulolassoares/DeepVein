import os

# Must be set before any src.* import so load_config.py picks up the test file
os.environ["DEEPVEIN_CONFIG_PATH"] = "tests/fixtures/config.yml"
