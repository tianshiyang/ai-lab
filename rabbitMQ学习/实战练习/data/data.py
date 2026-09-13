import json
from pathlib import Path

data_dir = Path(__file__).parent


def load(name):
    with open(data_dir / f"{name}.json", encoding="utf-8") as f:
        return json.load(f)


mock_config = load("mock_config")

requests = load("requests")

templates = load("templates")

users = load("users")

