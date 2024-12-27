import os

from ruamel.yaml import YAML

from util.log import get_logger

env = os.getenv("ENV", "dev")
yaml = YAML()
logger = get_logger("util.conf")


def read_yaml(yaml_path: str) -> dict:
    with open(yaml_path, "r") as f_yaml:
        datas = yaml.load(f_yaml)
    return datas


def get_conf() -> dict:
    if env == "dev":
        conf_path = "./conf/default.yaml"
    else:
        conf_path = f"./conf/default.{env}.yaml"
    conf_data = read_yaml(conf_path)
    conf_data["env"] = env
    return conf_data
