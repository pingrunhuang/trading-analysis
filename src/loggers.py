import logging
import logging.config
import yaml

with open("loggings.yaml", encoding="utf-8") as f:
    configs = yaml.safe_load(f)
    if isinstance(configs, dict):
        logging.config.dictConfig(config=configs)
