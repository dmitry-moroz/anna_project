import yaml


def load_conf(path):
    try:
        with open(path) as conf_file:
            return yaml.load(conf_file)
    except (yaml.YAMLError, OSError, IOError):
        raise


CONFIG = load_conf('config.yml')
cache_conf = CONFIG['cache']
srv_conf = CONFIG['server']
db_conf = CONFIG['database']
