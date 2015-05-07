import yaml

PATH = 'config.yml'

def load_conf(path):
    """Loads yml config file.

    path: path to target config
    """
    try:
        with open(path) as conf_file:
            return yaml.load(conf_file)
    except (yaml.YAMLError, OSError, IOError):
        raise

CONFIG = load_conf(PATH)
cache_conf = CONFIG['cache']
srv_conf = CONFIG['server']
db_conf = CONFIG['database']
