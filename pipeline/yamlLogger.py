import os
import logging.config
import logging
import yaml

def setup_logging(
        default_path='util/logging.yaml',
        default_level=logging.INFO,
        env_key='LOG_CFG'
):
    """Setup logging configuration

    """
    path = default_path

    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    else:
        logging.info("Unable to Read Log configuration Yaml")





if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger('airflowpipeline')
    logger.info("Run Yaml Logger")


