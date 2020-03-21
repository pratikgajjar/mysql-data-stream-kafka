import logging.config
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

LOGGER = {
  'version': 1,
  'disable_existing_loggers': False,
  'formatters': {
    'verbose': {
      'format': ("[%(asctime)s] %(levelname)s "
                 "[%(name)s:%(lineno)s] %(message)s"),
      'datefmt': "%d/%b/%Y %H:%M:%S",
    },
    'simple': {
      'format': '%(levelname)s %(message)s',
    },
  },
  'handlers': {
    'console': {
      'class': 'logging.StreamHandler',
      'formatter': 'verbose',
      'level': 'INFO',
    },
    'file': {
      'class': 'logging.handlers.RotatingFileHandler',
      'maxBytes': 1024 * 1024 * 100, # 100 MB
      'backupCount': 5,
      'filename': 'aws-stream.log',
      'formatter': 'verbose',
      'level': logging.DEBUG,
    },
  },
  'root': {
    'level': 'DEBUG',
    'handlers': ['console', 'file']
  },
}


def setup_logging():
  # Add the log message handler to the logger
  LOG_FILENAME = os.path.join(BASE_DIR, 'logs', os.getenv('SCRIPT_NAME', 'aws-stream') + '.log')
  LOGGER['handlers']['file']['filename'] = LOG_FILENAME
  logging.config.dictConfig(LOGGER)
  root = logging.getLogger()
  root.setLevel(os.getenv('LOG_LEVEL', logging.INFO))
