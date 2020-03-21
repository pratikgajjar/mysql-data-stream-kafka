import datetime
import decimal
import json
import logging
import os
import signal

from kafka import KafkaProducer
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
  UpdateRowsEvent,
  WriteRowsEvent,
)

from logger_config import setup_logging

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def get_config():
  config_file_path = os.path.join(BASE_DIR, 'configuration.json')
  if not os.path.exists(config_file_path):
    exit('Configuration file not found, run "cp configuration.json.default configuration.json"')
  with open(config_file_path) as f:
    return json.load(f)


def dump_mysql_replica_pos(data):
  with open(os.path.join(BASE_DIR, 'mysql-replica-pos.json'), 'w') as f:
    data['resume_stream'] = True
    json.dump(data, f)
    f.close()


def get_mysql_replica_pos():
  replica_pos_file_path = os.path.join(BASE_DIR, 'mysql-replica-pos.json')
  if not os.path.exists(replica_pos_file_path):
    logging.info('Replication progress not found. Will start generating stream from currant time.')
    return None

  with open(replica_pos_file_path) as f:
    logging.info('Replication progress found, will continue from there.')
    return json.load(f)


class StreamParser:
  TYPE_FUN_MAP = {
    'datetime': lambda x: x.__str__(),
    'float': lambda x: float(x),
    'recurse': lambda x: StreamParser.dict_to_byte_string(x, True),
    'no_op': lambda x: x
  }

  OP_MAP = {
    datetime.datetime: TYPE_FUN_MAP['datetime'],
    datetime.date: TYPE_FUN_MAP['datetime'],
    datetime.timedelta: TYPE_FUN_MAP['datetime'],
    decimal.Decimal: TYPE_FUN_MAP['float'],
    dict: TYPE_FUN_MAP['recurse']
  }

  @staticmethod
  def dict_to_byte_string(data, recurse=False):
    for key in data:
      value = data[key]
      if type(value) in StreamParser.OP_MAP:
        lambda_fun = StreamParser.OP_MAP[type(value)]
        data[key] = lambda_fun(value)

    if recurse:
      return data

    return bytes(json.dumps(data).encode('utf8'))


class DbStream:
  kafka_producer = KafkaProducer
  bin_log_stream_reader = BinLogStreamReader

  def __init__(self, config, replica_resume=None):
    self.producer = KafkaProducer(bootstrap_servers=config['kafka_brokers'])
    replica_resume = {'resume_stream': False} if not replica_resume else replica_resume
    self.bin_log_stream_reader = BinLogStreamReader(
      connection_settings=config['mysql_config'],
      server_id=100,
      blocking=True,
      only_events=[WriteRowsEvent, UpdateRowsEvent],
      only_schemas=config['whitelist_db_stream'].keys(),
      **replica_resume
    )

  def read_stream(self):
    for binlog_event in self.bin_log_stream_reader:
      for row in binlog_event.rows:
        event = {"schema": binlog_event.schema,
                 "table": binlog_event.table,
                 "type": type(binlog_event).__name__,
                 "row": row
                 }
        logging.debug(event)
        self.process_event(event)

  def process_event(self, event):
    if event['table'] in config['whitelist_db_stream'][event['schema']]:
      self.producer.send(topic=event['table'], value=StreamParser.dict_to_byte_string(event))

  def save_replication_progress(self):
    dump_mysql_replica_pos({
      'log_file': self.bin_log_stream_reader.log_file,
      'log_pos': self.bin_log_stream_reader.log_pos
    })

  def graceful_exit(self):
    try:
      self.producer.flush()
      logging.info('Flushed background queue to broker.')
    except:
      logging.error('Producer flush unsuccessful.')
    self.save_replication_progress()
    logging.info('Gracefully exited.')
    exit(0)

  def signal_handler(self, signal, frame):
    logging.info(f'Received signal {signal}, will gracefully exit.')
    self.graceful_exit()

if __name__ == "__main__":
  setup_logging()
  config = get_config()
  mysql_replica_pos = get_mysql_replica_pos()
  db = DbStream(config, mysql_replica_pos)
  # Handle all type of signal to always exit in graceful condition
  signal.signal(signal.SIGTERM, db.signal_handler)
  signal.signal(signal.SIGINT, db.signal_handler)
  signal.signal(signal.SIGQUIT, db.signal_handler)
  # Start reading mysql replica stream and push into kafka topic
  try:
    db.read_stream()
  except Exception as e:
    logging.error(e, exc_info=True)
    db.graceful_exit()