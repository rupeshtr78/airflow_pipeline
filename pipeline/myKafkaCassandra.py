import ast
from util.util import string_to_float,symbol_list
from util.config import config
from kafka import KafkaConsumer
from cassandra.cluster import Cluster,NoHostAvailable
from airflowPipeline.pipeline.yamlLogger import setup_logging
import logging

class CassandraStorage(object):

    """
    Kafka consumer reads the message from topic and store the received data in Cassandra database

    """
    def __init__(self,symbol):
        if symbol=='RTR':
            self.symbol='RTR'
        else:
            self.symbol=symbol

        self.key_space=config['key_space']

        # init a Cassandra cluster instance
        cluster = Cluster([config['cassandraHost']])

        # start Cassandra server before connecting
        self.logger = logging.getLogger('airflowpipeline')
        try:
            self.session = cluster.connect()
            self.logger.info("Connected to Cassandra Cluster")
        except NoHostAvailable:
            self.logger.error("Fatal Error: Unable to connect with Cassandra cluster",exc_info=True)
        else:
            self.create_table()




    # create Cassandra table of stock if not exist
    def create_table(self):

        self.session.execute("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'} AND durable_writes = 'true'" % config['key_space'])
        self.session.set_keyspace(self.key_space)

        # create table for intraday data
        self.session.execute("CREATE TABLE IF NOT EXISTS {} ( \
                                    	TIME timestamp,           \
                                    	SYMBOL text,              \
                                    	OPEN float,               \
                                    	HIGH float,               \
                                    	LOW float,                \
                                    	CLOSE float,              \
                                    VOLUME float,             \
                                    PRIMARY KEY (SYMBOL,TIME));".format(self.symbol))





    # initialize a Kafka consumer
    def kafka_consumer(self):

        try:
            self.consumer1 = KafkaConsumer(
                config['topic_name1'],
                bootstrap_servers=config['kafka_broker'])
            self.consumer2 = KafkaConsumer(
                config['topic_name2'],
                bootstrap_servers=config['kafka_broker'])

            self.consumer3 = KafkaConsumer(
                'news',
                bootstrap_servers=config['kafka_broker'])
        except Exception:
            self.logger.error("Kafka Broker not Reachable",exc_info=True)


        # store streaming data of 1min frequency to Cassandra database

    def stream_to_cassandra(self):

        try:

            for msg in self.consumer1:
                # decode msg value from byte to utf-8
                dict_data=ast.literal_eval(msg.value.decode("utf-8"))

                # transform price data from string to float
                for key in ['open', 'high', 'low', 'close', 'volume']:
                    dict_data[key]=string_to_float(dict_data[key])

                query="INSERT INTO {}(time, symbol,open,high,low,close,volume) VALUES ('{}','{}',{},{},{},{},{});".format(self.symbol, dict_data['time'],
                                                                                                                          dict_data['symbol'],dict_data['open'],
                                                                                                                          dict_data['high'],dict_data['low'],dict_data['close'],dict_data['volume'])


                self.session.execute(query)
                self.logger.info("Stored {}\'s min data at {}".format(dict_data['symbol'],dict_data['time']))
        except Exception:
            self.logger.error("Kafka Consumer Error",exc_info=True)



    def delete_table(self,table_name):
            self.session.execute("DROP TABLE {}".format(table_name))


def main_realtime(symbol='RTR',tick=True):
    setup_logging()
    # logger = logging.getLogger('airflowpipeline')
    database=CassandraStorage(symbol)
    database.kafka_consumer()
    logger = database.logger

    try:
        database.stream_to_cassandra()
    except Exception:
        logger.error("Kafka Consumer or Cassandra connection Error",exc_info=True)



if __name__ == '__main__':
    main_realtime()