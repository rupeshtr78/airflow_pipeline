import numpy as np
import datetime
from util.config import config
from airflowPipeline.pipeline.yamlLogger import setup_logging
import logging
from kafka import KafkaProducer
import schedule



def stock_fake(producer,symbol):
    close=4000
    close=close+np.random.uniform(-200,200)
    value={"symbol":symbol,
       "time":str(datetime.datetime.now().replace(microsecond=0)),
       "open":round(close+np.random.uniform(-1,1),2),
       "high":round(close+np.random.uniform(0,1),2),
       "low":round(close+np.random.uniform(-1,0),2),
       "close":round(close,2),
       "volume":round(np.random.uniform(0,1)*6e9,0)
           }
    producer.send(topic=config['topic_name1'], value=bytes(str(value), 'utf-8'))
    producer.flush()




def run_producer():
    setup_logging()
    logger = logging.getLogger('airflowpipeline')
    logger.info("Starting Kafka Producer")
    try:
        producer = KafkaProducer(bootstrap_servers=config['kafka_broker'])
        schedule.every(10).seconds.do(stock_fake,producer,'RTR')
        logger.info(f"Sending Stock data to Kafka ... ")
        producer.flush()
        while True:
            schedule.run_pending()
    except Exception as e:
        logger.error("Stocks Kafka Producer Failed",exc_info=True)







if __name__ == '__main__':

    run_producer()

