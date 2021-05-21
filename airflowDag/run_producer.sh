#!/bin/bash
cd || exit
cd /home/hyper/scripts/stock_pipeline || exit
python -c 'from myKafkaProducer import run_producer; run_producer();'
