#!/bin/bash
cd || exit
cd /home/hyper/scripts/stock_pipeline || exit
python -c 'from myKafkaCassandra import main_realtime; main_realtime();'