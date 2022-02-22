#!/bin/bash
ETL_DIR="/app"
cd $ETL_DIR && pipenv run python3 aircall_daily_load_main.py --execution-date $1
