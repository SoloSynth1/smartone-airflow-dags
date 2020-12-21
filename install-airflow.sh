#!/usr/bin/env bash

source ./venv/bin/activate

pip install apache-airflow==1.10.13 \
 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-1.10.13/constraints-3.8.txt" &&\
 airflow initdb