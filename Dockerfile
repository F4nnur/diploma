FROM apache/airflow:2.9.0-python3.10
COPY requirements.txt /requirements.txt
RUN pip3 install --upgrade pip
RUN pip3 install -r /requirements.txt