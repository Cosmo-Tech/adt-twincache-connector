FROM python:3.10

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY ADT_TwinCache_Connector/ ADT_TwinCache_Connector/
COPY main.py main.py
CMD python main.py