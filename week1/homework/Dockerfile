FROM python:3.9

RUN pip install pandas sqlalchemy psycopg2
RUN apt-get install wget
WORKDIR /app
COPY pipeline.py pipeline.py

ENTRYPOINT [ "python", "pipeline.py" ]