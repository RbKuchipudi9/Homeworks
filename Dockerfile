# getting base image python
FROM python

RUN pip install pandas


WORKDIR /app
COPY pipeline.py pipeline.py

ENTRYPOINT [ "python", "pipeline.py" ]