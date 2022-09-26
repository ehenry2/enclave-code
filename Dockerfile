FROM python:3.9-slim

RUN mkdir -p /app
WORKDIR /app
RUN python3 -m venv /app/env && \
    /app/env/bin/pip install -U pip setuptools wheel && \
    /app/env/bin/pip install pyarrow
COPY execute.py /app/
ENTRYPOINT ["/app/env/bin/python3", "/app/execute.py"]
