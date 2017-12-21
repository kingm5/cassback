FROM python:2

COPY . /cassback

RUN pip install /cassback

ENTRYPOINT ["/usr/local/bin/cassback"]
