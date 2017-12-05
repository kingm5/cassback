FROM python:2

COPY . /cassback

RUN pip install /cassback

CMD ["cassback", "--help"]
