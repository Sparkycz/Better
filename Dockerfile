FROM alpine:3.7

WORKDIR /app

RUN apk add --update --no-cache python3 python3-dev

RUN pip3 install --upgrade pip

# install requirements
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# load app into image
COPY run.py .
COPY better better

ENV PYTHONUNBUFFERED 1

ENTRYPOINT ["python3", "run.py"]
