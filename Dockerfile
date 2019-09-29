FROM alpine:3.9

WORKDIR /app

RUN apk add --update --no-cache python3 python3-dev gcc g++

RUN pip3 install --upgrade pip

RUN apk add --virtual scipy-build \
        build-base python-dev openblas-dev freetype-dev pkgconfig gfortran \
    && ln -s /usr/include/locale.h /usr/include/xlocale.h


# install requirements
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# load app into image
COPY run.py .
COPY better better

ENV PYTHONUNBUFFERED 1

ENTRYPOINT ["python3", "run.py"]
