FROM python:3.11-slim

ARG $HTTP_PROXY

ENV HTTP_PROXY $HTTP_PROXY
ENV HTTPS_PROXY $HTTP_PROXY

RUN pip install --upgrade pip

COPY . /app

COPY _CI/entrypoint.sh /app

WORKDIR /app

RUN chmod +x entrypoint.sh

RUN pip install -r _CI/requirements.txt