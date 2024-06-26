FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

#ENV HOST 127.0.0.1
#ENV PORT 8080
#ENV PROTOCOL ws

EXPOSE 8080

COPY distributed/ /app/distributed/
COPY pyproject.toml /app/
RUN pip install --upgrade pip
RUN pip install -e .
RUN pip install --no-cache-dir \
    aio-pika==9.4.1

ENTRYPOINT ["python", "-m", "distributed.serverless.deploy.dispatcher_app"]