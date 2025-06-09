FROM public.ecr.aws/docker/library/python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

ENV PYTHONPATH="/app"

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["sh", "-c", "hyrex init-db && hyrex run-worker hyrex_app:app"]