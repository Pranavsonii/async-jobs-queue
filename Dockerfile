# one image for everything - api, workers, beat and flower.
# only the command in docker-compose.yml changes. keeps the task code
# the same on producer and consumer side.

FROM python:3.12-slim

# no .pyc files, and unbuffered output so logs show up right away
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# requirements first so pip install is cached when only code changes
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/
COPY static/ ./static/

# don't run as root
RUN useradd --create-home --uid 1000 celery && chown -R celery:celery /app
USER celery

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
