FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY config/ ./config/

RUN addgroup --system dataforge \
    && adduser --system --ingroup dataforge dataforge \
    && mkdir -p data/raw data/bronze data/silver data/gold \
        data/quarantine data/state reports logs \
    && chown -R dataforge:dataforge /app/data /app/reports /app/logs

USER dataforge

CMD ["python", "src/dataforge/cli.py"]