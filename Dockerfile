FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

COPY pyproject.toml README.md ./
COPY src/ ./src/
COPY schemas/ ./schemas/
COPY data/ ./data/

RUN pip install --no-cache-dir -e ".[neon]"

# Default entrypoint runs non-destructive quality checks across F1 and F4
CMD ["python3", "-m", "buenavista_csv.validator"]
