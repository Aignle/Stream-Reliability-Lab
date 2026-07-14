FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY .streamlit ./.streamlit
COPY src ./src
RUN python -m pip install --no-cache-dir . \
    && useradd --create-home --uid 10001 streamlab \
    && mkdir -p /data \
    && chown -R streamlab:streamlab /app /data

USER streamlab

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "streamlab.main:app", "--host", "0.0.0.0", "--port", "8000"]
