FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=5000

WORKDIR /app

COPY req.txt ./
RUN pip install --no-cache-dir -r req.txt \
    && addgroup --system app \
    && adduser --system --ingroup app app

COPY --chown=app:app app.py ./
COPY --chown=app:app templates ./templates
COPY --chown=app:app static ./static

USER app
EXPOSE 5000

CMD ["python", "app.py"]
