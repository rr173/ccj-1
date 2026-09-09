FROM python:3.11-slim

WORKDIR /app
COPY mock_upstream /app/mock_upstream

EXPOSE 8000
CMD ["python", "-m", "mock_upstream.app"]
