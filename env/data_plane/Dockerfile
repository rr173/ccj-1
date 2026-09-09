FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY common /app/common
COPY data_plane /app/data_plane

EXPOSE 8080
CMD ["python", "-m", "data_plane.app"]
