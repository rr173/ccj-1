FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 控制面与数据面是两个独立镜像/进程，只共享同一份协议代码（common/）。
COPY common /app/common
COPY control_plane /app/control_plane

EXPOSE 8000
CMD ["uvicorn", "control_plane.app:app", "--host", "0.0.0.0", "--port", "8000"]
