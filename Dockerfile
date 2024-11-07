FROM python:3.10-slim
WORKDIR /app
COPY loadbalancer.py .
RUN pip install pika
ENTRYPOINT ["python3", "loadbalancer.py"]