FROM python:3.11-slim
WORKDIR /app
COPY worker.py server.py ./
ENV PORT=10000
EXPOSE 10000
CMD ["python", "server.py"]
