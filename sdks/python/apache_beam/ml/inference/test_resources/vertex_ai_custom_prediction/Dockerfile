FROM python:3.10-slim
WORKDIR /app
RUN pip install flask gunicorn
COPY echo_server.py main.py
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "main:app"]
