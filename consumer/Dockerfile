FROM python:3.14-slim

WORKDIR /app

# Install only what we need for the lightweight consumer.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py ./

CMD ["python", "app.py"]
