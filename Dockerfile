FROM python:3.11-slim
WORKDIR /app
COPY app/requirements.txt .
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && pip install --upgrade pip \
    && pip install -r requirements.txt
    
CMD ["tail", "-f", "/dev/null"]
