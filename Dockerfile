# Single image shared by the API and the workers. Which role a container plays
# is decided by its command in docker-compose.yml, so we build once and run the
# same artifact everywhere.
FROM python:3.12-slim

# Faster, quieter, no .pyc clutter, unbuffered logs so docker logs are live.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install deps first so this layer is cached unless requirements change.
COPY requirements.txt .
RUN pip install -r requirements.txt

# Then the application code.
COPY jobqueue ./jobqueue
COPY loadtest ./loadtest

# Default command runs a worker. The api service overrides this with gunicorn.
CMD ["python", "-m", "jobqueue", "worker"]
