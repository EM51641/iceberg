###########################
# Stage 1: Builder - Install dependencies with uv
###########################
FROM python:3.9-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y gcc 

# Copy dependency files
COPY uv.lock pyproject.toml .python-version ./

# Install uv with increased timeout and retries
RUN pip install uv && \
    uv sync

RUN ls -la /build
###########################
# Stage 2: Final Runtime
###########################
FROM tabulario/spark-iceberg

WORKDIR /home/iceberg

# Copy installed packages from builder
COPY --from=builder /build/.venv/lib/python3.9/site-packages/ /usr/local/lib/python3.9/site-packages

# Copy the source code
COPY ./src/main.py .

# Copy and setup entrypoint: copy it to the working directory
COPY entrypoint.sh /home/iceberg/entrypoint.sh
RUN chmod +x /home/iceberg/entrypoint.sh

CMD ["python", "/home/iceberg/main.py"]