FROM airbyte/python-connector-base:1.1.0

COPY . ./airbyte/integration_code
RUN pip install ./airbyte/integration_code

# Install LibreOffice
# Note: Running 'apt-get update' and 'apt-get install' in a single RUN to avoid caching issues
RUN apt-get update && \
    apt-get install -y libreoffice && \
    # Clean up the cache by removing /var/lib/apt/lists, it reduces the image size
    rm -rf /var/lib/apt/lists/*


# The entrypoint and default env vars are already set in the base image
ENV AIRBYTE_ENTRYPOINT "python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]