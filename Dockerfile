FROM python:3.14
WORKDIR /usr/local/app

# Install the application dependencies
COPY pyproject.toml ./
RUN pip install --no-cache-dir --only-deps .

# Copy in the source code
COPY src ./src
RUN pip install --no-deps -e .
EXPOSE 80

# Setup an app user so the container doesn't run as the root user
RUN useradd app
USER app

COPY config config

HEALTHCHECK --interval=30s --retries=3 CMD curl --fail http://localhost:80 || exit 1
CMD ["radicale", "--config", "config/radicale_config.ini", "--server-hosts", "0.0.0.0:80", "--storage-type", "calendar_proxy.radicale_storage_backend"]
#CMD ["bash"]