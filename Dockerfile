FROM quay.io/astronomer/astro-runtime:12.7.0

# Install airflow-exporter dependencies
USER root
RUN pip install airflow-exporter prometheus-client

# Expose port for exporter
EXPOSE 9112

USER astro