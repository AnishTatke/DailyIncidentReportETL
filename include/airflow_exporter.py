from airflow_prometheus_exporter import PrometheusMetrics

def configure_exporter():
    PrometheusMetrics().start_http_server(9112)

if __name__ == "__main__":
    configure_exporter()