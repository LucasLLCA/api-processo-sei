"""
OpenTelemetry initialization and custom metrics for the API.
Call configure_telemetry() once at startup, before FastAPI app creation.
"""
import logging
import os

from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry import _logs as otel_logs

logger = logging.getLogger(__name__)

_configured = False


def configure_telemetry():
    """Initialize OTEL providers, exporters, and auto-instrumentations."""
    global _configured
    if _configured:
        return
    _configured = True

    endpoint = os.getenv(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "http://otelcollectorhttp.10.0.122.91.sslip.io",
    )

    resource = Resource.create({
        SERVICE_NAME: os.getenv("OTEL_SERVICE_NAME", "api-processo-sei"),
        SERVICE_VERSION: os.getenv("API_VERSION", "2.0.0"),
        "deployment.environment": os.getenv("DEPLOYMENT_ENV", "production"),
    })

    # --- Traces ---
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{endpoint}/v1/traces"))
    )
    trace.set_tracer_provider(tracer_provider)

    # --- Metrics ---
    metric_reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=f"{endpoint}/v1/metrics"),
        export_interval_millis=60_000,
    )
    meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
    metrics.set_meter_provider(meter_provider)

    # --- Logs ---
    log_provider = LoggerProvider(resource=resource)
    log_provider.add_log_record_processor(
        BatchLogRecordProcessor(OTLPLogExporter(endpoint=f"{endpoint}/v1/logs"))
    )
    otel_logs.set_logger_provider(log_provider)

    # Forward all Python logger.* calls to OTEL collector with trace correlation
    otel_handler = LoggingHandler(level=logging.INFO, logger_provider=log_provider)
    logging.getLogger().addHandler(otel_handler)

    # --- Auto-Instrumentations ---
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
    from opentelemetry.instrumentation.redis import RedisInstrumentor
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
    from .database import engine

    HTTPXClientInstrumentor().instrument()
    RedisInstrumentor().instrument()
    SQLAlchemyInstrumentor().instrument(engine=engine.sync_engine)

    logger.info("OpenTelemetry configured: traces, metrics, logs -> %s", endpoint)


# ── Custom Metrics ──────────────────────────────────────────────────────

meter = metrics.get_meter("api.processo_sei", "1.0.0")

# Cache
cache_hit_counter = meter.create_counter("cache.hits", description="Cache hits", unit="1")
cache_miss_counter = meter.create_counter("cache.misses", description="Cache misses", unit="1")
cache_set_failure_counter = meter.create_counter("cache.set.failures", description="Cache set failures", unit="1")

# LLM
llm_request_duration = meter.create_histogram("llm.request.duration", description="LLM call duration", unit="s")
llm_token_usage = meter.create_counter("llm.token.usage", description="LLM tokens consumed", unit="tokens")
llm_timeout_counter = meter.create_counter("llm.timeouts", description="LLM timeout errors", unit="1")

# SEI API
sei_retry_counter = meter.create_counter("sei.api.retries", description="SEI API retry attempts", unit="1")
sei_request_duration = meter.create_histogram("sei.api.request.duration", description="SEI API call duration", unit="s")

# SSE Streaming
sse_active_connections = meter.create_up_down_counter("sse.active_connections", description="Active SSE connections", unit="1")
sse_stream_duration = meter.create_histogram("sse.stream.duration", description="SSE stream total duration", unit="s")

# PDF Processing
pdf_processing_duration = meter.create_histogram("pdf.processing.duration", description="PDF conversion duration", unit="s")
