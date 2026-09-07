# Copyright 2016-2026 the Advisor Backend team at Red Hat.
# This file is part of the Insights Advisor project.

"""
api/advisor/telemetry.py

Centralized OpenTelemetry configuration and instrumentation module for Insights Advisor.
Follows HBI and Puptoo architecture adapted for Django and Confluent Kafka.
"""

import os
import logging
from typing import Optional
from urllib.parse import urlparse

import thread_storage

logger = logging.getLogger("advisor-telemetry")

_INITIALIZED_PID: Optional[int] = None
_IS_INITIALIZED = False


def string_to_bool(value: Optional[str]) -> bool:
    if not value:
        return False
    return value.strip().lower() in ("true", "1", "yes", "t")


try:
    from opentelemetry import trace, baggage
    from opentelemetry.sdk.trace import SpanProcessor, ReadableSpan

    class RHAttributeSpanProcessor(SpanProcessor):
        """
        SpanProcessor following HBI / Puptoo pattern:
        - Sets 'rh.service' = 'advisor' (or configured service_name) on all spans.
        - Propagates 'rh.org_id' and 'rh.request_id' onto child spans,
          falling back to thread_storage context (and Django request META).
        """

        def __init__(self, service_name: str = "advisor"):
            self.service_name = service_name

        def on_start(self, span, parent_context: Optional[trace.Context] = None) -> None:
            if not span.is_recording():
                return

            # 1. Set service identifier
            span.set_attribute("rh.service", self.service_name)

            # 2. Extract org_id and request_id with precedence: Baggage -> thread_storage -> Request META
            org_id = None
            request_id = None

            if parent_context is not None:
                org_id = baggage.get_baggage("rh.org_id", parent_context)
                request_id = baggage.get_baggage("rh.request_id", parent_context)

            if not org_id:
                org_id = thread_storage.get_value("org_id")
            if not request_id:
                request_id = thread_storage.get_value("request_id")

            # Fallback to Django request object stored in thread_storage (API flow)
            if not (org_id and request_id):
                req = thread_storage.get_value("request")
                if req:
                    if not request_id and hasattr(req, "META"):
                        request_id = req.META.get("HTTP_X_RH_INSIGHTS_REQUEST_ID")
                    if not org_id:
                        org_id = getattr(req, "org_id", None)

            if org_id:
                span.set_attribute("rh.org_id", str(org_id))
            if request_id:
                span.set_attribute("rh.request_id", str(request_id))

        def on_end(self, span: ReadableSpan) -> None:
            pass

        def shutdown(self) -> None:
            pass

        def force_flush(self, timeout_millis: int = 30000) -> bool:
            return True

except ImportError:
    class RHAttributeSpanProcessor:  # type: ignore[no-redef]
        def __init__(self, service_name: str = "advisor"):
            self.service_name = service_name


class OTelContextualFilter(logging.Filter):
    """
    Logging filter that injects hex-encoded trace_id and span_id into log records.
    Safely emits None when outside an active span.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            from opentelemetry import trace
            span = trace.get_current_span()
            span_context = span.get_span_context() if span else None

            if span_context and span_context.is_valid:
                record.trace_id = format(span_context.trace_id, "032x")
                record.span_id = format(span_context.span_id, "016x")
            else:
                record.trace_id = None
                record.span_id = None
        except Exception:
            record.trace_id = None
            record.span_id = None
        return True


def _outbound_request_hook(span, request, *_args, **_kwargs):
    """Standardizes outbound HTTP span names to 'METHOD /path'."""
    if not span or not span.is_recording():
        return
    parsed = urlparse(request.url if hasattr(request, 'url') else request.path_url)
    path = parsed.path or "/"
    span.update_name(f"{request.method} {path}")


def _django_response_hook(span, request, response):
    """
    Executes after view processing to enrich the root SERVER span with
    rh.org_id, rh.request_id, and rh.account once DRF authentication has resolved.
    """
    if not span or not span.is_recording():
        return
    org_id = getattr(request, "org_id", None)
    if org_id:
        span.set_attribute("rh.org_id", str(org_id))
    request_id = request.META.get("HTTP_X_RH_INSIGHTS_REQUEST_ID") if hasattr(request, "META") else None
    if request_id:
        span.set_attribute("rh.request_id", str(request_id))
    account = getattr(request, "account", None)
    if account:
        span.set_attribute("rh.account", str(account))


def init_telemetry(
    service_name: str = "advisor",
    excluded_urls: str = "metrics,healthz,health,status",
    force_reinit: bool = False,
) -> None:
    """
    Initialize OpenTelemetry tracer provider, sampler, processors, and auto-instrumentations.
    Fork-safe: re-initializes TracerProvider and BatchSpanProcessor if running in a forked child process.
    """
    global _IS_INITIALIZED, _INITIALIZED_PID
    current_pid = os.getpid()

    if _IS_INITIALIZED and _INITIALIZED_PID == current_pid and not force_reinit:
        return

    otel_enabled = string_to_bool(os.getenv("OTEL_ENABLED", "false"))
    if not otel_enabled:
        return

    try:
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider, SpanLimits
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
        from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION
        from opentelemetry.exporter.otlp.proto.http import Compression
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    except ImportError as e:
        logger.warning("OpenTelemetry packages not installed, skipping initialization: %s", e)
        return

    if _INITIALIZED_PID is not None and _INITIALIZED_PID != current_pid:
        logger.info(
            "Process fork detected (Parent PID %s -> Child PID %s). Reinitializing OpenTelemetry...",
            _INITIALIZED_PID,
            current_pid,
        )

    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
    if not endpoint.endswith("/v1/traces"):
        endpoint = f"{endpoint.rstrip('/')}/v1/traces"

    raw_rate = os.getenv("OTEL_SAMPLING_RATE", "0.05")
    try:
        sampling_rate = min(max(float(raw_rate), 0.0), 1.0)
    except ValueError:
        sampling_rate = 0.05

    image_tag = os.getenv("IMAGE_TAG", os.getenv("OPENSHIFT_BUILD_COMMIT", "unknown"))
    env_name = os.getenv("ADVISOR_ENV", os.getenv("ENV_NAME", "dev"))
    resolved_service_name = os.getenv("OTEL_SERVICE_NAME", service_name)

    resource = Resource.create(
        {
            SERVICE_NAME: resolved_service_name,
            SERVICE_VERSION: image_tag,
            "deployment.environment": env_name,
        }
    )

    sampler = ParentBased(root=TraceIdRatioBased(sampling_rate))
    span_limits = SpanLimits(
        max_attributes=int(os.getenv("OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT", "64")),
        max_attribute_length=int(os.getenv("OTEL_SPAN_ATTRIBUTE_VALUE_LENGTH_LIMIT", "1024")),
    )

    provider = TracerProvider(resource=resource, sampler=sampler, span_limits=span_limits)
    provider.add_span_processor(RHAttributeSpanProcessor(service_name=resolved_service_name))

    compression_map = {"gzip": Compression.Gzip, "deflate": Compression.Deflate}
    compression = compression_map.get(
        os.getenv("OTEL_EXPORTER_OTLP_COMPRESSION", "gzip").lower(),
        Compression.NoCompression,
    )
    exporter = OTLPSpanExporter(
        endpoint=endpoint,
        compression=compression,
        timeout=int(os.getenv("OTEL_EXPORTER_OTLP_TIMEOUT", "10")),
    )
    bsp = BatchSpanProcessor(
        exporter,
        max_queue_size=int(os.getenv("OTEL_BSP_MAX_QUEUE_SIZE", "8192")),
        schedule_delay_millis=int(os.getenv("OTEL_BSP_SCHEDULE_DELAY", "2000")),
        max_export_batch_size=int(os.getenv("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", "256")),
        export_timeout_millis=int(os.getenv("OTEL_BSP_EXPORT_TIMEOUT", "10000")),
    )
    provider.add_span_processor(bsp)
    if hasattr(trace, "_TRACER_PROVIDER_SET_ONCE"):
        trace._TRACER_PROVIDER_SET_ONCE._done = False
    trace.set_tracer_provider(provider)

    try:
        from opentelemetry.instrumentation.django import DjangoInstrumentor
        DjangoInstrumentor().instrument(excluded_urls=excluded_urls, response_hook=_django_response_hook)
    except Exception as e:
        logger.warning("Django instrumentation failed or skipped: %s", e)

    try:
        from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
        Psycopg2Instrumentor().instrument(enable_commenter=False)
    except Exception as e:
        logger.warning("Psycopg2 instrumentation failed or skipped: %s", e)

    try:
        from opentelemetry.instrumentation.requests import RequestsInstrumentor
        RequestsInstrumentor().instrument(request_hook=_outbound_request_hook)
    except Exception as e:
        logger.warning("Requests instrumentation failed or skipped: %s", e)

    try:
        from opentelemetry.instrumentation.confluent_kafka import ConfluentKafkaInstrumentor
        ConfluentKafkaInstrumentor().instrument()
    except Exception as e:
        logger.warning("Confluent Kafka instrumentation failed or skipped: %s", e)

    _IS_INITIALIZED = True
    _INITIALIZED_PID = current_pid
    logger.info("OpenTelemetry initialized for %s (PID %s, sampler=%s)", resolved_service_name, current_pid, sampler)


def get_tracer(name: str = "advisor"):
    """Return tracer only if OpenTelemetry is initialized and enabled."""
    if not _IS_INITIALIZED:
        return None
    try:
        from opentelemetry import trace
        return trace.get_tracer(name)
    except ImportError:
        return None


def extract_kafka_headers_to_context(headers: Optional[list[tuple[str, bytes]]]):
    """Extract W3C trace context from Kafka message headers."""
    try:
        from opentelemetry import propagate, trace
        if not headers:
            return trace.Context()
        carrier = {}
        for key, val in headers:
            if isinstance(val, bytes):
                carrier[key] = val.decode("utf-8", errors="replace")
            elif isinstance(val, str):
                carrier[key] = val
        return propagate.extract(carrier)
    except ImportError:
        return None


def shutdown_telemetry(timeout_millis: int = 5000) -> None:
    """
    Flush all buffered spans and cleanly shut down TracerProvider.
    Ensures queued spans in BatchSpanProcessor are exported prior to process exit.
    """
    global _IS_INITIALIZED, _INITIALIZED_PID
    if not _IS_INITIALIZED:
        return
    try:
        from opentelemetry import trace
        provider = trace.get_tracer_provider()
        if hasattr(provider, "force_flush"):
            provider.force_flush(timeout_millis=timeout_millis)
        if hasattr(provider, "shutdown"):
            provider.shutdown()
        _IS_INITIALIZED = False
        _INITIALIZED_PID = None
        logger.info("OpenTelemetry telemetry flushed and shut down successfully.")
    except Exception as e:
        logger.warning("Error flushing OpenTelemetry spans on shutdown: %s", e)
