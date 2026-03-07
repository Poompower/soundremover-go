module gateway

go 1.22

require (
  github.com/golang-jwt/jwt/v5 v5.2.1
  github.com/prometheus/client_golang v1.20.5
  github.com/rabbitmq/amqp091-go v1.10.0
  github.com/sony/gobreaker v1.0.0
  github.com/glebarez/sqlite latest
  go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.58.0
  go.opentelemetry.io/otel v1.33.0
  go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.33.0
  go.opentelemetry.io/otel/sdk v1.33.0
)
