module processing-service

go 1.22

require (
  github.com/prometheus/client_golang v1.20.5
  github.com/rabbitmq/amqp091-go v1.10.0
  github.com/sony/gobreaker v1.0.0
  gorm.io/driver/sqlite v1.5.6
  gorm.io/gorm v1.25.11
  github.com/glebarez/sqlite latest
  go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.58.0
  go.opentelemetry.io/otel v1.33.0
  go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.33.0
  go.opentelemetry.io/otel/sdk v1.33.0
)
