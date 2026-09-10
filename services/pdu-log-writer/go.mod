module mpp/pdu-log-writer

go 1.26.2

require (
	github.com/ClickHouse/clickhouse-go/v2 v2.47.0
	github.com/KimMachineGun/automemlimit v0.7.5
	github.com/twmb/franz-go v1.21.5
	google.golang.org/protobuf v1.36.11
	mpp/platformcontracts v0.0.0
)

require (
	github.com/ClickHouse/ch-go v0.73.0 // indirect
	github.com/andybalholm/brotli v1.2.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-faster/city v1.0.1 // indirect
	github.com/go-faster/errors v0.7.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/paulmach/orb v0.13.0 // indirect
	github.com/pbnjay/memory v0.0.0-20210728143218-7b4eea64cf58 // indirect
	github.com/pierrec/lz4/v4 v4.1.27 // indirect
	github.com/segmentio/asm v1.2.1 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
	go.opentelemetry.io/otel v1.44.0 // indirect
	go.opentelemetry.io/otel/trace v1.44.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/sys v0.46.0 // indirect
)

replace mpp/platformcontracts => ./internal/proto/gen/mpp/platformcontracts
