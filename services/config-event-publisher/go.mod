module mpp/config-event-publisher

go 1.26.2

require (
	github.com/jackc/pgx/v5 v5.10.0
	github.com/twmb/franz-go v1.21.5
	google.golang.org/protobuf v1.36.11
	mpp/platformcontracts v0.0.0
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
	golang.org/x/sync v0.17.0 // indirect
	golang.org/x/text v0.29.0 // indirect
)

replace mpp/platformcontracts => ./internal/proto/gen/mpp/platformcontracts
