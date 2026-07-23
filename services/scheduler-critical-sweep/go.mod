module mpp/scheduler-critical-sweep

go 1.26.2

require mpp/platformcontracts v0.0.0

require (
	github.com/alicebob/miniredis/v2 v2.38.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/redis/go-redis/v9 v9.21.0 // indirect
	github.com/twmb/franz-go v1.21.5 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	go.uber.org/atomic v1.11.0 // indirect
)

replace mpp/platformcontracts => ./internal/proto/gen/mpp/platformcontracts
