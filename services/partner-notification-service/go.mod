module mpp/partner-notification-service

go 1.26.2

require (
	github.com/KimMachineGun/automemlimit v0.7.5
	github.com/alicebob/miniredis/v2 v2.38.0
	github.com/prometheus/client_golang v1.24.1
	github.com/redis/go-redis/v9 v9.21.0
	github.com/twmb/franz-go v1.21.5
	github.com/twmb/franz-go/pkg/kadm v1.18.0
	google.golang.org/grpc v1.68.1
	google.golang.org/protobuf v1.36.11
	mpp/platformcontracts v0.0.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/klauspost/compress v1.19.1 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pbnjay/memory v0.0.0-20210728143218-7b4eea64cf58 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.70.1 // indirect
	github.com/prometheus/procfs v0.21.1 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/crypto v0.54.0 // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240903143218-8af14fe29dc1 // indirect
)

replace mpp/platformcontracts => ./internal/proto/gen/mpp/platformcontracts
