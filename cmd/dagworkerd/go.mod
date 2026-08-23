module github.com/specialistvlad/dagworker/cmd/dagworkerd

go 1.25.0

require (
	github.com/redis/go-redis/v9 v9.22.0
	github.com/specialistvlad/dagworker v0.0.1
	github.com/specialistvlad/dagworker/adapters/grpc v0.0.0-20260823103802-f1e0334d8ac4
	github.com/specialistvlad/dagworker/adapters/http v0.0.0-20260823103802-f1e0334d8ac4
	github.com/specialistvlad/dagworker/storage/postgres v0.0.0-20260823103802-f1e0334d8ac4
	github.com/specialistvlad/dagworker/storage/redis v0.0.0-20260823103802-f1e0334d8ac4
	google.golang.org/grpc v1.83.1
	google.golang.org/protobuf v1.36.12
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.7.6 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/rogpeppe/go-internal v1.16.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/crypto v0.51.0 // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/text v0.37.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
)
