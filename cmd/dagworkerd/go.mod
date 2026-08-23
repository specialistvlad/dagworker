module github.com/specialistvlad/dagworker/cmd/dagworkerd

go 1.25.0

require (
	github.com/redis/go-redis/v9 v9.22.0
	github.com/specialistvlad/dagworker v0.0.1
	github.com/specialistvlad/dagworker/adapters/grpc v0.0.0-20260823180902-32ecab58c1c0
	github.com/specialistvlad/dagworker/adapters/http v0.0.0-20260823180902-32ecab58c1c0
	github.com/specialistvlad/dagworker/storage/postgres v0.0.0-20260823180902-32ecab58c1c0
	github.com/specialistvlad/dagworker/storage/redis v0.0.0-20260823180902-32ecab58c1c0
	google.golang.org/grpc v1.83.1
	google.golang.org/protobuf v1.36.12
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.10.0 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/rogpeppe/go-internal v1.16.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260819154853-08b0e4226688 // indirect
)
