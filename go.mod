module github.com/hoangnguyenba/syncdb

go 1.21

toolchain go1.24.0

require (
	github.com/go-sql-driver/mysql v1.9.0
	github.com/lib/pq v1.10.9
	github.com/spf13/cobra v1.9.1
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/spf13/pflag v1.0.6 // indirect
)

// Replaced by module syncdb/config with:
replace github.com/hoangnguyenba/syncdb/internal/config => ../pkg/config
