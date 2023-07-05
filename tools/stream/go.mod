module streamer

go 1.13

require (
	github.com/codenotary/immudb v0.9.1
	github.com/golang/protobuf v1.5.2
	github.com/mitchellh/go-homedir v1.1.0
	github.com/spf13/cobra v1.2.1
	github.com/spf13/viper v1.8.1
	google.golang.org/grpc v1.53.0
)

replace github.com/codenotary/immudb v0.9.1 => ../../
