package dbmongo

type Channels map[string]Config

type Config struct {
	DSN  string `mapstructure:"dsn" json:"dsn,omitempty" yaml:"dsn,omitempty"`
	Ping bool   `mapstructure:"ping" json:"ping,omitempty" yaml:"ping,omitempty"`
}
