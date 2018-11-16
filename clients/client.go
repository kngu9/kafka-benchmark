// Package clients contains all the different types of libraries to test
package clients

// Client will be used as a generic interface for normalizing testing.
// Furthermore, each Produce should be synchronous.
type Client interface {
	Produce(buff []byte) error
	Name() string
	Close() error
}
