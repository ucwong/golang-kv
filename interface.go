package bucket

type Bucket interface {
	Get(k []byte) []byte
	Set(k, v []byte) error
	Del(k []byte) error
	Close() error
}
