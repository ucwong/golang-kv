package bucket

type Bucket interface {
	Get(k []byte) []byte
	Set(k, v []byte) error
	Del(k []byte) error
	Prefix(k []byte) [][]byte
	Suffix(k []byte) [][]byte
	Scan() [][]byte
	Close() error
}
