package bucket

type Bucket interface {
	New(i int) error
	Get(k []byte) ([]byte, error)
	Set(k, v []byte) error
}
