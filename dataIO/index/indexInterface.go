package index

type Index interface {
	Get(key string) (string, error)
	Set(key string, val string) error
	Delete(key string) error
	Init()
	ImportData()
	GetDataDirectory() string
}
