package index

type Index interface {
	GetOrPanic(key string) string
	Get(key string) (string, bool)
	Set(key string, val string) error
	Init()
	ImportDataSegment(fileName string, initValCheck func(k string) bool)
	GetDataDirectory() string
}
