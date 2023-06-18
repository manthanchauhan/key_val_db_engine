package index

type Index interface {
	GetOrPanic(key string) string
	Set(key string, val string) error
	Init()
	ImportDataSegment(fileName string, initValCheck func(k string) bool)
	GetDataDirectory() string
}
