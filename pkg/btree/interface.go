package btree

type KeyType = int

const (
	StringType KeyType = iota
)

type Keys []Key

type Key struct {
	KeyValue interface{}
	Value string
	Type KeyType
	isNull bool
}

func (k Key) Compare(other Key) int {
	switch k.Type {
	case StringType:
		val := k.KeyValue.(string)
		oth := other.KeyValue.(string)
		if val > oth {
			return 1
		} else if val == oth {
			return 0
		}
		return -1
	}
	return 0
}