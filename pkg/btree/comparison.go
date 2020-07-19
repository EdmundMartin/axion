package btree

type KeyType = int

const (
	StringType KeyType = iota
	IntegerType
)

type Keys []Key

type Key struct {
	KeyValue interface{}
	Value    interface{}
	Type     KeyType
	isNull   bool
}

func compareString(first, second Key) int {
	val := first.KeyValue.(string)
	oth := second.KeyValue.(string)
	if val > oth {
		return 1
	} else if val == oth {
		return 0
	}
	return -1
}

func compareInt(first, second Key) int {
	val := first.KeyValue.(int)
	oth := second.KeyValue.(int)
	if val > oth {
		return 1
	} else if val == oth {
		return 0
	}
	return -1
}

func (k Key) Compare(other Key) int {
	switch k.Type {
	case StringType:
		return compareString(k, other)
	case IntegerType:
		return compareInt(k, other)
	}
	return 0
}
