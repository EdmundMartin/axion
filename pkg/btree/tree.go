package btree

import (
	"encoding/gob"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)


const numberOfItemsBeforeMultithread = 10

type Blink struct {
	Root                     *node
	lock                     sync.RWMutex
	Number, Ary, NumRoutines uint64
	TreeType KeyType
}

func (blink *Blink) insert(key Key, stack *nodes) Key {
	var parent *node
	blink.lock.Lock()
	if blink.Root == nil {
		blink.Root = newNode(
			true, make(Keys, 0, blink.Ary), make(nodes, 0, blink.Ary+1),
		)
		blink.Root.Keys = make(Keys, 0, blink.Ary)
		blink.Root.IsLeaf = true
	}
	parent = blink.Root
	blink.lock.Unlock()

	result := insert(blink, parent, stack, key)
	if result.isNull == true {
		atomic.AddUint64(&blink.Number, 1)
		return Key{isNull: true}
	}

	return result
}

func (blink *Blink) multithreadedInsert(keys Keys) Keys {
	chunks := chunkKeys(keys, int64(blink.NumRoutines))
	overwritten := make(Keys, len(keys))
	var offset uint64
	var wg sync.WaitGroup
	wg.Add(len(chunks))

	for _, chunk := range chunks {
		go func(chunk Keys, offset uint64) {
			defer wg.Done()
			stack := make(nodes, 0, blink.Ary)

			for i := 0; i < len(chunk); i++ {
				result := blink.insert(chunk[i], &stack)
				stack.reset()
				overwritten[offset+uint64(i)] = result
			}
		}(chunk, offset)
		offset += uint64(len(chunk))
	}

	wg.Wait()

	return overwritten
}

// Insert will insert the provided Keys into the b-tree and return
// a list of Keys overwritten, if any.  Each insert is an O(log n)
// operation.
func (blink *Blink) Insert(keys ...Key) Keys {
	if len(keys) > numberOfItemsBeforeMultithread {
		return blink.multithreadedInsert(keys)
	}
	overwritten := make(Keys, 0, len(keys))
	stack := make(nodes, 0, blink.Ary)
	for _, k := range keys {
		overwritten = append(overwritten, blink.insert(k, &stack))
		stack.reset()
	}

	return overwritten
}

// Len returns the Number of items in this b-link tree.
func (blink *Blink) Len() uint64 {
	return atomic.LoadUint64(&blink.Number)
}

func (blink *Blink) get(key Key) Key {
	var parent *node
	blink.lock.RLock()
	parent = blink.Root
	blink.lock.RUnlock()
	k := search(parent, key)
	if k.isNull == true {
		return Key{isNull: true}
	}

	if k.Compare(key) == 0 {
		return k
	}

	return Key{isNull: true}
}

// Get will retrieve the Keys if they exist in this tree.  If not,
// a nil is returned in the proper place in the list of Keys.  Each
// lookup is O(log n) time complexity.
func (blink *Blink) Get(keys ...Key) Keys {
	found := make(Keys, 0, len(keys))
	for _, k := range keys {
		result := blink.get(k)
		if !result.isNull {
			found = append(found, blink.get(k))
		}
	}
	return found
}

func (blink *Blink) Print() {
	fmt.Println(`PRINTING B-LINK`)
	if blink.Root == nil {
		return
	}

	blink.Root.print()
}

func NewTree(ary, numRoutines uint64, treeType KeyType) *Blink {
	return &Blink{Ary: ary, NumRoutines: numRoutines, TreeType: treeType}
}


func (blink *Blink) Serialize(treeName string) error {
	dataFile, err := os.OpenFile(fmt.Sprintf("%s.gob", treeName), os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
	defer dataFile.Close()
	if err != nil {
		return err
	}
	dataEncoder := gob.NewEncoder(dataFile)
	err = dataEncoder.Encode(blink)

	if err != nil {
		return err
	}
	return nil
}

func BlinkFromFile(filename string) (*Blink, error) {
	dataFile, err := os.Open(filename)
	defer dataFile.Close()
	if err != nil {
		return nil, err
	}
	var bl *Blink
	dataDecoder := gob.NewDecoder(dataFile)
	err = dataDecoder.Decode(&bl)
	if err != nil {
		return nil, err
	}
	return bl, nil
}