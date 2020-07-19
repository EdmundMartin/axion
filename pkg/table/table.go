package table

import (
	"axion/pkg/btree"
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
)

type Table struct {
	Name        string
	PrimaryTree string
	ColumnTrees map[string]ColumnInfo
}

func NewTable(name string) *Table {
	return &Table{
		Name:        name,
		ColumnTrees: make(map[string]ColumnInfo),
	}
}

func LoadTable(filename string) (*Table, error) {
	dataFile, err := os.Open(filename)
	defer dataFile.Close()
	if err != nil {
		return nil, err
	}
	var tb *Table
	dataDecoder := gob.NewDecoder(dataFile)
	err = dataDecoder.Decode(&tb)
	if err != nil {
		return nil, err
	}
	return tb, nil
}

func (t *Table) LoadColumn(columnName string) (*btree.Blink, error) {
	_, ok := t.ColumnTrees[columnName]
	if !ok {
		return nil, fmt.Errorf("no column named: %s", columnName)
	}
	filename := fmt.Sprintf("%s_%s.db_col.gob", t.Name, columnName)
	return btree.BlinkFromFile(filename)
}

func (t *Table) Serialize() error {
	buf := &bytes.Buffer{}
	fo, err := os.Create(fmt.Sprintf("%s.db", t.Name))
	defer fo.Close()
	if err != nil {
		return err
	}

	err = gob.NewEncoder(buf).Encode(t)
	if err != nil {
		return err
	}

	_, err = fo.Write(buf.Bytes())
	if err != nil {
		return err
	}
	return nil
}

type ColumnInfo struct {
	ColumnName string
	ColumnType btree.KeyType
}
