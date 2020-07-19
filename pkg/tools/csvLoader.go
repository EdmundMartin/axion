package tools

import (
	"axion/pkg/btree"
	"axion/pkg/table"
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
)

func LoadFromCSV(filepath string, tblname string, mapping map[int]table.ColumnInfo) error {

	emptyTable := table.NewTable(tblname)

	primaryTable := btree.NewTree("Primary", 100, 10, btree.IntegerType)
	treeMap := make(map[int]*btree.Blink)
	for key, value := range mapping {
		treeMap[key] = btree.NewTree(value.ColumnName, 100, 10, value.ColumnType)
	}

	csvFile, err := os.Open(filepath)
	if err != nil {
		return err
	}
	reader := csv.NewReader(bufio.NewReader(csvFile))
	pk := 0
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		primaryTable.Insert(btree.Key{Value: line, KeyValue: pk, Type: btree.IntegerType})
		for key, info := range mapping {
			if key < len(line) {
				targetTree := treeMap[key]
				targetTree.Insert(btree.Key{Value: pk, KeyValue: line[key], Type: info.ColumnType})
			}
		}
		pk++
	}
	primaryTable.Serialize(fmt.Sprintf("%s_%s.db", tblname, primaryTable.Name))

	for _, tree := range treeMap {
		fileName := fmt.Sprintf("%s_%s.db_col", tblname, tree.Name)
		emptyTable.ColumnTrees[tree.Name] = table.ColumnInfo{ColumnName: tree.Name, ColumnType: tree.TreeType}
		tree.Serialize(fileName)
	}
	emptyTable.Serialize()

	return nil
}
