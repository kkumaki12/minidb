package table

import (
	"github.com/kkumaki12/minidb/btree"
	"github.com/kkumaki12/minidb/buffer"
	"github.com/kkumaki12/minidb/disk"
)

// SimpleTable はB-treeをベースにしたシンプルなテーブル
// Tupleの最初のnumKeyElems個の要素をキーとして使用する
type SimpleTable struct {
	MetaPageID  disk.PageID // B-treeのメタページID
	NumKeyElems int         // キーを構成する要素数
}

// Create は新しいSimpleTableを作成する
func Create(bufmgr *buffer.BufferPoolManager, numKeyElems int) (*SimpleTable, error) {
	tree, err := btree.Create(bufmgr)
	if err != nil {
		return nil, err
	}

	return &SimpleTable{
		MetaPageID:  tree.MetaPageID,
		NumKeyElems: numKeyElems,
	}, nil
}

// NewSimpleTable は既存のSimpleTableを開く
func NewSimpleTable(metaPageID disk.PageID, numKeyElems int) *SimpleTable {
	return &SimpleTable{
		MetaPageID:  metaPageID,
		NumKeyElems: numKeyElems,
	}
}

// btree は内部のB-treeを取得する
func (t *SimpleTable) btree() *btree.BTree {
	return btree.NewBTree(t.MetaPageID)
}

// Insert はTupleをテーブルに挿入する
func (t *SimpleTable) Insert(bufmgr *buffer.BufferPoolManager, tuple Tuple) error {
	key, value := SplitTuple(tuple, t.NumKeyElems)
	keyBytes := key.Encode()
	valueBytes := value.Encode()

	return t.btree().Insert(bufmgr, keyBytes, valueBytes)
}

// Scan はテーブルの全行をスキャンするイテレータを返す
func (t *SimpleTable) Scan(bufmgr *buffer.BufferPoolManager) (*TableIter, error) {
	iter, err := t.btree().Search(bufmgr, btree.NewSearchStart())
	if err != nil {
		return nil, err
	}

	return &TableIter{
		btreeIter:   iter,
		numKeyElems: t.NumKeyElems,
	}, nil
}

// ScanFrom は指定したキーからスキャンするイテレータを返す
func (t *SimpleTable) ScanFrom(bufmgr *buffer.BufferPoolManager, searchKey Tuple) (*TableIter, error) {
	keyBytes := searchKey.Encode()
	iter, err := t.btree().Search(bufmgr, btree.NewSearchKey(keyBytes))
	if err != nil {
		return nil, err
	}

	return &TableIter{
		btreeIter:   iter,
		numKeyElems: t.NumKeyElems,
	}, nil
}

// TableIter はテーブルのイテレータ
type TableIter struct {
	btreeIter   *btree.Iter
	numKeyElems int
}

// Next は次のTupleを返す
func (it *TableIter) Next(bufmgr *buffer.BufferPoolManager) (Tuple, error) {
	pair, err := it.btreeIter.Next(bufmgr)
	if err != nil {
		return nil, err
	}
	if pair == nil {
		return nil, nil
	}

	key := DecodeTuple(pair.Key)
	value := DecodeTuple(pair.Value)

	return MergeTuple(key, value), nil
}
