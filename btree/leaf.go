package btree

import (
	"bytes"

	"github.com/kkumaki12/minidb/disk"
)

// Leafヘッダーのレイアウト:
// [prev_page_id: 8] [next_page_id: 8] [num_pairs: 2] [free_space_offset: 2]
// その後にスロット配列（各2バイト）が続き、ページ末尾からデータが詰められる

const (
	LeafPrevPageIDOffset      = 0
	LeafNextPageIDOffset      = 8
	LeafNumPairsOffset        = 16
	LeafFreeSpaceOffsetOffset = 18
	LeafHeaderSize            = 20
	LeafSlotSize              = 2 // 各スロットはオフセット値（2バイト）
)

// InvalidPageID は無効なページIDを示す
const InvalidPageID = disk.PageID(0xFFFFFFFFFFFFFFFF)

// Leaf はリーフノードを表す
type Leaf struct {
	data []byte
}

// NewLeaf はデータからLeafを作成する
func NewLeaf(data []byte) *Leaf {
	return &Leaf{data: data}
}

// Initialize はリーフノードを初期化する
func (l *Leaf) Initialize() {
	l.setPrevPageID(InvalidPageID)
	l.setNextPageID(InvalidPageID)
	l.setNumPairs(0)
	l.setFreeSpaceOffset(uint16(len(l.data)))
}

// PrevPageID は前のリーフページIDを返す
func (l *Leaf) PrevPageID() *disk.PageID {
	id := disk.PageID(readUint64(l.data[LeafPrevPageIDOffset:]))
	if id == InvalidPageID {
		return nil
	}
	return &id
}

// NextPageID は次のリーフページIDを返す
func (l *Leaf) NextPageID() *disk.PageID {
	id := disk.PageID(readUint64(l.data[LeafNextPageIDOffset:]))
	if id == InvalidPageID {
		return nil
	}
	return &id
}

func (l *Leaf) setPrevPageID(id disk.PageID) {
	writeUint64(l.data[LeafPrevPageIDOffset:], uint64(id))
}

func (l *Leaf) setNextPageID(id disk.PageID) {
	writeUint64(l.data[LeafNextPageIDOffset:], uint64(id))
}

// SetPrevPageID は前のリーフページIDを設定する
func (l *Leaf) SetPrevPageID(id *disk.PageID) {
	if id == nil {
		l.setPrevPageID(InvalidPageID)
	} else {
		l.setPrevPageID(*id)
	}
}

// SetNextPageID は次のリーフページIDを設定する
func (l *Leaf) SetNextPageID(id *disk.PageID) {
	if id == nil {
		l.setNextPageID(InvalidPageID)
	} else {
		l.setNextPageID(*id)
	}
}

// NumPairs はペアの数を返す
func (l *Leaf) NumPairs() int {
	return int(readUint16(l.data[LeafNumPairsOffset:]))
}

func (l *Leaf) setNumPairs(n uint16) {
	writeUint16(l.data[LeafNumPairsOffset:], n)
}

func (l *Leaf) freeSpaceOffset() uint16 {
	return readUint16(l.data[LeafFreeSpaceOffsetOffset:])
}

func (l *Leaf) setFreeSpaceOffset(offset uint16) {
	writeUint16(l.data[LeafFreeSpaceOffsetOffset:], offset)
}

// slotOffset はスロット配列の開始位置を返す
func (l *Leaf) slotOffset(slotID int) int {
	return LeafHeaderSize + slotID*LeafSlotSize
}

// getSlot は指定スロットのデータオフセットを返す
func (l *Leaf) getSlot(slotID int) uint16 {
	return readUint16(l.data[l.slotOffset(slotID):])
}

// setSlot は指定スロットにデータオフセットを設定する
func (l *Leaf) setSlot(slotID int, offset uint16) {
	writeUint16(l.data[l.slotOffset(slotID):], offset)
}

// freeSpace は空き領域のサイズを返す
func (l *Leaf) freeSpace() int {
	slotsEnd := l.slotOffset(l.NumPairs())
	return int(l.freeSpaceOffset()) - slotsEnd
}

// PairAt は指定スロットのペアを返す
func (l *Leaf) PairAt(slotID int) *Pair {
	offset := l.getSlot(slotID)
	return PairFromBytes(l.data[offset:])
}

// SearchSlotID はキーを検索してスロットIDを返す
// 見つかった場合は (slotID, true)、見つからない場合は (挿入位置, false)
func (l *Leaf) SearchSlotID(key []byte) (int, bool) {
	// 二分探索
	lo, hi := 0, l.NumPairs()
	for lo < hi {
		mid := (lo + hi) / 2
		pair := l.PairAt(mid)
		cmp := bytes.Compare(pair.Key, key)
		if cmp < 0 {
			lo = mid + 1
		} else if cmp > 0 {
			hi = mid
		} else {
			return mid, true
		}
	}
	return lo, false
}

// Insert はキーと値を挿入する
// 成功したらtrue、スペース不足ならfalseを返す
func (l *Leaf) Insert(slotID int, key, value []byte) bool {
	pairBytes := (&Pair{Key: key, Value: value}).ToBytes()
	pairLen := len(pairBytes)

	// 空き領域チェック（スロット分 + データ分）
	if l.freeSpace() < LeafSlotSize+pairLen {
		return false
	}

	numPairs := l.NumPairs()

	// スロットをずらす
	for i := numPairs; i > slotID; i-- {
		l.setSlot(i, l.getSlot(i-1))
	}

	// データを書き込む
	newOffset := l.freeSpaceOffset() - uint16(pairLen)
	copy(l.data[newOffset:], pairBytes)
	l.setSlot(slotID, newOffset)
	l.setFreeSpaceOffset(newOffset)
	l.setNumPairs(uint16(numPairs + 1))

	return true
}

// SplitInsert はリーフを分割して挿入する
// 新しいリーフにデータの前半を移動し、オーバーフローキーを返す
func (l *Leaf) SplitInsert(newLeaf *Leaf, key, value []byte) []byte {
	// 全ペアを一時的に取り出す
	pairs := make([]*Pair, l.NumPairs())
	for i := 0; i < l.NumPairs(); i++ {
		pairs[i] = l.PairAt(i)
	}

	// 挿入位置を見つける
	insertPos := 0
	for insertPos < len(pairs) && bytes.Compare(pairs[insertPos].Key, key) < 0 {
		insertPos++
	}

	// 新しいペアを挿入
	newPair := &Pair{Key: key, Value: value}
	pairs = append(pairs[:insertPos], append([]*Pair{newPair}, pairs[insertPos:]...)...)

	// 分割点（中央）
	mid := len(pairs) / 2

	// 新しいリーフ（前半）を再構築
	newLeaf.Initialize()
	for i := 0; i < mid; i++ {
		newLeaf.Insert(i, pairs[i].Key, pairs[i].Value)
	}

	// 現在のリーフ（後半）を再構築
	l.Initialize()
	for i := mid; i < len(pairs); i++ {
		l.Insert(i-mid, pairs[i].Key, pairs[i].Value)
	}

	// オーバーフローキー（新しいリーフの最後のキー）を返す
	return pairs[mid-1].Key
}
