package btree

import (
	"bytes"

	"github.com/kkumaki12/minidb/disk"
)

// Branchノードのレイアウト:
// [num_children: 2] [free_space_offset: 2]
// スロット配列（各2バイト：キーデータへのオフセット）
// 子ページID配列（各8バイト）
// ページ末尾からキーデータが詰められる
//
// 例: n個のキーがある場合、n+1個の子ページIDがある
// keys:     [k0] [k1] [k2] ... [k(n-1)]
// children: [c0] [c1] [c2] ... [cn]
// c0 < k0 <= c1 < k1 <= c2 < ... < k(n-1) <= cn

const (
	BranchNumChildrenOffset     = 0
	BranchFreeSpaceOffsetOffset = 2
	BranchHeaderSize            = 4
	BranchSlotSize              = 2  // キーオフセット
	BranchChildSize             = 8  // PageID
)

// Branch はブランチノードを表す
type Branch struct {
	data []byte
}

// NewBranch はデータからBranchを作成する
func NewBranch(data []byte) *Branch {
	return &Branch{data: data}
}

// NumChildren は子の数を返す
func (b *Branch) NumChildren() int {
	return int(readUint16(b.data[BranchNumChildrenOffset:]))
}

func (b *Branch) setNumChildren(n uint16) {
	writeUint16(b.data[BranchNumChildrenOffset:], n)
}

func (b *Branch) freeSpaceOffset() uint16 {
	return readUint16(b.data[BranchFreeSpaceOffsetOffset:])
}

func (b *Branch) setFreeSpaceOffset(offset uint16) {
	writeUint16(b.data[BranchFreeSpaceOffsetOffset:], offset)
}

// NumKeys はキーの数を返す（子の数 - 1）
func (b *Branch) NumKeys() int {
	n := b.NumChildren()
	if n == 0 {
		return 0
	}
	return n - 1
}

// keySlotOffset はキースロットのオフセットを返す
func (b *Branch) keySlotOffset(idx int) int {
	return BranchHeaderSize + idx*BranchSlotSize
}

// childOffset は子ページIDのオフセットを返す
func (b *Branch) childOffset(idx int) int {
	// スロット配列の後に子ページID配列がある
	maxKeys := b.maxKeys()
	return BranchHeaderSize + maxKeys*BranchSlotSize + idx*BranchChildSize
}

// maxKeys は最大キー数を計算する
func (b *Branch) maxKeys() int {
	// 簡略化：固定値として計算
	// 実際には動的に計算すべきだが、ここでは十分な値を使用
	return 100
}

// getKeySlot は指定インデックスのキーオフセットを返す
func (b *Branch) getKeySlot(idx int) uint16 {
	return readUint16(b.data[b.keySlotOffset(idx):])
}

// setKeySlot は指定インデックスにキーオフセットを設定する
func (b *Branch) setKeySlot(idx int, offset uint16) {
	writeUint16(b.data[b.keySlotOffset(idx):], offset)
}

// ChildAt は指定インデックスの子ページIDを返す
func (b *Branch) ChildAt(idx int) disk.PageID {
	return disk.PageID(readUint64(b.data[b.childOffset(idx):]))
}

// setChild は指定インデックスに子ページIDを設定する
func (b *Branch) setChild(idx int, pageID disk.PageID) {
	writeUint64(b.data[b.childOffset(idx):], uint64(pageID))
}

// KeyAt は指定インデックスのキーを返す
func (b *Branch) KeyAt(idx int) []byte {
	offset := b.getKeySlot(idx)
	keyLen := readUint16(b.data[offset:])
	return b.data[offset+2 : offset+2+keyLen]
}

// Initialize はブランチノードを初期化する
func (b *Branch) Initialize(key []byte, leftChild, rightChild disk.PageID) {
	b.setNumChildren(2)
	b.setFreeSpaceOffset(uint16(len(b.data)))

	// キーを書き込む
	keyLen := len(key)
	newOffset := b.freeSpaceOffset() - uint16(2+keyLen)
	writeUint16(b.data[newOffset:], uint16(keyLen))
	copy(b.data[newOffset+2:], key)
	b.setKeySlot(0, newOffset)
	b.setFreeSpaceOffset(newOffset)

	// 子ページIDを設定
	b.setChild(0, leftChild)
	b.setChild(1, rightChild)
}

// SearchChildIdx はキーに対応する子のインデックスを返す
func (b *Branch) SearchChildIdx(key []byte) int {
	// 二分探索
	lo, hi := 0, b.NumKeys()
	for lo < hi {
		mid := (lo + hi) / 2
		midKey := b.KeyAt(mid)
		if bytes.Compare(midKey, key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// SearchChild はキーに対応する子ページIDを返す
func (b *Branch) SearchChild(key []byte) disk.PageID {
	idx := b.SearchChildIdx(key)
	return b.ChildAt(idx)
}

// freeSpace は空き領域のサイズを返す
func (b *Branch) freeSpace() int {
	numChildren := b.NumChildren()
	slotsEnd := BranchHeaderSize + b.maxKeys()*BranchSlotSize + numChildren*BranchChildSize
	return int(b.freeSpaceOffset()) - slotsEnd
}

// Insert はキーと子ページIDを挿入する
// 成功したらtrue、スペース不足ならfalseを返す
func (b *Branch) Insert(childIdx int, key []byte, newChildPageID disk.PageID) bool {
	keyLen := len(key)
	needed := 2 + keyLen + BranchChildSize // キー長 + キー + 子ページID

	if b.freeSpace() < needed {
		return false
	}

	numChildren := b.NumChildren()
	numKeys := b.NumKeys()

	// 子ページIDをずらす
	for i := numChildren; i > childIdx+1; i-- {
		b.setChild(i, b.ChildAt(i-1))
	}
	b.setChild(childIdx+1, newChildPageID)

	// キースロットをずらす
	for i := numKeys; i > childIdx; i-- {
		b.setKeySlot(i, b.getKeySlot(i-1))
	}

	// キーを書き込む
	newOffset := b.freeSpaceOffset() - uint16(2+keyLen)
	writeUint16(b.data[newOffset:], uint16(keyLen))
	copy(b.data[newOffset+2:], key)
	b.setKeySlot(childIdx, newOffset)
	b.setFreeSpaceOffset(newOffset)

	b.setNumChildren(uint16(numChildren + 1))

	return true
}

// SplitInsert はブランチを分割して挿入する
// オーバーフローキーを返す
func (b *Branch) SplitInsert(newBranch *Branch, key []byte, newChildPageID disk.PageID) []byte {
	// 全データを一時的に取り出す
	numKeys := b.NumKeys()
	keys := make([][]byte, numKeys)
	children := make([]disk.PageID, numKeys+1)

	for i := 0; i < numKeys; i++ {
		keys[i] = append([]byte{}, b.KeyAt(i)...)
	}
	for i := 0; i <= numKeys; i++ {
		children[i] = b.ChildAt(i)
	}

	// 挿入位置を見つける
	insertPos := 0
	for insertPos < len(keys) && bytes.Compare(keys[insertPos], key) <= 0 {
		insertPos++
	}

	// 新しいキーと子を挿入
	keys = append(keys[:insertPos], append([][]byte{key}, keys[insertPos:]...)...)
	children = append(children[:insertPos+1], append([]disk.PageID{newChildPageID}, children[insertPos+1:]...)...)

	// 分割点
	mid := len(keys) / 2
	overflowKey := keys[mid]

	// 新しいブランチ（前半）を構築
	newBranch.setNumChildren(uint16(mid + 1))
	newBranch.setFreeSpaceOffset(uint16(len(newBranch.data)))
	for i := 0; i < mid; i++ {
		k := keys[i]
		newOffset := newBranch.freeSpaceOffset() - uint16(2+len(k))
		writeUint16(newBranch.data[newOffset:], uint16(len(k)))
		copy(newBranch.data[newOffset+2:], k)
		newBranch.setKeySlot(i, newOffset)
		newBranch.setFreeSpaceOffset(newOffset)
	}
	for i := 0; i <= mid; i++ {
		newBranch.setChild(i, children[i])
	}

	// 現在のブランチ（後半）を再構築
	b.setNumChildren(uint16(len(keys) - mid))
	b.setFreeSpaceOffset(uint16(len(b.data)))
	for i := mid + 1; i < len(keys); i++ {
		k := keys[i]
		newOffset := b.freeSpaceOffset() - uint16(2+len(k))
		writeUint16(b.data[newOffset:], uint16(len(k)))
		copy(b.data[newOffset+2:], k)
		b.setKeySlot(i-mid-1, newOffset)
		b.setFreeSpaceOffset(newOffset)
	}
	for i := mid + 1; i <= len(keys); i++ {
		b.setChild(i-mid-1, children[i])
	}

	return overflowKey
}
