package btree

import (
	"errors"

	"github.com/kkumaki12/minidb/buffer"
	"github.com/kkumaki12/minidb/disk"
)

// エラー定義
var (
	ErrDuplicateKey = errors.New("duplicate key")
)

// SearchMode は検索モードを表す
type SearchMode int

const (
	// SearchModeStart は先頭から検索を開始する
	SearchModeStart SearchMode = iota
	// SearchModeKey は指定キーから検索を開始する
	SearchModeKey
)

// Search は検索条件を表す
type Search struct {
	Mode SearchMode
	Key  []byte
}

// NewSearchStart は先頭からの検索を作成する
func NewSearchStart() *Search {
	return &Search{Mode: SearchModeStart}
}

// NewSearchKey は指定キーからの検索を作成する
func NewSearchKey(key []byte) *Search {
	return &Search{Mode: SearchModeKey, Key: key}
}

// childPageID はブランチノードから子ページIDを取得する
func (s *Search) childPageID(branch *Branch) disk.PageID {
	switch s.Mode {
	case SearchModeStart:
		return branch.ChildAt(0)
	case SearchModeKey:
		return branch.SearchChild(s.Key)
	}
	return branch.ChildAt(0)
}

// tupleSlotID はリーフノードからスロットIDを取得する
// 見つかった場合は (slotID, true)、見つからない場合は (挿入位置, false)
func (s *Search) tupleSlotID(leaf *Leaf) (int, bool) {
	switch s.Mode {
	case SearchModeStart:
		return 0, false
	case SearchModeKey:
		return leaf.SearchSlotID(s.Key)
	}
	return 0, false
}

// BTree はB+木を表す
type BTree struct {
	MetaPageID disk.PageID
}

// Create は新しいB-treeを作成する
func Create(bufmgr *buffer.BufferPoolManager) (*BTree, error) {
	// メタページを作成
	metaBuffer, err := bufmgr.CreatePage()
	if err != nil {
		return nil, err
	}
	meta := NewMeta(metaBuffer.Page[:])

	// ルートページ（リーフ）を作成
	rootBuffer, err := bufmgr.CreatePage()
	if err != nil {
		return nil, err
	}
	rootNode := NewNode(rootBuffer.Page[:])
	rootNode.InitializeAsLeaf()
	rootNode.WriteHeader(rootBuffer.Page[:])
	leaf := NewLeaf(rootBuffer.Page[NodeHeaderSize:])
	leaf.Initialize()

	// メタページにルートページIDを設定
	meta.Header.RootPageID = rootBuffer.PageID
	meta.Sync()

	metaBuffer.IsDirty = true
	rootBuffer.IsDirty = true

	return &BTree{MetaPageID: metaBuffer.PageID}, nil
}

// NewBTree は既存のB-treeを開く
func NewBTree(metaPageID disk.PageID) *BTree {
	return &BTree{MetaPageID: metaPageID}
}

// fetchRootPage はルートページを取得する
func (t *BTree) fetchRootPage(bufmgr *buffer.BufferPoolManager) (*buffer.Buffer, error) {
	metaBuffer, err := bufmgr.FetchPage(t.MetaPageID)
	if err != nil {
		return nil, err
	}
	meta := NewMeta(metaBuffer.Page[:])
	rootPageID := meta.Header.RootPageID

	return bufmgr.FetchPage(rootPageID)
}

// Search は指定された検索条件でイテレータを返す
func (t *BTree) Search(bufmgr *buffer.BufferPoolManager, search *Search) (*Iter, error) {
	rootBuffer, err := t.fetchRootPage(bufmgr)
	if err != nil {
		return nil, err
	}
	return t.searchInternal(bufmgr, rootBuffer, search)
}

// searchInternal は内部検索処理
func (t *BTree) searchInternal(bufmgr *buffer.BufferPoolManager, nodeBuffer *buffer.Buffer, search *Search) (*Iter, error) {
	node := NewNode(nodeBuffer.Page[:])

	switch node.Header.NodeType {
	case NodeTypeLeaf:
		leaf := NewLeaf(nodeBuffer.Page[NodeHeaderSize:])
		slotID, _ := search.tupleSlotID(leaf)
		isRightMost := leaf.NumPairs() == slotID

		iter := &Iter{
			buffer: nodeBuffer,
			slotID: slotID,
		}

		if isRightMost {
			if err := iter.advance(bufmgr); err != nil {
				return nil, err
			}
		}
		return iter, nil

	case NodeTypeBranch:
		branch := NewBranch(nodeBuffer.Page[NodeHeaderSize:])
		childPageID := search.childPageID(branch)
		childBuffer, err := bufmgr.FetchPage(childPageID)
		if err != nil {
			return nil, err
		}
		return t.searchInternal(bufmgr, childBuffer, search)
	}

	return nil, errors.New("invalid node type")
}

// Insert はキーと値を挿入する
func (t *BTree) Insert(bufmgr *buffer.BufferPoolManager, key, value []byte) error {
	metaBuffer, err := bufmgr.FetchPage(t.MetaPageID)
	if err != nil {
		return err
	}
	meta := NewMeta(metaBuffer.Page[:])
	rootPageID := meta.Header.RootPageID

	rootBuffer, err := bufmgr.FetchPage(rootPageID)
	if err != nil {
		return err
	}

	overflow, err := t.insertInternal(bufmgr, rootBuffer, key, value)
	if err != nil {
		return err
	}

	// オーバーフローがあれば新しいルートを作成
	if overflow != nil {
		newRootBuffer, err := bufmgr.CreatePage()
		if err != nil {
			return err
		}
		newRootNode := NewNode(newRootBuffer.Page[:])
		newRootNode.InitializeAsBranch()
		newRootNode.WriteHeader(newRootBuffer.Page[:])
		branch := NewBranch(newRootBuffer.Page[NodeHeaderSize:])
		branch.Initialize(overflow.key, overflow.childPageID, rootPageID)

		meta.Header.RootPageID = newRootBuffer.PageID
		meta.Sync()
		metaBuffer.IsDirty = true
		newRootBuffer.IsDirty = true
	}

	return nil
}

// overflow は分割時のオーバーフロー情報
type overflow struct {
	key         []byte
	childPageID disk.PageID
}

// insertInternal は内部挿入処理
func (t *BTree) insertInternal(bufmgr *buffer.BufferPoolManager, nodeBuffer *buffer.Buffer, key, value []byte) (*overflow, error) {
	node := NewNode(nodeBuffer.Page[:])

	switch node.Header.NodeType {
	case NodeTypeLeaf:
		leaf := NewLeaf(nodeBuffer.Page[NodeHeaderSize:])
		slotID, found := leaf.SearchSlotID(key)
		if found {
			return nil, ErrDuplicateKey
		}

		if leaf.Insert(slotID, key, value) {
			nodeBuffer.IsDirty = true
			return nil, nil
		}

		// スペース不足：分割が必要
		prevPageID := leaf.PrevPageID()
		var prevBuffer *buffer.Buffer
		if prevPageID != nil {
			var err error
			prevBuffer, err = bufmgr.FetchPage(*prevPageID)
			if err != nil {
				return nil, err
			}
		}

		newLeafBuffer, err := bufmgr.CreatePage()
		if err != nil {
			return nil, err
		}

		// 前のリーフのnextを更新
		if prevBuffer != nil {
			prevNode := NewNode(prevBuffer.Page[:])
			prevLeaf := NewLeaf(prevNode.Body)
			prevLeaf.SetNextPageID(&newLeafBuffer.PageID)
			prevBuffer.IsDirty = true
		}
		leaf.SetPrevPageID(&newLeafBuffer.PageID)

		// 新しいリーフを初期化
		newLeafNode := NewNode(newLeafBuffer.Page[:])
		newLeafNode.InitializeAsLeaf()
		newLeafNode.WriteHeader(newLeafBuffer.Page[:])
		newLeaf := NewLeaf(newLeafBuffer.Page[NodeHeaderSize:])
		newLeaf.Initialize()

		// 分割
		overflowKey := leaf.SplitInsert(newLeaf, key, value)
		newLeaf.SetNextPageID(&nodeBuffer.PageID)
		newLeaf.SetPrevPageID(prevPageID)

		nodeBuffer.IsDirty = true
		newLeafBuffer.IsDirty = true

		return &overflow{key: overflowKey, childPageID: newLeafBuffer.PageID}, nil

	case NodeTypeBranch:
		branch := NewBranch(nodeBuffer.Page[NodeHeaderSize:])
		childIdx := branch.SearchChildIdx(key)
		childPageID := branch.ChildAt(childIdx)

		childBuffer, err := bufmgr.FetchPage(childPageID)
		if err != nil {
			return nil, err
		}

		childOverflow, err := t.insertInternal(bufmgr, childBuffer, key, value)
		if err != nil {
			return nil, err
		}

		if childOverflow == nil {
			return nil, nil
		}

		if branch.Insert(childIdx, childOverflow.key, childOverflow.childPageID) {
			nodeBuffer.IsDirty = true
			return nil, nil
		}

		// ブランチの分割
		newBranchBuffer, err := bufmgr.CreatePage()
		if err != nil {
			return nil, err
		}
		newBranchNode := NewNode(newBranchBuffer.Page[:])
		newBranchNode.InitializeAsBranch()
		newBranchNode.WriteHeader(newBranchBuffer.Page[:])
		newBranch := NewBranch(newBranchBuffer.Page[NodeHeaderSize:])

		overflowKey := branch.SplitInsert(newBranch, childOverflow.key, childOverflow.childPageID)

		nodeBuffer.IsDirty = true
		newBranchBuffer.IsDirty = true

		return &overflow{key: overflowKey, childPageID: newBranchBuffer.PageID}, nil
	}

	return nil, errors.New("invalid node type")
}

// Iter はB-treeのイテレータ
type Iter struct {
	buffer *buffer.Buffer
	slotID int
}

// get は現在位置のキーと値を返す
func (it *Iter) get() *Pair {
	leaf := NewLeaf(it.buffer.Page[NodeHeaderSize:])
	if it.slotID < leaf.NumPairs() {
		return leaf.PairAt(it.slotID)
	}
	return nil
}

// advance は次の位置に進む
func (it *Iter) advance(bufmgr *buffer.BufferPoolManager) error {
	it.slotID++
	leaf := NewLeaf(it.buffer.Page[NodeHeaderSize:])
	if it.slotID < leaf.NumPairs() {
		return nil
	}

	nextPageID := leaf.NextPageID()
	if nextPageID != nil {
		nextBuffer, err := bufmgr.FetchPage(*nextPageID)
		if err != nil {
			return err
		}
		it.buffer = nextBuffer
		it.slotID = 0
	}
	return nil
}

// Next は次のキーと値を返す
func (it *Iter) Next(bufmgr *buffer.BufferPoolManager) (*Pair, error) {
	pair := it.get()
	if err := it.advance(bufmgr); err != nil {
		return nil, err
	}
	return pair, nil
}
