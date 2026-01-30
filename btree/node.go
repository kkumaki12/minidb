package btree

import (
	"encoding/binary"
)

// NodeType はノードの種類を表す
type NodeType uint8

const (
	NodeTypeLeaf   NodeType = 1
	NodeTypeBranch NodeType = 2
)

// ノードヘッダーのサイズ
const NodeHeaderSize = 8

// NodeHeader はノードのヘッダー情報
type NodeHeader struct {
	NodeType NodeType
}

// Node はB-treeのノードを表す
type Node struct {
	Header NodeHeader
	Body   []byte
}

// NewNode はページデータからNodeを作成する
func NewNode(data []byte) *Node {
	return &Node{
		Header: NodeHeader{
			NodeType: NodeType(data[0]),
		},
		Body: data[NodeHeaderSize:],
	}
}

// InitializeAsLeaf はノードをリーフノードとして初期化する
func (n *Node) InitializeAsLeaf() {
	n.Header.NodeType = NodeTypeLeaf
}

// InitializeAsBranch はノードをブランチノードとして初期化する
func (n *Node) InitializeAsBranch() {
	n.Header.NodeType = NodeTypeBranch
}

// WriteHeader はヘッダーをバイト列に書き込む
func (n *Node) WriteHeader(data []byte) {
	data[0] = byte(n.Header.NodeType)
}

// ヘルパー関数：バイト列からuint64を読む
func readUint64(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data)
}

// ヘルパー関数：バイト列にuint64を書く
func writeUint64(data []byte, v uint64) {
	binary.LittleEndian.PutUint64(data, v)
}

// ヘルパー関数：バイト列からuint16を読む
func readUint16(data []byte) uint16 {
	return binary.LittleEndian.Uint16(data)
}

// ヘルパー関数：バイト列にuint16を書く
func writeUint16(data []byte, v uint16) {
	binary.LittleEndian.PutUint16(data, v)
}
