package btree

import (
	"github.com/kkumaki12/minidb/disk"
)

// MetaHeader はメタページのヘッダー情報
// ルートページのIDを保持する
type MetaHeader struct {
	RootPageID disk.PageID
}

const MetaHeaderSize = 8

// Meta はB-treeのメタデータページを表す
type Meta struct {
	Header *MetaHeader
	data   []byte
}

// NewMeta はページデータからMetaを作成する
func NewMeta(data []byte) *Meta {
	return &Meta{
		Header: &MetaHeader{
			RootPageID: disk.PageID(readUint64(data[0:8])),
		},
		data: data,
	}
}

// Sync はヘッダーの内容をデータに書き戻す
func (m *Meta) Sync() {
	writeUint64(m.data[0:8], uint64(m.Header.RootPageID))
}
