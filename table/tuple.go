package table

import (
	"encoding/binary"
)

// Tuple はテーブルの1行を表す
// 各要素はバイト列として格納される
type Tuple [][]byte

// Encode はTupleをバイト列にエンコードする
// フォーマット: [num_elems: 2] ([elem_len: 2] [elem_data])...
func (t Tuple) Encode() []byte {
	// サイズを計算
	size := 2 // num_elems
	for _, elem := range t {
		size += 2 + len(elem) // elem_len + elem_data
	}

	buf := make([]byte, size)
	offset := 0

	// 要素数
	binary.LittleEndian.PutUint16(buf[offset:], uint16(len(t)))
	offset += 2

	// 各要素
	for _, elem := range t {
		binary.LittleEndian.PutUint16(buf[offset:], uint16(len(elem)))
		offset += 2
		copy(buf[offset:], elem)
		offset += len(elem)
	}

	return buf
}

// DecodeTuple はバイト列からTupleをデコードする
func DecodeTuple(data []byte) Tuple {
	numElems := int(binary.LittleEndian.Uint16(data[0:2]))
	offset := 2

	tuple := make(Tuple, numElems)
	for i := 0; i < numElems; i++ {
		elemLen := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += 2
		elem := make([]byte, elemLen)
		copy(elem, data[offset:offset+elemLen])
		offset += elemLen
		tuple[i] = elem
	}

	return tuple
}

// SplitTuple はTupleをキー部分と値部分に分割する
func SplitTuple(tuple Tuple, numKeyElems int) (key Tuple, value Tuple) {
	if numKeyElems > len(tuple) {
		numKeyElems = len(tuple)
	}
	return tuple[:numKeyElems], tuple[numKeyElems:]
}

// MergeTuple はキーと値を結合してTupleを作成する
func MergeTuple(key, value Tuple) Tuple {
	result := make(Tuple, len(key)+len(value))
	copy(result, key)
	copy(result[len(key):], value)
	return result
}
