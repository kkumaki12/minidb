package btree

import (
	"encoding/binary"
)

// Pair はキーと値のペアを表す
type Pair struct {
	Key   []byte
	Value []byte
}

// ToBytes はPairをバイト列にシリアライズする
// フォーマット: [key_len(2)] [value_len(2)] [key] [value]
func (p *Pair) ToBytes() []byte {
	keyLen := len(p.Key)
	valueLen := len(p.Value)
	buf := make([]byte, 4+keyLen+valueLen)
	binary.LittleEndian.PutUint16(buf[0:2], uint16(keyLen))
	binary.LittleEndian.PutUint16(buf[2:4], uint16(valueLen))
	copy(buf[4:4+keyLen], p.Key)
	copy(buf[4+keyLen:], p.Value)
	return buf
}

// PairFromBytes はバイト列からPairをデシリアライズする
func PairFromBytes(data []byte) *Pair {
	keyLen := binary.LittleEndian.Uint16(data[0:2])
	valueLen := binary.LittleEndian.Uint16(data[2:4])
	key := make([]byte, keyLen)
	value := make([]byte, valueLen)
	copy(key, data[4:4+keyLen])
	copy(value, data[4+keyLen:4+int(keyLen)+int(valueLen)])
	return &Pair{Key: key, Value: value}
}

// PairSize はシリアライズ後のバイト数を返す
func PairSize(keyLen, valueLen int) int {
	return 4 + keyLen + valueLen
}
