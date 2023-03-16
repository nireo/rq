package store

import "encoding/binary"

var (
	dacksSize = 4
)

type Value struct {
	Dacks uint32
	Raw   []byte
}

// Encode writes a value into bytes. It simply encodes the uint32 into bytes and appends the
// raw value after that.
func (v *Value) Encode() []byte {
	buf := make([]byte, dacksSize+len(v.Raw))
	binary.LittleEndian.PutUint32(buf, v.Dacks)
	copy(buf[dacksSize:], v.Raw)

	return buf
}

func Decode(buf []byte) *Value {
	dacks := binary.LittleEndian.Uint32(buf)

	return &Value{
		Dacks: dacks,
		Raw:   buf[dacksSize:],
	}
}

func NewValue(data []byte) *Value {
	return &Value{
		Raw: data,
	}
}
