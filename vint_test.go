package sequencefile

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

// To genenerate these tests in a scala shell:
// scala> def vIntHex(l: Long) = {
//   val baos = new java.io.ByteArrayOutputStream()
//   val dos = new java.io.DataOutputStream(baos)
//   baos.reset()
//   org.apache.hadoop.io.WritableUtils.writeVLong(dos, l)
//   Hex.encodeHexString(baos.toByteArray)
// }
var vints = []struct {
	b []byte
	i int64
}{
	{[]byte{0x00}, 0},
	{[]byte{0x01}, 1},
	{[]byte{0xff}, -1},
	{[]byte{0x64}, 100},
	{[]byte{0x9c}, -100},
	{[]byte{0x8f, 0xc8}, 200},
	{[]byte{0x87, 0xc7}, -200},
	{[]byte{0x8e, 0x1f, 0xff}, 8191},
	{[]byte{0x86, 0x1f, 0xfe}, -8191},
	{[]byte{0x8c, 0x7f, 0xff, 0xff, 0xff}, 2147483647},
	{[]byte{0x84, 0x7f, 0xff, 0xff, 0xfe}, -2147483647},
	{[]byte{0x8c, 0x6d, 0x7f, 0x77, 0x58}, 1837070168},
	{[]byte{0x84, 0x6d, 0x7f, 0x77, 0x57}, -1837070168},
	{[]byte{0x8c, 0xff, 0xff, 0xff, 0xfe}, 4294967294},
	{[]byte{0x84, 0xff, 0xff, 0xff, 0xfd}, -4294967294},
	{[]byte{0x88, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, 576460752303423488},
	{[]byte{0x80, 0x07, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, -576460752303423488},
}

func TestVInt(t *testing.T) {
	for _, spec := range vints {
		t.Run("ReadVInt "+strconv.FormatInt(spec.i, 10), func(t *testing.T) {
			res, err := ReadVInt(bytes.NewBuffer(spec.b))
			assert.NoError(t, err, "ReadVInt should return successfully")
			assert.Equal(t, spec.i, res, "ReadVInt should return the correct result")
		})
		t.Run("WriteVInt "+strconv.FormatInt(spec.i, 10), func(t *testing.T) {
			var buf bytes.Buffer
			err := WriteVInt(&buf, spec.i)
			assert.NoError(t, err, "WriteVInt should return successfully")
			assert.Equal(t, spec.b, buf.Bytes(), "WriteVInt should write the correct result")
		})
	}
}
