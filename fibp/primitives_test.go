package fibp

import (
	"bytes"
	"math"
	"testing"
)

func TestPrimitivesRoundTrip(t *testing.T) {
	w := NewWriter(256)
	w.WriteU8(42)
	w.WriteU16(1234)
	w.WriteU32(0xDEADBEEF)
	w.WriteU64(0x0102030405060708)
	w.WriteI64(-12345678)
	w.WriteF64(3.14159)
	w.WriteBool(true)
	w.WriteBool(false)

	r := NewReader(w.Bytes())

	v8, err := r.ReadU8()
	assertNoError(t, err)
	assertEqual(t, uint8(42), v8)

	v16, err := r.ReadU16()
	assertNoError(t, err)
	assertEqual(t, uint16(1234), v16)

	v32, err := r.ReadU32()
	assertNoError(t, err)
	assertEqual(t, uint32(0xDEADBEEF), v32)

	v64, err := r.ReadU64()
	assertNoError(t, err)
	assertEqual(t, uint64(0x0102030405060708), v64)

	vi64, err := r.ReadI64()
	assertNoError(t, err)
	assertEqual(t, int64(-12345678), vi64)

	vf64, err := r.ReadF64()
	assertNoError(t, err)
	if math.Abs(vf64-3.14159) > 1e-10 {
		t.Fatalf("expected 3.14159, got %f", vf64)
	}

	vb1, err := r.ReadBool()
	assertNoError(t, err)
	assertEqual(t, true, vb1)

	vb2, err := r.ReadBool()
	assertNoError(t, err)
	assertEqual(t, false, vb2)

	assertEqual(t, 0, r.Remaining())
}

func TestStringRoundTrip(t *testing.T) {
	w := NewWriter(64)
	assertNoError(t, w.WriteString("hello world"))
	assertNoError(t, w.WriteString(""))
	assertNoError(t, w.WriteString("unicode: \u00e9\u00e0\u00fc"))

	r := NewReader(w.Bytes())
	s1, err := r.ReadString()
	assertNoError(t, err)
	assertEqual(t, "hello world", s1)

	s2, err := r.ReadString()
	assertNoError(t, err)
	assertEqual(t, "", s2)

	s3, err := r.ReadString()
	assertNoError(t, err)
	assertEqual(t, "unicode: \u00e9\u00e0\u00fc", s3)
}

func TestBytesRoundTrip(t *testing.T) {
	data := []byte{0x01, 0x02, 0x03, 0xFF, 0x00}
	w := NewWriter(32)
	w.WriteBytes(data)
	w.WriteBytes(nil)

	r := NewReader(w.Bytes())
	b1, err := r.ReadBytes()
	assertNoError(t, err)
	if !bytes.Equal(data, b1) {
		t.Fatalf("bytes mismatch: got %v", b1)
	}

	b2, err := r.ReadBytes()
	assertNoError(t, err)
	assertEqual(t, 0, len(b2))
}

func TestStringMapRoundTrip(t *testing.T) {
	m := map[string]string{"key1": "val1", "key2": "val2"}
	w := NewWriter(64)
	assertNoError(t, w.WriteStringMap(m))

	r := NewReader(w.Bytes())
	m2, err := r.ReadStringMap()
	assertNoError(t, err)
	assertEqual(t, len(m), len(m2))
	for k, v := range m {
		assertEqual(t, v, m2[k])
	}
}

func TestStringSliceRoundTrip(t *testing.T) {
	ss := []string{"a", "bb", "ccc"}
	w := NewWriter(32)
	assertNoError(t, w.WriteStringSlice(ss))

	r := NewReader(w.Bytes())
	ss2, err := r.ReadStringSlice()
	assertNoError(t, err)
	assertEqual(t, len(ss), len(ss2))
	for i, s := range ss {
		assertEqual(t, s, ss2[i])
	}
}

func TestOptionalStringRoundTrip(t *testing.T) {
	w := NewWriter(32)
	s := "present"
	assertNoError(t, w.WriteOptionalString(&s))
	assertNoError(t, w.WriteOptionalString(nil))

	r := NewReader(w.Bytes())
	o1, err := r.ReadOptionalString()
	assertNoError(t, err)
	if o1 == nil || *o1 != "present" {
		t.Fatalf("expected 'present', got %v", o1)
	}

	o2, err := r.ReadOptionalString()
	assertNoError(t, err)
	if o2 != nil {
		t.Fatalf("expected nil, got %v", *o2)
	}
}

func TestFrameHeaderRoundTrip(t *testing.T) {
	hdr := FrameHeader{Opcode: OpcodeEnqueue, Flags: FlagContinuation, RequestID: 12345}
	w := NewWriter(HeaderSize)
	EncodeHeader(w, hdr)
	assertEqual(t, HeaderSize, w.Len())

	r := NewReader(w.Bytes())
	hdr2, err := DecodeHeader(r)
	assertNoError(t, err)
	assertEqual(t, hdr.Opcode, hdr2.Opcode)
	assertEqual(t, hdr.Flags, hdr2.Flags)
	assertEqual(t, hdr.RequestID, hdr2.RequestID)
	assertEqual(t, true, hdr2.IsContinuation())
}

func TestFrameIO(t *testing.T) {
	body := []byte("hello frame")
	var buf bytes.Buffer
	assertNoError(t, WriteFrameTo(&buf, body))

	got, err := ReadFrameFrom(&buf, DefaultMaxFrameSize)
	assertNoError(t, err)
	if !bytes.Equal(body, got) {
		t.Fatalf("frame body mismatch: got %q", got)
	}
}

func TestReaderTruncated(t *testing.T) {
	r := NewReader([]byte{0x01})
	_, err := r.ReadU16()
	if err == nil {
		t.Fatal("expected error on truncated data")
	}
}

func assertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func assertEqual[T comparable](t *testing.T, expected, actual T) {
	t.Helper()
	if expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
}
