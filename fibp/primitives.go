// Package fibp implements the Fila Binary Protocol (FIBP) codec.
//
// All multi-byte integers are big-endian (network byte order).
// Strings are encoded as [u16 length][UTF-8 bytes].
// Byte arrays are encoded as [u32 length][raw bytes].
package fibp

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// Writer encodes FIBP primitives into a byte buffer.
type Writer struct {
	buf []byte
}

// NewWriter creates a Writer with the given initial capacity.
func NewWriter(capacity int) *Writer {
	return &Writer{buf: make([]byte, 0, capacity)}
}

// Bytes returns the accumulated bytes.
func (w *Writer) Bytes() []byte { return w.buf }

// Len returns the current buffer length.
func (w *Writer) Len() int { return len(w.buf) }

// Reset clears the buffer.
func (w *Writer) Reset() { w.buf = w.buf[:0] }

func (w *Writer) WriteU8(v uint8) {
	w.buf = append(w.buf, v)
}

func (w *Writer) WriteU16(v uint16) {
	w.buf = append(w.buf, 0, 0)
	binary.BigEndian.PutUint16(w.buf[len(w.buf)-2:], v)
}

func (w *Writer) WriteU32(v uint32) {
	w.buf = append(w.buf, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(w.buf[len(w.buf)-4:], v)
}

func (w *Writer) WriteU64(v uint64) {
	w.buf = append(w.buf, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64(w.buf[len(w.buf)-8:], v)
}

func (w *Writer) WriteI64(v int64) {
	w.WriteU64(uint64(v))
}

func (w *Writer) WriteF64(v float64) {
	w.WriteU64(math.Float64bits(v))
}

func (w *Writer) WriteBool(v bool) {
	if v {
		w.buf = append(w.buf, 1)
	} else {
		w.buf = append(w.buf, 0)
	}
}

// WriteString encodes a string as [u16 length][UTF-8 bytes].
func (w *Writer) WriteString(s string) error {
	if len(s) > math.MaxUint16 {
		return fmt.Errorf("string too long: %d bytes (max %d)", len(s), math.MaxUint16)
	}
	w.WriteU16(uint16(len(s)))
	w.buf = append(w.buf, s...)
	return nil
}

// WriteBytes encodes a byte slice as [u32 length][raw bytes].
func (w *Writer) WriteBytes(b []byte) {
	w.WriteU32(uint32(len(b)))
	w.buf = append(w.buf, b...)
}

// WriteStringMap encodes a map as [u16 count][repeated: string key, string value].
func (w *Writer) WriteStringMap(m map[string]string) error {
	if len(m) > math.MaxUint16 {
		return fmt.Errorf("map too large: %d entries (max %d)", len(m), math.MaxUint16)
	}
	w.WriteU16(uint16(len(m)))
	for k, v := range m {
		if err := w.WriteString(k); err != nil {
			return err
		}
		if err := w.WriteString(v); err != nil {
			return err
		}
	}
	return nil
}

// WriteStringSlice encodes a string slice as [u16 count][repeated: string].
func (w *Writer) WriteStringSlice(ss []string) error {
	if len(ss) > math.MaxUint16 {
		return fmt.Errorf("slice too large: %d entries (max %d)", len(ss), math.MaxUint16)
	}
	w.WriteU16(uint16(len(ss)))
	for _, s := range ss {
		if err := w.WriteString(s); err != nil {
			return err
		}
	}
	return nil
}

// WriteOptionalString encodes an optional string as [u8 present][string if present].
func (w *Writer) WriteOptionalString(s *string) error {
	if s == nil {
		w.WriteU8(0)
		return nil
	}
	w.WriteU8(1)
	return w.WriteString(*s)
}

// Reader decodes FIBP primitives from a byte slice.
type Reader struct {
	data []byte
	pos  int
}

// NewReader creates a Reader over the given data.
func NewReader(data []byte) *Reader {
	return &Reader{data: data}
}

// Remaining returns the number of unread bytes.
func (r *Reader) Remaining() int { return len(r.data) - r.pos }

func (r *Reader) need(n int) error {
	if r.pos+n > len(r.data) {
		return fmt.Errorf("unexpected end of data: need %d bytes, have %d", n, r.Remaining())
	}
	return nil
}

func (r *Reader) ReadU8() (uint8, error) {
	if err := r.need(1); err != nil {
		return 0, err
	}
	v := r.data[r.pos]
	r.pos++
	return v, nil
}

func (r *Reader) ReadU16() (uint16, error) {
	if err := r.need(2); err != nil {
		return 0, err
	}
	v := binary.BigEndian.Uint16(r.data[r.pos:])
	r.pos += 2
	return v, nil
}

func (r *Reader) ReadU32() (uint32, error) {
	if err := r.need(4); err != nil {
		return 0, err
	}
	v := binary.BigEndian.Uint32(r.data[r.pos:])
	r.pos += 4
	return v, nil
}

func (r *Reader) ReadU64() (uint64, error) {
	if err := r.need(8); err != nil {
		return 0, err
	}
	v := binary.BigEndian.Uint64(r.data[r.pos:])
	r.pos += 8
	return v, nil
}

func (r *Reader) ReadI64() (int64, error) {
	v, err := r.ReadU64()
	return int64(v), err
}

func (r *Reader) ReadF64() (float64, error) {
	v, err := r.ReadU64()
	return math.Float64frombits(v), err
}

func (r *Reader) ReadBool() (bool, error) {
	v, err := r.ReadU8()
	if err != nil {
		return false, err
	}
	return v != 0, nil
}

// ReadString decodes a [u16 length][UTF-8 bytes] string.
func (r *Reader) ReadString() (string, error) {
	length, err := r.ReadU16()
	if err != nil {
		return "", err
	}
	if err := r.need(int(length)); err != nil {
		return "", err
	}
	s := string(r.data[r.pos : r.pos+int(length)])
	r.pos += int(length)
	return s, nil
}

// ReadBytes decodes a [u32 length][raw bytes] byte slice.
func (r *Reader) ReadBytes() ([]byte, error) {
	length, err := r.ReadU32()
	if err != nil {
		return nil, err
	}
	if err := r.need(int(length)); err != nil {
		return nil, err
	}
	b := make([]byte, length)
	copy(b, r.data[r.pos:r.pos+int(length)])
	r.pos += int(length)
	return b, nil
}

// ReadStringMap decodes a [u16 count][repeated: string key, string value] map.
func (r *Reader) ReadStringMap() (map[string]string, error) {
	count, err := r.ReadU16()
	if err != nil {
		return nil, err
	}
	m := make(map[string]string, count)
	for i := 0; i < int(count); i++ {
		k, err := r.ReadString()
		if err != nil {
			return nil, err
		}
		v, err := r.ReadString()
		if err != nil {
			return nil, err
		}
		m[k] = v
	}
	return m, nil
}

// ReadStringSlice decodes a [u16 count][repeated: string] slice.
func (r *Reader) ReadStringSlice() ([]string, error) {
	count, err := r.ReadU16()
	if err != nil {
		return nil, err
	}
	ss := make([]string, count)
	for i := 0; i < int(count); i++ {
		s, err := r.ReadString()
		if err != nil {
			return nil, err
		}
		ss[i] = s
	}
	return ss, nil
}

// ReadOptionalString decodes an optional string [u8 present][string if present].
func (r *Reader) ReadOptionalString() (*string, error) {
	present, err := r.ReadU8()
	if err != nil {
		return nil, err
	}
	if present == 0 {
		return nil, nil
	}
	s, err := r.ReadString()
	if err != nil {
		return nil, err
	}
	return &s, nil
}

// ReadFrameFrom reads a length-prefixed frame from an io.Reader.
// Returns the frame body (excluding the 4-byte length prefix).
func ReadFrameFrom(r io.Reader, maxFrameSize uint32) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	frameLen := binary.BigEndian.Uint32(lenBuf[:])
	if frameLen > maxFrameSize {
		return nil, fmt.Errorf("frame too large: %d bytes (max %d)", frameLen, maxFrameSize)
	}
	body := make([]byte, frameLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	return body, nil
}

// WriteFrameTo writes a length-prefixed frame to an io.Writer.
func WriteFrameTo(w io.Writer, body []byte) error {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(body)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err := w.Write(body)
	return err
}
