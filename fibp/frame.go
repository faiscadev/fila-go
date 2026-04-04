package fibp

import (
	"fmt"
	"io"
	"sync"
)

// FrameWriter writes length-prefixed FIBP frames to an io.Writer.
// It handles continuation frames for payloads exceeding maxFrameSize.
// It is safe for concurrent use.
type FrameWriter struct {
	w            io.Writer
	mu           sync.Mutex
	maxFrameSize uint32
}

// NewFrameWriter creates a FrameWriter with the given max frame size.
func NewFrameWriter(w io.Writer, maxFrameSize uint32) *FrameWriter {
	if maxFrameSize == 0 {
		maxFrameSize = DefaultMaxFrameSize
	}
	return &FrameWriter{w: w, maxFrameSize: maxFrameSize}
}

// WriteFrame writes a complete frame (header + body). If the body exceeds
// maxFrameSize - HeaderSize, it splits across continuation frames.
func (fw *FrameWriter) WriteFrame(opcode Opcode, requestID uint32, body []byte) error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	maxBody := int(fw.maxFrameSize) - HeaderSize
	if maxBody <= 0 {
		return fmt.Errorf("maxFrameSize %d too small (must be > %d)", fw.maxFrameSize, HeaderSize)
	}

	if len(body) <= maxBody {
		// Single frame, no continuation.
		w := NewWriter(HeaderSize + len(body))
		EncodeHeader(w, FrameHeader{
			Opcode:    opcode,
			Flags:     0,
			RequestID: requestID,
		})
		w.buf = append(w.buf, body...)
		return WriteFrameTo(fw.w, w.Bytes())
	}

	// Multi-frame: split body across continuation frames.
	offset := 0
	for offset < len(body) {
		end := offset + maxBody
		if end > len(body) {
			end = len(body)
		}
		chunk := body[offset:end]
		isLast := end == len(body)

		var flags uint8
		if !isLast {
			flags = FlagContinuation
		}

		w := NewWriter(HeaderSize + len(chunk))
		EncodeHeader(w, FrameHeader{
			Opcode:    opcode,
			Flags:     flags,
			RequestID: requestID,
		})
		w.buf = append(w.buf, chunk...)
		if err := WriteFrameTo(fw.w, w.Bytes()); err != nil {
			return err
		}
		offset = end
	}
	return nil
}

// FrameReader reads length-prefixed FIBP frames from an io.Reader.
// It reassembles continuation frames automatically.
type FrameReader struct {
	r            io.Reader
	maxFrameSize uint32
	// Continuation buffers keyed by (opcode, requestID).
	continuations map[contKey][]byte
}

type contKey struct {
	opcode    Opcode
	requestID uint32
}

// NewFrameReader creates a FrameReader.
func NewFrameReader(r io.Reader, maxFrameSize uint32) *FrameReader {
	if maxFrameSize == 0 {
		maxFrameSize = DefaultMaxFrameSize
	}
	return &FrameReader{
		r:             r,
		maxFrameSize:  maxFrameSize,
		continuations: make(map[contKey][]byte),
	}
}

// ReadFrame reads the next complete frame, reassembling continuations.
// Returns the header (with continuation flag cleared) and the complete body.
func (fr *FrameReader) ReadFrame() (FrameHeader, []byte, error) {
	for {
		frameBody, err := ReadFrameFrom(fr.r, fr.maxFrameSize)
		if err != nil {
			return FrameHeader{}, nil, err
		}

		if len(frameBody) < HeaderSize {
			return FrameHeader{}, nil, fmt.Errorf("frame body too short: %d bytes", len(frameBody))
		}

		r := NewReader(frameBody)
		hdr, err := DecodeHeader(r)
		if err != nil {
			return FrameHeader{}, nil, err
		}

		payload := frameBody[HeaderSize:]
		key := contKey{opcode: hdr.Opcode, requestID: hdr.RequestID}

		if hdr.IsContinuation() {
			// Buffer this chunk and keep reading.
			fr.continuations[key] = append(fr.continuations[key], payload...)
			continue
		}

		// Final frame (continuation=0).
		if prev, ok := fr.continuations[key]; ok {
			// Reassemble: previous chunks + this final chunk.
			delete(fr.continuations, key)
			complete := append(prev, payload...)
			hdr.Flags = 0
			return hdr, complete, nil
		}

		// No continuation — this is a standalone frame.
		return hdr, payload, nil
	}
}
