package broker

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"sync"
	"time"

	"streamq/proto"
)

// On-disk record layout:
//
//	[4 bytes body length][body][...]
//
// where body is:
//
//	[8 offset][8 timestamp unix-nano][4 key len][key][4 value len][value][4 crc32]
//
// The length prefix lets a recovering broker walk the file without trusting
// its contents, and the trailing CRC over the body detects a torn final write
// after a crash. Such a tail is truncated on open rather than failing startup.
const recordHeaderSize = 4

// minimum body size: offset + timestamp + two length fields + crc, no payload.
const minBodySize = 8 + 8 + 4 + 4 + 4

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// appendLog is the persistent, ordered backing store for a single topic. It is
// safe for concurrent appends and reads.
type appendLog struct {
	mu       sync.Mutex
	file     *os.File
	path     string
	index    []int64 // file position of each retained record
	base     uint64  // log offset of index[0]
	writePos int64
}

func openLog(path string) (*appendLog, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	l := &appendLog{file: f, path: path}
	if err := l.recover(); err != nil {
		f.Close()
		return nil, err
	}
	return l, nil
}

// recover rebuilds the in-memory index by scanning the file and drops any
// trailing partial or corrupt record left behind by an unclean shutdown.
func (l *appendLog) recover() error {
	info, err := l.file.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	header := make([]byte, recordHeaderSize)
	var pos int64
	for pos < size {
		if _, err := l.file.ReadAt(header, pos); err != nil {
			break
		}
		bodyLen := int64(binary.BigEndian.Uint32(header))
		if bodyLen < minBodySize || pos+recordHeaderSize+bodyLen > size {
			break
		}
		body := make([]byte, bodyLen)
		if _, err := l.file.ReadAt(body, pos+recordHeaderSize); err != nil {
			break
		}
		stored := binary.BigEndian.Uint32(body[bodyLen-4:])
		if crc32.Checksum(body[:bodyLen-4], crcTable) != stored {
			break
		}
		if len(l.index) == 0 {
			l.base = binary.BigEndian.Uint64(body)
		}
		l.index = append(l.index, pos)
		pos += recordHeaderSize + bodyLen
	}
	if pos < size {
		if err := l.file.Truncate(pos); err != nil {
			return err
		}
	}
	l.writePos = pos
	return nil
}

func encodeRecord(offset uint64, ts time.Time, key string, value []byte) []byte {
	bodyLen := 8 + 8 + 4 + len(key) + 4 + len(value) + 4
	buf := make([]byte, recordHeaderSize+bodyLen)
	binary.BigEndian.PutUint32(buf, uint32(bodyLen))
	body := buf[recordHeaderSize:]
	binary.BigEndian.PutUint64(body, offset)
	binary.BigEndian.PutUint64(body[8:], uint64(ts.UnixNano()))
	binary.BigEndian.PutUint32(body[16:], uint32(len(key)))
	n := copy(body[20:], key)
	binary.BigEndian.PutUint32(body[20+n:], uint32(len(value)))
	copy(body[24+n:], value)
	binary.BigEndian.PutUint32(body[bodyLen-4:], crc32.Checksum(body[:bodyLen-4], crcTable))
	return buf
}

func decodeRecord(rec []byte) proto.Message {
	body := rec[recordHeaderSize:]
	keyLen := binary.BigEndian.Uint32(body[16:])
	key := string(body[20 : 20+keyLen])
	valLen := binary.BigEndian.Uint32(body[20+keyLen:])
	valStart := 24 + keyLen
	value := make([]byte, valLen)
	copy(value, body[valStart:valStart+valLen])
	return proto.Message{
		Offset:    binary.BigEndian.Uint64(body),
		Key:       key,
		Value:     value,
		Timestamp: time.Unix(0, int64(binary.BigEndian.Uint64(body[8:]))),
	}
}

// append writes one record and returns its assigned offset. Writes land in the
// OS page cache and are made durable separately by sync, which keeps the hot
// path fast while still surviving process crashes.
func (l *appendLog) append(key string, value []byte, ts time.Time) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	offset := l.base + uint64(len(l.index))
	rec := encodeRecord(offset, ts, key, value)
	if _, err := l.file.WriteAt(rec, l.writePos); err != nil {
		return 0, err
	}
	l.index = append(l.index, l.writePos)
	l.writePos += int64(len(rec))
	return offset, nil
}

func (l *appendLog) readRaw(pos int64) ([]byte, error) {
	header := make([]byte, recordHeaderSize)
	if _, err := l.file.ReadAt(header, pos); err != nil {
		return nil, err
	}
	bodyLen := int64(binary.BigEndian.Uint32(header))
	rec := make([]byte, recordHeaderSize+bodyLen)
	if _, err := l.file.ReadAt(rec, pos); err != nil {
		return nil, err
	}
	return rec, nil
}

// readFrom returns up to max messages beginning at offset. Offsets below the
// oldest retained record are clamped forward so a replay survives compaction.
func (l *appendLog) readFrom(offset uint64, max int) []proto.Message {
	l.mu.Lock()
	defer l.mu.Unlock()
	if offset < l.base {
		offset = l.base
	}
	next := l.base + uint64(len(l.index))
	if offset >= next || max <= 0 {
		return nil
	}
	start := int(offset - l.base)
	end := start + max
	if end > len(l.index) {
		end = len(l.index)
	}
	msgs := make([]proto.Message, 0, end-start)
	for i := start; i < end; i++ {
		rec, err := l.readRaw(l.index[i])
		if err != nil {
			break
		}
		msgs = append(msgs, decodeRecord(rec))
	}
	return msgs
}

// bounds reports the oldest retained offset, the next offset to be written and
// the number of retained records.
func (l *appendLog) bounds() (oldest, next, count uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := uint64(len(l.index))
	return l.base, l.base + n, n
}

func (l *appendLog) sizeBytes() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.writePos
}

// firstOffsetAfter returns the offset of the oldest record whose timestamp is
// not before cutoff, or the next offset if every record predates it.
func (l *appendLog) firstOffsetAfter(cutoff time.Time) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, pos := range l.index {
		rec, err := l.readRaw(pos)
		if err != nil {
			break
		}
		if !decodeRecord(rec).Timestamp.Before(cutoff) {
			return l.base + uint64(i)
		}
	}
	return l.base + uint64(len(l.index))
}

// offsetForMaxSize returns the lowest offset that, if kept as the new oldest
// record, leaves the log no larger than maxSize bytes.
func (l *appendLog) offsetForMaxSize(maxSize int64) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, pos := range l.index {
		if l.writePos-pos <= maxSize {
			return l.base + uint64(i)
		}
	}
	return l.base + uint64(len(l.index))
}

// truncate rewrites the log keeping only records at or after retainFrom. It is
// the physical side of retention: messages below retainFrom become unreadable
// and the oldest offset advances.
func (l *appendLog) truncate(retainFrom uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	next := l.base + uint64(len(l.index))
	if retainFrom <= l.base {
		return nil
	}
	if retainFrom > next {
		retainFrom = next
	}
	tmpPath := l.path + ".compact"
	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	newIndex := make([]int64, 0, next-retainFrom)
	var writePos int64
	for i := int(retainFrom - l.base); i < len(l.index); i++ {
		rec, err := l.readRaw(l.index[i])
		if err != nil {
			tmp.Close()
			return err
		}
		if _, err := tmp.WriteAt(rec, writePos); err != nil {
			tmp.Close()
			return err
		}
		newIndex = append(newIndex, writePos)
		writePos += int64(len(rec))
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	tmp.Close()
	if err := l.file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, l.path); err != nil {
		return err
	}
	f, err := os.OpenFile(l.path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	l.file = f
	l.index = newIndex
	l.base = retainFrom
	l.writePos = writePos
	return nil
}

func (l *appendLog) sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Sync()
}

func (l *appendLog) close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.file.Sync(); err != nil {
		l.file.Close()
		return err
	}
	return l.file.Close()
}
