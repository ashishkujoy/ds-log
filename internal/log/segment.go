package log

import (
	log_v1 "ashishkujoy/ds-log/api/v1"
	"fmt"
	"google.golang.org/protobuf/proto"
	"os"
	"path"
)

type segment struct {
	store                  *store
	index                  *index
	baseOffSet, nextOffSet uint64
	config                 Config
}

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	segment := &segment{
		config:     c,
		baseOffSet: baseOffset,
	}
	var err error
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if segment.store, err = newStore(storeFile); err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if segment.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}
	if off, _, err := segment.index.Read(-1); err != nil {
		segment.nextOffSet = baseOffset
	} else {
		segment.nextOffSet = baseOffset + uint64(off) + 1
	}
	return segment, nil
}

func (s *segment) Append(record *log_v1.Record) (offset uint64, err error) {
	cur := s.nextOffSet
	record.Offset = cur
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}
	if err = s.index.Write(
		uint32(s.nextOffSet-s.baseOffSet),
		pos,
	); err != nil {
		return 0, err
	}
	s.nextOffSet++
	return cur, nil
}

func (s *segment) Read(offset uint64) (*log_v1.Record, error) {
	_, position, err := s.index.Read(int64(offset - s.baseOffSet))
	if err != nil {
		return nil, err
	}
	bytes, err := s.store.Read(position)
	if err != nil {
		return nil, err
	}
	var r log_v1.Record
	err = proto.Unmarshal(bytes, &r)

	return &r, err
}

func (s *segment) IsMaxed() bool {
	return s.index.size >= s.config.Segment.MaxIndexBytes ||
		s.store.size >= s.config.Segment.MaxStoreBytes
}

func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

func (s *segment) Close() error {
	if err := s.store.Close(); err != nil {
		return err
	}
	if err := s.index.Close(); err != nil {
		return err
	}
	return nil
}

func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}
