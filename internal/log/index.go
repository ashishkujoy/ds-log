package log

import (
	"github.com/tysonmote/gommap"
	"io"
	"os"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	endWidth        = posWidth + offWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	stat, err := os.Stat(f.Name())

	if err != nil {
		return nil, err
	}
	idx.size = uint64(stat.Size())
	if err := os.Truncate(f.Name(), c.Segment.MaxIndexBytes); err != nil {
		return nil, err
	}
	if idx.mmap, err = gommap.Map(idx.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED); err != nil {
		return nil, err
	}
	return idx, err
}

func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

func (i *index) Read(relativeOffset int64) (offset uint32, position uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if relativeOffset == -1 {
		offset = uint32((i.size / endWidth) - 1)
	} else {
		offset = uint32(relativeOffset)
	}
	position = uint64(offset) * endWidth

	if i.size < position+endWidth {
		return 0, 0, io.EOF
	}
	offset = enc.Uint32(i.mmap[position : position+offWidth])
	position = enc.Uint64(i.mmap[position+offWidth : position+endWidth])
	return offset, position, nil
}

func (i *index) Write(offset uint32, position uint64) error {
	if uint64(len(i.mmap)) < i.size+endWidth {
		return io.EOF
	}
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], offset)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+endWidth], position)
	i.size += endWidth
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}

type Config struct {
	Segment Segment
}

type Segment struct {
	MaxIndexBytes int64
}
