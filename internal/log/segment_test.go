package log

import (
	log_v1 "ashishkujoy/ds-log/api/v1"
	"github.com/stretchr/testify/require"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func Test_Segment(t *testing.T) {
	dir, err := ioutil.TempDir("", "s-test")
	require.NoError(t, err)
	defer os.Remove(dir)
	record := &log_v1.Record{Value: []byte("Hello world")}
	c := Config{Segment: Segment{
		MaxIndexBytes: endWidth * 3,
		MaxStoreBytes: 1024,
		InitialOffset: 0,
	}}
	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)

	require.Equal(t, uint64(16), s.baseOffSet)
	require.Equal(t, false, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(record)
		require.NoError(t, err)
		require.Equal(t, off, i+16)
	}
	_, err = s.Append(record)
	require.Equal(t, err, io.EOF)
	require.Equal(t, true, s.IsMaxed())

	err = s.Close()
	require.NoError(t, err)

	c.Segment.MaxStoreBytes = uint64(len(record.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.True(t, s.IsMaxed())
}
