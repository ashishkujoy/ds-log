package log

import (
	"github.com/stretchr/testify/require"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func Test_New_Index(t *testing.T) {
	tempFile, err := ioutil.TempFile(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	c := Config{Segment: Segment{MaxIndexBytes: 1024}}
	index, err := newIndex(tempFile, c)
	defer index.Close()
	require.NoError(t, err)

	_, _, err = index.Read(-1)
	require.Error(t, err)
	require.Equal(t, index.size, uint64(0))
	require.Equal(t, index.Name(), tempFile.Name())
}

func Test_Index_Read(t *testing.T) {
	tempFile, err := ioutil.TempFile(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	c := Config{Segment: Segment{MaxIndexBytes: 1024}}
	index, _ := newIndex(tempFile, c)
	defer index.Close()
	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{
			Off: 0,
			Pos: 0,
		},
		{
			Off: 1,
			Pos: 10,
		},
	}

	for _, entry := range entries {
		err := index.Write(entry.Off, entry.Pos)
		require.NoError(t, err)

		_, position, err := index.Read(int64(entry.Off))
		require.NoError(t, err)
		require.Equal(t, position, entry.Pos)
	}

}

func Test_Invalid_Index_Read(t *testing.T) {
	tempFile, err := ioutil.TempFile(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	c := Config{Segment: Segment{MaxIndexBytes: 1024}}
	index, _ := newIndex(tempFile, c)
	defer index.Close()

	_, _, err = index.Read(-1)
	require.Equal(t, err, io.EOF)
}

func Test_Index_Restore(t *testing.T) {
	tempFile, err := ioutil.TempFile(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	c := Config{Segment: Segment{MaxIndexBytes: 1024}}
	index, _ := newIndex(tempFile, c)

	_ = index.Write(0, 1)
	_ = index.Write(1, 10)

	err = index.Close()
	require.NoError(t, err)
	f, err := os.OpenFile(tempFile.Name(), os.O_RDWR, 0600)
	require.NoError(t, err)

	index2, err := newIndex(f, c)
	require.NoError(t, err)
	defer index2.Close()
	offset, position, err := index2.Read(-1)
	require.NoError(t, err)
	require.Equal(t, offset, uint32(1))
	require.Equal(t, position, uint64(10))
}
