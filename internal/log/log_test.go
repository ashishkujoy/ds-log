package log

import (
	log_v1 "ashishkujoy/ds-log/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"io/ioutil"
	"os"
	"testing"
)

func Test_Log(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, l *Log){
		"append and read a record succeed": testAppendRead,
		"Offset out of range error":        testOutOfRangeErr,
		"init with existing segments":      testInitExisting,
		"reader for given log":             testReader,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "store-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)
			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			require.NoError(t, err)
			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, l *Log) {
	record := &log_v1.Record{Value: []byte("Hello world")}
	offset, err := l.Append(record)
	require.NoError(t, err)
	require.Equal(t, offset, uint64(0))
}

func testOutOfRangeErr(t *testing.T, l *Log) {
	_, err := l.Read(0)
	require.Error(t, err)
}

func testReader(t *testing.T, l *Log) {
	record := &log_v1.Record{Value: []byte("Hello world")}
	offset, err := l.Append(record)
	require.NoError(t, err)
	require.Equal(t, offset, uint64(0))

	reader := l.Reader()
	b, err := ioutil.ReadAll(reader)
	require.NoError(t, err)

	r := &log_v1.Record{}
	err = proto.Unmarshal(b[lenWidth:], r)
	require.NoError(t, err)
	require.Equal(t, record.Value, r.Value)
}

func testInitExisting(t *testing.T, l *Log) {
	record := &log_v1.Record{Value: []byte("Hello world")}
	for i := 0; i < 3; i++ {
		_, err := l.Append(record)
		require.NoError(t, err)
	}
	require.NoError(t, l.Close())
	lowestOffset, err := l.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), lowestOffset)

	highestOffset, err := l.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), highestOffset)

	newLog, err := NewLog(l.Dir, l.Config)
	require.NoError(t, err)

	newLowestOffset, err := newLog.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, newLowestOffset, lowestOffset)

	newHighestOffset, err := newLog.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, newHighestOffset, highestOffset)
}
