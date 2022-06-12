package log

import (
	log_v1 "ashishkujoy/ds-log/api/v1"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

func Test_Log(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, l *Log){
		"append and read a record succeed": testAppendRead,
		"offset out of range error":        testOutOfRangeErr,
		"init with existing segments":      testInitExisting,
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

}

func testInitExisting(t *testing.T, l *Log) {

}
