package airbyte

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseGroup(t *testing.T) {
	testsCases := []struct {
		input         *Row
		expectedLevel string
	}{
		{
			input: &Row{
				Log: &LogRow{
					Level: "DEBUG",
					Message: "INFO i.a.i.s.r.AbstractDbSource(lambda$createReadIterator$8):470 " +
						"Reading stream table_name.  Reading stream table_name. Records read: 300000",
				},
			},
			expectedLevel: "INFO",
		},
		{
			input: &Row{
				Log: &LogRow{
					Level: "WARN",
					Message: "INFO i.a.i.s.r.AbstractDbSource(lambda$createReadIterator$8):470 " +
						"Reading stream table_name.  Reading stream table_name. Records read: 310000",
				},
			},
			expectedLevel: "WARN",
		},
		{
			input: &Row{
				Log: &LogRow{
					Level:   "ERROR",
					Message: "Stream [migrations] Clearing table",
				},
			},
			expectedLevel: "ERROR",
		},
	}

	for i, testCase := range testsCases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			assert.Equal(t, testCase.expectedLevel, getLevelForRecordsRead(testCase.input))
		})
	}
}
