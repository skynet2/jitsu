package airbyte

import (
	"os"
	"regexp"
	"strconv"
)

var recordCountDivider = 100000

func init() {
	val, _ := strconv.Atoi(os.Getenv("RECORD_COUNT_DIVIDER"))
	if val > 0 {
		recordCountDivider = val
	}
}

var rg = regexp.MustCompile(`Records read: ([0-9]+)`)

func getLevelForRecordsRead(row *Row) string {
	if row == nil || row.Log == nil {
		return "DEBUG"
	}

	rs := rg.FindStringSubmatch(row.Log.Message)
	if len(rs) != 2 {
		return row.Log.Level
	}

	parsed, _ := strconv.Atoi(rs[1])
	if parsed == 0 {
		return row.Log.Level
	}

	if parsed%recordCountDivider == 0 {
		return "INFO"
	}

	return row.Log.Level
}
