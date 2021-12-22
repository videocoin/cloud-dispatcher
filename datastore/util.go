package datastore

import (
	"strconv"
	"strings"
)

func extractNumFromSegmentName(name string) int64 {
	s := strings.TrimPrefix(name, "index")
	s = strings.TrimSuffix(s, ".ts")
	s = strings.TrimSuffix(s, ".mkv")
	s = strings.TrimSuffix(s, ".mp4")
	num, _ := strconv.ParseInt(s, 10, 64)
	return num
}
