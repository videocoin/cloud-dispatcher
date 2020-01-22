package hls

import (
	"bytes"
	"io/ioutil"
	"net/http"

	"github.com/grafov/m3u8"
)

func ParseHLSFromURL(url string) (m3u8.Playlist, m3u8.ListType, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}

	buf := bytes.NewBuffer(bodyBytes)
	pl, plType, err := m3u8.Decode(*buf, true)
	if err != nil {
		return nil, 0, err
	}

	return pl, plType, nil
}
