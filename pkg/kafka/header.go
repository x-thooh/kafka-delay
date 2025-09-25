package kafka

import "github.com/confluentinc/confluent-kafka-go/kafka"

type Header struct {
	Header kafka.Header
	Index  int
}

func GetHeaderValue(headers []kafka.Header, keys ...string) (ret map[string]*Header) {
	ret = make(map[string]*Header)
	mHeader := make(map[string]*Header)
	for idx, header := range headers {
		mHeader[header.Key] = &Header{
			Index:  idx,
			Header: header,
		}
	}

	for _, key := range keys {
		item, exist := mHeader[key]
		if !exist {
			item = &Header{
				Index: -1,
			}
		}
		ret[key] = item
	}
	return
}
