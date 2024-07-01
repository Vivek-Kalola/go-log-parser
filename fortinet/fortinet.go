package fortinet

import (
	"log-processor/elastic"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

var (
	compile, _ = regexp.Compile("(\\w+)=(\"[^\"]*\"|\\S+)")

	mapper = map[string]string{
		"srcport":     "srcport",
		"subtype":     "subtype",
		"dstip":       "dstip",
		"dstintf":     "dstintf",
		"service":     "service",
		"timestamp":   "timestamp",
		"devid":       "devid",
		"srcintf":     "srcintf",
		"devname":     "devname",
		"proto":       "proto",
		"action":      "action",
		"dstport":     "dstport",
		"srcip":       "srcip",
		"sentpkt":     "sentpkt",
		"rcvdbyte":    "rcvdbyte",
		"rcvdpkt":     "rcvdpkt",
		"tz":          "tz",
		"date":        "date",
		"policyid":    "policyid",
		"dstmac":      "dstmac",
		"type":        "type",
		"dstintfrole": "dstintfrole",
		"srcintfrole": "srcintfrole",
		"time":        "time",
		"logid":       "logid",
		"sessionid":   "sessionid",
		"sentbyte":    "sentbyte",
		"sentdelta":   "sentdelta",
		"rcvddelta":   "rcvddelta",
	}
)

func parse(message string) map[string]interface{} {

	result := make(map[string]interface{})

	matches := compile.FindAllStringSubmatch(message, -1)

	if matches != nil {

		for _, group := range matches {

			// group[0] is the entire match (e.g., `key=value`)
			// group[1] is the key
			// group[2] is the value, which might include quotes

			key := group[1]
			value := strings.Trim(group[2], "\"")

			if mapped, ok := mapper[key]; ok {
				if numericValue, err := strconv.Atoi(value); err == nil {
					// It's an integer
					result[mapped] = numericValue
				} else if numericValue, err := strconv.ParseFloat(value, 64); err == nil {
					// It's a float
					result[mapped] = numericValue
				} else {
					// It's a string
					result[mapped] = value
				}
			}
		}

		if v, ok := result["rcvddelta"]; ok {
			result["rcvd_bytes"] = v
		} else if v, ok := result["rcvdbyte"]; ok {
			result["rcvd_bytes"] = v
		} else {
			result["rcvd_bytes"] = 0
		}

		if v, ok := result["sentdelta"]; ok {
			result["sent_bytes"] = v
		} else if v, ok := result["sentbyte"]; ok {
			result["sent_bytes"] = v
		} else {
			result["sent_bytes"] = 0
		}

	}
	return result
}

func Worker(inputChan <-chan string, wg *sync.WaitGroup, elastic *elastic.Connection) {
	defer wg.Done()
	for logMessage := range inputChan {
		document := parse(logMessage)
		_ = elastic.Send(document)
	}
}
