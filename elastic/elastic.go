package elastic

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"go-log-parser/constants"
	"log"
	"net"
	"net/http"
	"os"
	"time"
)

type Connection struct {
	client *elasticsearch.Client
	index  string
	eps    uint64
	total  uint64
}

func NewConnection(hosts []string, index string, elasticCreateIndex string, user string, password string) (*Connection, error) {

	// Get the SystemCertPool, continue with an empty pool on error
	rootCAs, _ := x509.SystemCertPool()
	if rootCAs == nil {
		rootCAs = x509.NewCertPool()
	}

	// Read in the cert file
	localCertFile, err := os.ReadFile(constants.File(constants.CWD, "cert.pem"))
	if err != nil {
		log.Fatalf("Failed to append %q to RootCAs: %v", localCertFile, err)
	}

	config := elasticsearch.Config{
		Addresses: hosts,
		Username:  user,
		Password:  password,
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: time.Second,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
				RootCAs:            rootCAs,
			},
		}}

	client, err := elasticsearch.NewClient(config)

	if err != nil {
		return nil, err
	}

	conn := &Connection{
		client: client,
		index:  index,
		eps:    0,
		total:  0,
	}

	go func() {
		for {
			time.Sleep(10 * time.Second)
			fmt.Println(time.Now(), "Elastic publishing Logs/Sec = ", conn.eps/10, "Total Logs:", conn.total)
			conn.eps = 0

			if conn.total+1000 >= constants.MaxUint64 {
				conn.total = 0
			}
		}
	}()

	if elasticCreateIndex == "yes" {
		_, err = conn.client.Indices.Create(index)

		if err != nil {
			fmt.Println(err)
		}
	}

	return conn, nil
}

func (conn *Connection) Send(document map[string]interface{}) error {

	data, _ := json.Marshal(document)

	_, err := conn.client.Index(conn.index, bytes.NewReader(data))

	conn.total++
	conn.eps++
	return err
}
