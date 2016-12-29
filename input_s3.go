package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gobwas/glob"
)

type S3InputConfig struct {
	bucket string
	region string
}

type byteInputReader struct {
	reader    *bufio.Reader
	data      []byte
	buffer    io.ReadCloser
	timestamp int64
}

func (f *byteInputReader) parseNext() error {
	payloadSeparatorAsBytes := []byte(payloadSeparator)
	var buffer bytes.Buffer

	for {
		line, err := f.reader.ReadBytes('\n')

		if err != nil {
			if err != io.EOF {
				log.Println(err)
				return err
			}

			if err == io.EOF {
				f.buffer.Close()
				f.buffer = nil
				return err
			}
		}

		if bytes.Equal(payloadSeparatorAsBytes[1:], line) {
			asBytes := buffer.Bytes()
			meta := payloadMeta(asBytes)

			f.timestamp, _ = strconv.ParseInt(string(meta[2]), 10, 64)
			f.data = asBytes[:len(asBytes)-1]

			return nil
		}

		buffer.Write(line)
	}

	//return nil
}

func (f *byteInputReader) ReadPayload() []byte {
	defer f.parseNext()

	return f.data
}
func (f *byteInputReader) Close() error {
	if f.buffer != nil {
		f.buffer.Close()
	}

	return nil
}

func NewByteInputReader(key string, buf io.ReadCloser) *byteInputReader {
	r := &byteInputReader{buffer: buf}
	if strings.HasSuffix(key, ".gz") {
		gzReader, err := gzip.NewReader(buf)
		if err != nil {
			log.Println(err)
			return nil
		}
		r.reader = bufio.NewReader(gzReader)
	} else {
		r.reader = bufio.NewReader(buf)
	}

	r.parseNext()

	return r
}

// S3Input can read requests generated by S3Output
type S3Input struct {
	mu          sync.Mutex
	data        chan []byte
	exit        chan bool
	key         string
	readers     []*byteInputReader
	speedFactor float64
	loop        bool
	bucket      string
	region      string
	svc         *s3.S3
	config      *S3InputConfig
}

// NewFileInput constructor for FileInput. Accepts file path as argument.
func NewS3Input(key string, config *S3InputConfig) (i *S3Input) {
	i = new(S3Input)
	i.data = make(chan []byte, 1000)
	i.exit = make(chan bool, 1)
	i.key = key
	i.speedFactor = 1
	i.config = config
	i.createClient()

	if err := i.init(); err != nil {
		return
	}

	go i.emit()

	return
}

func (i *S3Input) createClient() {
	sess, err := session.NewSession()
	if err != nil {
		panic(err)
	}

	e := ec2metadata.New(sess)

	creds := credentials.NewChainCredentials(
		[]credentials.Provider{
			&ec2rolecreds.EC2RoleProvider{
				Client:       e,
				ExpiryWindow: 5 * time.Minute,
			},
			&credentials.SharedCredentialsProvider{},
		})

	sess.Config.Credentials = creds

	svc := s3.New(sess, &aws.Config{Region: &i.config.region})
	i.svc = svc
}

type NextKeyNotFound struct{}

func (_ *NextKeyNotFound) Error() string {
	return "There is no new keys"
}

func (i *S3Input) listKeys() ([]string, error) {
	params := &s3.ListObjectsInput{
		Bucket: aws.String(i.config.bucket),
	}

	resp, err := i.svc.ListObjects(params)
	if err != nil {
		return []string{}, err
	}
	keys := []string{}
	for _, key := range resp.Contents {
		keys = append(keys, *key.Key)
	}

	return keys, nil
}

func (i *S3Input) getKey(key string) (io.ReadCloser, error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(i.config.bucket),
		Key:    aws.String(key),
	}

	resp, err := i.svc.GetObject(params)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (i *S3Input) init() (err error) {
	defer i.mu.Unlock()
	i.mu.Lock()

	var matches []string
	keys, err := i.listKeys()
	if err != nil {
		return err
	}

	g := glob.MustCompile(i.key)
	for _, key := range keys {
		if g.Match(key) {
			matches = append(matches, key)
		}
	}

	if len(matches) == 0 {
		log.Println("No keys match pattern: ", i.key)
		return errors.New("No matching keys")
	}

	i.readers = make([]*byteInputReader, len(matches))

	for idx, p := range matches {
		body, err := i.getKey(p)
		if err != nil {
			return err
		}
		i.readers[idx] = NewByteInputReader(p, body)
	}

	return nil
}

func (i *S3Input) Read(data []byte) (int, error) {
	buf := <-i.data
	copy(data, buf)

	return len(buf), nil
}

func (i *S3Input) String() string {
	return "Key input: " + i.key
}

// Find reader with smallest timestamp e.g next payload in row
func (i *S3Input) nextReader() (next *byteInputReader) {
	for _, r := range i.readers {
		if r == nil || r.buffer == nil {
			continue
		}

		if next == nil || r.timestamp < next.timestamp {
			next = r
			continue
		}
	}

	return
}

func (i *S3Input) emit() {
	var lastTime int64 = -1

	for {
		select {
		case <-i.exit:
			return
		default:
		}

		reader := i.nextReader()

		if reader == nil {
			if i.loop {
				i.init()
				lastTime = -1
				continue
			} else {
				break
			}
		}

		if lastTime != -1 {
			diff := reader.timestamp - lastTime
			lastTime = reader.timestamp

			if i.speedFactor != 1 {
				diff = int64(float64(diff) / i.speedFactor)
			}

			time.Sleep(time.Duration(diff))
		} else {
			lastTime = reader.timestamp
		}

		i.data <- reader.ReadPayload()
	}

	log.Printf("S3Input: end of file '%s'\n", i.key)
}

func (i *S3Input) Close() error {
	defer i.mu.Unlock()
	i.mu.Lock()

	i.exit <- true

	for _, r := range i.readers {
		r.Close()
	}

	return nil
}