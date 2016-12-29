package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var dateKeyNameFuncs = map[string]func(*S3Output) string{
	"%Y":  func(o *S3Output) string { return time.Now().Format("2006") },
	"%m":  func(o *S3Output) string { return time.Now().Format("01") },
	"%d":  func(o *S3Output) string { return time.Now().Format("02") },
	"%H":  func(o *S3Output) string { return time.Now().Format("15") },
	"%M":  func(o *S3Output) string { return time.Now().Format("04") },
	"%S":  func(o *S3Output) string { return time.Now().Format("05") },
	"%NS": func(o *S3Output) string { return fmt.Sprint(time.Now().Nanosecond()) },
	"%r":  func(o *S3Output) string { return o.currentID },
}

type S3OutputConfig struct {
	flushInterval time.Duration
	sizeLimit     unitSizeVar
	queueLimit    int
	bucket        string
	region        string
}

type S3Output struct {
	mu             sync.Mutex
	keyTemplate    string
	currentName    string
	key            string
	uploadID       string
	queueLength    int
	chunkSize      int
	parts          []*s3.CompletedPart
	buffer         *bytes.Buffer
	writer         io.Writer
	requestPerFile bool
	currentID      string

	svc    *s3.S3
	config *S3OutputConfig
}

func NewS3Output(keyTemplate string, config *S3OutputConfig) *S3Output {
	o := new(S3Output)
	o.keyTemplate = keyTemplate
	o.config = config
	o.updateName()

	o.createClient()

	if strings.Contains(keyTemplate, "%r") {
		o.requestPerFile = true
	}

	go func() {
		for {
			time.Sleep(time.Second)
			o.updateName()
		}
	}()

	return o
}

func (o *S3Output) createClient() {
	sess, err := session.NewSession()
	if err != nil {
		log.Fatal(o, "Could not create Session. Error: %s", err)
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

	o.svc = s3.New(sess, &aws.Config{Region: &o.config.region})
}

func (o *S3Output) filename() string {
	defer o.mu.Unlock()
	o.mu.Lock()

	key := o.keyTemplate

	for name, fn := range dateKeyNameFuncs {
		key = strings.Replace(key, name, fn(o), -1)
	}

	nextChunk := false

	if o.currentName == "" ||
		((o.config.queueLimit > 0 && o.queueLength >= o.config.queueLimit) ||
			(o.config.sizeLimit > 0 && o.chunkSize >= int(o.config.sizeLimit))) {
		nextChunk = true
	}

	ext := filepath.Ext(key)
	withoutExt := strings.TrimSuffix(key, ext)

	if matches, err := filepath.Glob(withoutExt + "*" + ext); err == nil {
		if len(matches) == 0 {
			return setFileIndex(key, 0)
		}
		sort.Sort(sortByFileIndex(matches))

		last := matches[len(matches)-1]

		fileIndex := 0
		if idx := getFileIndex(last); idx != -1 {
			fileIndex = idx

			if nextChunk {
				fileIndex++
			}
		}

		return setFileIndex(last, fileIndex)
	}

	return key
}

func (o *S3Output) updateName() {
	o.currentName = filepath.Clean(o.filename())
}

func (o *S3Output) Write(data []byte) (n int, err error) {
	if o.requestPerFile {
		o.currentID = string(payloadMeta(data)[1])
		o.updateName()
	}

	if !isOriginPayload(data) {
		return len(data), nil
	}

	if o.uploadID == "" || o.currentName != o.key {
		o.mu.Lock()
		o.Close()
		o.key = o.currentName
		o.parts = []*s3.CompletedPart{}

		Debug("Creating multipartupload for ", o.config.bucket, o.key)
		params := &s3.CreateMultipartUploadInput{
			Bucket: aws.String(o.config.bucket),
			Key:    aws.String(o.key),
		}

		resp, err := o.svc.CreateMultipartUpload(params)
		if err != nil {
			log.Fatal(o, "Could not initiate upload to S3. Error: %s", err)
		}

		o.uploadID = *resp.UploadId

		o.buffer = bytes.NewBuffer(nil)

		if strings.HasSuffix(o.key, ".gz") {
			o.writer = gzip.NewWriter(o.buffer)
		} else {
			o.writer = bufio.NewWriter(o.buffer)
		}

		o.queueLength = 0
		o.mu.Unlock()
	}

	o.writer.Write(data)
	o.writer.Write([]byte(payloadSeparator))

	o.queueLength++

	return len(data), nil
}

func (o *S3Output) flush() {
	// Don't exit on panic
	defer func() {
		if r := recover(); r != nil {
			log.Println("PANIC while file flush: ", r, o, string(debug.Stack()))
		}
	}()

	defer o.mu.Unlock()
	o.mu.Lock()

	if o.buffer != nil {
		if strings.HasSuffix(o.key, ".gz") {
			o.writer.(*gzip.Writer).Flush()
		} else {
			o.writer.(*bufio.Writer).Flush()
		}
		err := o.uploadPart()
		if err != nil {
			panic(err)
		}
		o.chunkSize += o.buffer.Len()
		o.buffer.Reset()
	}
}

func (o *S3Output) String() string {
	return fmt.Sprintf("Key output: %s/%s", o.config.bucket, o.key)
}

func (o *S3Output) Close() error {
	if o.uploadID != "" {
		if strings.HasSuffix(o.key, ".gz") {
			o.writer.(*gzip.Writer).Close()
		} else {
			o.writer.(*bufio.Writer).Flush()
		}

		err := o.uploadPart()
		if err != nil {
			Debug("Failed to upload part!", err)
			return err
		}

		o.buffer.Reset()
		o.chunkSize = 0

		Debug("Completing multipartupload for ", o.key)
		params := &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(o.config.bucket),
			Key:      aws.String(o.key),
			UploadId: aws.String(o.uploadID),
			MultipartUpload: &s3.CompletedMultipartUpload{
				Parts: o.parts,
			},
		}

		_, err = o.svc.CompleteMultipartUpload(params)
		if err != nil {
			Debug("Failed to compelte upload!", err)
		}
		o.uploadID = ""
		o.parts = []*s3.CompletedPart{}
		return err
	}
	return nil
}

func (o *S3Output) uploadPart() error {

	Debug("Uploading part to S3")
	partNum := int64(len(o.parts) + 1)
	params := &s3.UploadPartInput{
		Bucket:     aws.String(o.config.bucket),
		Key:        aws.String(o.key),
		UploadId:   aws.String(o.uploadID),
		Body:       bytes.NewReader(o.buffer.Bytes()),
		PartNumber: aws.Int64(partNum),
	}
	resp, err := o.svc.UploadPart(params)
	if err != nil {
		return err
	}
	comp := &s3.CompletedPart{
		ETag:       resp.ETag,
		PartNumber: aws.Int64(partNum),
	}
	o.parts = append(o.parts, comp)
	return nil
}
