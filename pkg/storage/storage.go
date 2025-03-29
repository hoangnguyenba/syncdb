package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Storage interface {
	Upload([]byte, string) error
	Download(string) ([]byte, error)
}

type localStorage struct {
	path string
}

func (l *localStorage) Upload(data []byte, filename string) error {
	return os.WriteFile(filename, data, 0644)
}

func (l *localStorage) Download(filename string) ([]byte, error) {
	return os.ReadFile(filename)
}

func NewLocalStorage(path string) Storage {
	return &localStorage{path: path}
}

func NewS3Storage(bucket, region string) Storage {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
	)
	if err != nil {
		fmt.Printf("Error loading AWS config: %v\n", err)
		fmt.Println("Please ensure AWS credentials are set via environment variables:")
		fmt.Println("  export AWS_ACCESS_KEY_ID=<your-access-key>")
		fmt.Println("  export AWS_SECRET_ACCESS_KEY=<your-secret-key>")
		fmt.Println("  export AWS_SESSION_TOKEN=<your-session-token> # if using temporary credentials")
		return nil
	}

	s3Client := s3.NewFromConfig(cfg)

	return &s3Storage{
		client: s3Client,
		bucket: bucket,
	}
}

type s3Storage struct {
	client *s3.Client
	bucket string
}

func (s *s3Storage) Upload(data []byte, filename string) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(filename),
		Body:   bytes.NewReader(data),
	}
	_, err := s.client.PutObject(context.Background(), input)
	return err
}

func (s *s3Storage) Download(filename string) ([]byte, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(filename),
	}
	output, err := s.client.GetObject(context.Background(), input)
	if err != nil {
		return nil, err
	}
	defer output.Body.Close()

	data, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, err
	}

	return data, nil
}
