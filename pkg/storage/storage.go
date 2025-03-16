package storage

import (
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"gocloud.dev/gcloudv4/google/cloud/storage"
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

func NewS3Storage(bucket, region, accessKey, secretKey string) Storage {
	cfg := &aws.Config{
		Region:          region,
		Credentials:     credentials.NewStaticCredentials(accessKey, secretKey, ""),
		HTTPClient:      nil,
		Logger:          nil,
		Mutex:           nil,
		TokenSource:     nil,
		APIVersion:      "latest",
		SignedURLEnabled: true,
	}

	s3Client := s3.New(cfg)

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
	input := s3.PutObjectInput{
		Bucket: &s.bucket,
		Key:    filename,
		Body:   bytes.NewReader(data),
	}
	_, err := s.client.PutObject(&input)
	return err
}

func (s *s3Storage) Download(filename string) ([]byte, error) {
	output, err := s.client.GetObject(&s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    filename,
	})
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

func NewGoogleDriveStorage(serviceAccountJSON string, folderID string) Storage {
	ctx := context.Background()

	client, err := gcloudstorage.NewClient(ctx, option.WithCredentialsFile(serviceAccountJSON))
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %v", err)
	}

	return &googleDriveStorage{
		client:    client,
		folderID: folderID,
	}, nil
}

type googleDriveStorage struct {
	client *storage.Client
	folderID string
}

func (g *googleDriveStorage) Upload(data []byte, filename string) error {
	req := storage.InsertRequest{
		Bucket:     g.folderID,
		Name:      filename,
		Data:      data,
		MimeType:  "text/plain",
		Overwrite: true,
	}

	_, err := g.client.Upload(req)
	return err
}

func (g *googleDriveStorage) Download(filename string) ([]byte, error) {
	rc, err := g.client.Download(g.folderID, filename, nil)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	data, err := io.ReadAll(rc.Body)
	if err != nil {
		return nil, err
	}

	return data, nil
}
