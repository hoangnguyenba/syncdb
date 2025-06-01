package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
)

type Storage interface {
	Upload([]byte, string) error
	Download(string) ([]byte, error)
	ListObjects(prefix string) ([]string, error)
	GetLatestZipFile() (string, error)
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

func (l *localStorage) ListObjects(prefix string) ([]string, error) {
	var files []string
	entries, err := os.ReadDir(l.path)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			files = append(files, entry.Name())
		}
	}
	return files, nil
}

func (l *localStorage) GetLatestZipFile() (string, error) {
	files, err := l.ListObjects("")
	if err != nil {
		return "", err
	}

	var latestZip string
	for _, file := range files {
		if len(file) > 4 && file[len(file)-4:] == ".zip" {
			if latestZip == "" || file > latestZip {
				latestZip = file
			}
		}
	}

	if latestZip == "" {
		return "", fmt.Errorf("no zip files found")
	}

	return latestZip, nil
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

func (s *s3Storage) ListObjects(prefix string) ([]string, error) {
	var files []string

	// Ensure prefix ends with / if not empty
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		output, err := paginator.NextPage(context.Background())
		if err != nil {
			return nil, err
		}

		for _, obj := range output.Contents {
			if obj.Key != nil {
				files = append(files, *obj.Key)
			}
		}
	}

	return files, nil
}

func (s *s3Storage) GetLatestZipFile() (string, error) {
	// List all zip files in the bucket
	files, err := s.ListObjects("")
	if err != nil {
		return "", err
	}

	var latestZip string
	for _, file := range files {
		if strings.HasSuffix(file, ".zip") {
			if latestZip == "" || file > latestZip {
				latestZip = file
			}
		}
	}

	if latestZip == "" {
		return "", fmt.Errorf("no zip files found in S3 bucket %s", s.bucket)
	}

	return latestZip, nil
}

type gdriveStorage struct {
	service    *drive.Service
	folderId   string
	fileFields string
}

func NewGoogleDriveStorage(credentialsFile string, folderId string) (Storage, error) {
	ctx := context.Background()
	srv, err := drive.NewService(ctx, option.WithCredentialsFile(credentialsFile))
	if err != nil {
		fmt.Printf("Error creating Google Drive service: %v\n", err)
		fmt.Println("Please ensure:")
		fmt.Println("  1. A valid service account credentials file is provided")
		fmt.Println("  2. The service account has appropriate permissions")
		fmt.Println("  3. The Google Drive API is enabled in your project")
		return nil, err
	}

	return &gdriveStorage{
		service:    srv,
		folderId:   folderId,
		fileFields: "id, name, createdTime",
	}, nil
}

func (g *gdriveStorage) Upload(data []byte, filename string) error {
	f := &drive.File{
		Name:    filename,
		Parents: []string{g.folderId},
	}

	fmt.Printf("Starting upload of %s to Google Drive folder %s...\n", filename, g.folderId)
	reader := bytes.NewReader(data)
	file, err := g.service.Files.Create(f).Media(reader).Do()
	if err != nil {
		fmt.Printf("Failed to upload %s to Google Drive: %v\n", filename, err)
		return err
	}
	fmt.Printf("Successfully uploaded %s to Google Drive (File ID: %s)\n", filename, file.Id)
	return nil
}

func (g *gdriveStorage) Download(filename string) ([]byte, error) {
	// Search for the file by name in the specified folder
	q := fmt.Sprintf("name = '%s' and '%s' in parents and trashed = false",
		filename, g.folderId)
	files, err := g.service.Files.List().Q(q).Fields("files(id)").Do()
	if err != nil {
		return nil, err
	}

	if len(files.Files) == 0 {
		return nil, fmt.Errorf("file %s not found in Google Drive folder", filename)
	}

	// Get the file content
	resp, err := g.service.Files.Get(files.Files[0].Id).Download()
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

func (g *gdriveStorage) ListObjects(prefix string) ([]string, error) {
	var files []string
	var query string

	if prefix != "" {
		query = fmt.Sprintf("name contains '%s' and '%s' in parents and trashed = false",
			prefix, g.folderId)
	} else {
		query = fmt.Sprintf("'%s' in parents and trashed = false", g.folderId)
	}

	pageToken := ""
	for {
		fileList, err := g.service.Files.List().
			Q(query).
			Fields("nextPageToken, files(name)").
			PageToken(pageToken).
			Do()
		if err != nil {
			return nil, err
		}

		for _, file := range fileList.Files {
			files = append(files, file.Name)
		}

		pageToken = fileList.NextPageToken
		if pageToken == "" {
			break
		}
	}

	return files, nil
}

func (g *gdriveStorage) GetLatestZipFile() (string, error) {
	query := fmt.Sprintf("mimeType = 'application/zip' and '%s' in parents and trashed = false",
		g.folderId)

	fileList, err := g.service.Files.List().
		Q(query).
		OrderBy("createdTime desc").
		Fields("files(name)").
		PageSize(1).
		Do()
	if err != nil {
		return "", err
	}

	if len(fileList.Files) == 0 {
		return "", fmt.Errorf("no zip files found in Google Drive folder %s", g.folderId)
	}

	return fileList.Files[0].Name, nil
}
