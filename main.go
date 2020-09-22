package main

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/schollz/progressbar/v3"
)

type PayLoad struct {
	Region     string
	Bucket     string
	Prefix     string
	Files      []string
	OutPutFile string
}

// FakeWriter - Use for streaming download
type FakeWriterAt struct {
	w io.Writer
}

func (fw FakeWriterAt) WriteAt(p []byte, offset int64) (n int, err error) {
	// ignore 'offset' because we forced sequential downloads
	return fw.w.Write(p)
}

// Write error log then exit
func exitErrorf(msg string, args ...interface{}) {
	fmt.Printf(msg, args)
	os.Exit(1)
}

// List of files in a bucket
func fetchFiles(sess *session.Session, payload *PayLoad) {
	// Create S3 service client
	svc := s3.New(sess)

	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(payload.Bucket), Prefix: aws.String(payload.Prefix)})
	if err != nil {
		exitErrorf("Unable to list items in bucket %q, %v", payload.Bucket, err)
	}

	for _, s := range resp.Contents {
		payload.Files = append(payload.Files, *s.Key)
	}
}

func zipS3Files(payload PayLoad, sess *session.Session) error {

	uploader := s3manager.NewUploader(sess)
	downloader := s3manager.NewDownloader(sess)
	s3Service := s3.New(sess)

	//declare read/write pipe
	pr, pw := io.Pipe()

	result := &s3manager.UploadInput{
		Bucket: aws.String(payload.Bucket),
		Key:    aws.String(payload.OutPutFile),
	}

	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(payload.Bucket),
		Key:    aws.String(""),
	}

	zipWriter := zip.NewWriter(pw)
	wg := sync.WaitGroup{}
	// Wait for downloader and uploader
	wg.Add(2)

	totalFiles := len(payload.Files)
	var count = 0
	bar := progressbar.Default(int64(totalFiles))
	go func() {
		defer func() {
			wg.Done()
			zipWriter.Close()
			pw.Close()
		}()

		for _, file := range payload.Files {
			if strings.TrimSpace(file) == "" {
				continue
			}
			headInput.Key = aws.String(file)
			_, hErr := s3Service.HeadObject(headInput)
			if hErr != nil {
				fmt.Printf("Zip file not found: '%s'\n", file)
				continue
			}

			//Download file one by one to fakewriter
			w, err := zipWriter.Create(file)
			if err != nil {
				fmt.Println(err)
			}

			_, err = downloader.Download(FakeWriterAt{w}, &s3.GetObjectInput{
				Bucket: aws.String(payload.Bucket),
				Key:    aws.String(file),
			})
			if err != nil {
				fmt.Println(err)
			} else {
				count = count + 1
			}
			bar.Add(1)
		}
	}()

	go func() {
		defer func() {
			wg.Done()
			fmt.Printf("Zipped %d files to %s\n", totalFiles, payload.OutPutFile)
		}()
		// Upload the file, body is `io.Reader` from pipe
		result.Body = pr
		_, err := uploader.Upload(result)
		fmt.Println("Upload data.....")
		if err != nil {
			fmt.Println(err)
		}
	}()

	wg.Wait()

	return nil
}

func main() {
	var region = ""
	var s3Key = ""
	var s3Secret = ""
	var bucket = ""
	var prefix = ""
	var sess *session.Session

	if len(os.Args) == 6 {
		region = os.Args[3]
		s3Key = os.Args[4]
		s3Secret = os.Args[5]
		bucket = os.Args[1]
		prefix = os.Args[2]

		sess, _ = session.NewSession(&aws.Config{
			Region:      aws.String(region),
			Credentials: credentials.NewStaticCredentials(s3Key, s3Secret, ""),
		})
	} else if len(os.Args) == 4 {
		bucket = os.Args[1]
		prefix = os.Args[2]
		region = os.Args[3]
		sess, _ = session.NewSession(&aws.Config{
			Region: aws.String(region),
		})
	} else {
		fmt.Println("Usage: s3zipfolder <bucket> <prefix> <region>")
		fmt.Println("   OR  s3zipfolder <bucket> <prefix> <region> <aws key> <aws secret>")
	}

	outputFile := fmt.Sprintf("%s.%d.zip", strings.TrimRight(prefix, "/"), time.Now().Unix())
	var payload = PayLoad{
		Region:     region,
		Bucket:     bucket,
		Prefix:     prefix,
		OutPutFile: outputFile,
	}

	fetchFiles(sess, &payload)
	zipS3Files(payload, sess)

	fmt.Println("DONE")
}
