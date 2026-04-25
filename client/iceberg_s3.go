package client

import (
	"context"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type IcebergS3Client struct {
	S3       *s3.Client
	Bucket   string
	DataPath string
}

func NewIcebergS3Client(ctx context.Context, endpoint, region, accessKey, secretKey, bucket, dataPath string) (*IcebergS3Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	)
	if err != nil {
		return nil, err
	}
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
		o.UsePathStyle = true
	})
	return &IcebergS3Client{S3: s3Client, Bucket: bucket, DataPath: dataPath}, nil
}

func (c *IcebergS3Client) CountDataFiles(ctx context.Context) (int, error) {
	prefix := strings.TrimSuffix(c.DataPath, "/") + "/"
	var count int
	var continuation *string
	for {
		out, err := c.S3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &c.Bucket,
			Prefix:            &prefix,
			ContinuationToken: continuation,
		})
		if err != nil {
			return 0, err
		}
		for _, obj := range out.Contents {
			if obj.Key != nil && strings.HasSuffix(*obj.Key, ".parquet") {
				count++
			}
		}
		if out.IsTruncated == nil || !*out.IsTruncated {
			break
		}
		continuation = out.NextContinuationToken
	}
	return count, nil
}

func (c *IcebergS3Client) WaitForFileIncrease(ctx context.Context, startCount int, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for {
		n, err := c.CountDataFiles(ctx)
		if err == nil && n > startCount {
			return n, nil
		}
		if time.Now().After(deadline) {
			return n, err
		}
		time.Sleep(1 * time.Second)
	}
}
