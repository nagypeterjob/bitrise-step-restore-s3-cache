package step

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/bitrise-io/go-steputils/v2/cache/network"
	"github.com/bitrise-io/go-utils/retry"
	"github.com/bitrise-io/go-utils/v2/log"
)

const (
	maxKeyLength = 512

	maxKeyCount = 8
)

var (
	errCacheNotFound = errors.New("no cache archive found for the provided keys")
	errS3KeyNotFound = errors.New("key not found in s3 bucket")
	errNoKeyProvided = errors.New("no keys provided")
)

type DownloadService struct {
	Client          *s3.Client
	Bucket          string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
}

// Download archive from the provided S3 bucket based on the provided keys in params.
// If there is no match for any of the keys, the error is ErrCacheNotFound.
func (s DownloadService) Download(ctx context.Context, params network.DownloadParams, logger log.Logger) (string, error) {
	truncatedKeys, err := validateKeys(params.CacheKeys)
	if err != nil {
		return "", fmt.Errorf("validate keys: %w", err)
	}

	if s.Bucket == "" {
		return "", fmt.Errorf("bucket must not be empty")
	}

	cfg, err := loadAWSCredentials(
		ctx,
		s.Region,
		s.AccessKeyID,
		s.SecretAccessKey,
		logger,
	)
	if err != nil {
		return "", fmt.Errorf("load aws credentials: %w", err)
	}

	s.Client = s3.NewFromConfig(*cfg)
	return s.downloadWithS3Client(ctx, truncatedKeys, params, logger)
}

func (s DownloadService) downloadWithS3Client(
	ctx context.Context,
	cacheKeys []string,
	params network.DownloadParams,
	logger log.Logger,
) (string, error) {
	firstValidKey, err := s.firstAvailableKey(ctx, cacheKeys, logger)
	if err != nil {
		if !errors.Is(errS3KeyNotFound, err) {
			return "", fmt.Errorf("matching key: %w", err)
		}

		logger.Debugf("Could not match provided cache keys, falling back to find archive by prefix...")
		firstValidKey, err = s.firstAvailableKeyWithPrefix(ctx, cacheKeys)
		if err != nil {
			if errors.Is(errS3KeyNotFound, err) {
				return "", errCacheNotFound
			}
			return "", fmt.Errorf("finding archive by prefix: %w", err)
		}
	}

	err = retry.Times(uint(params.NumFullRetries)).Wait(5 * time.Second).TryWithAbort(func(attempt uint) (error, bool) {
		if err := s.getObject(ctx, firstValidKey, params.DownloadPath); err != nil {
			return fmt.Errorf("download object: %w", err), false
		}

		return nil, true
	})
	if err != nil {
		return "", fmt.Errorf("all retries failed: %w", err)
	}

	return firstValidKey, nil
}

func (s DownloadService) firstAvailableKey(
	ctx context.Context,
	keys []string,
	logger log.Logger,
) (string, error) {
	for _, key := range keys {
		fileKey := strings.Join([]string{key, "tzst"}, ".")

		_, err := s.Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(s.Bucket),
			Key:    aws.String(fileKey),
		})
		if err != nil {
			var apiError smithy.APIError
			if errors.As(err, &apiError) {
				switch apiError.(type) {
				case *types.NotFound:
					logger.Debugf("archive with key %s not found in bucket", key)
					continue
				default:
					return "", fmt.Errorf("aws api error: %w", err)
				}
			}
			return "", fmt.Errorf("generic aws error: %w", err)
		}

		return key, nil
	}
	return "", errS3KeyNotFound
}

func (s DownloadService) firstAvailableKeyWithPrefix(ctx context.Context, keys []string) (string, error) {
	for _, key := range keys {
		p := s3.NewListObjectsV2Paginator(s.Client, &s3.ListObjectsV2Input{
			Bucket: aws.String(s.Bucket),
			Prefix: aws.String(key),
		}, func(o *s3.ListObjectsV2PaginatorOptions) {})

		pCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		// Getting the first "valid" key from the first result page is enough
		page, err := p.NextPage(pCtx)
		if err != nil {
			return "", fmt.Errorf("find artifact for key prefix: %w", err)
		}

		if len(page.Contents) != 0 && page.Contents[0].Key != nil {
			return *page.Contents[0].Key, nil
		}
	}

	return "", errS3KeyNotFound
}

func (s *DownloadService) getObject(ctx context.Context, key string, downloadPath string) error {
	file, err := os.Create(downloadPath)
	if err != nil {
		return fmt.Errorf("creating file: %w", err)
	}
	defer file.Close() //nolint:errcheck

	downloader := manager.NewDownloader(s.Client, func(d *manager.Downloader) {
		// 50MB
		d.PartSize = 50 * 1024 * 1024
		d.Concurrency = runtime.NumCPU()
	})

	_, err = downloader.Download(ctx, file, &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("get object: %w", err)
	}

	return nil
}

func loadAWSCredentials(
	ctx context.Context,
	region string,
	accessKeyID string,
	secretKey string,
	logger log.Logger,
) (*aws.Config, error) {
	if region == "" {
		return nil, fmt.Errorf("region must not be empty")
	}

	opts := []func(*config.LoadOptions) error{
		config.WithRegion(region),
	}

	if accessKeyID != "" && secretKey != "" {
		logger.Debugf("aws credentials provided, using them...")
		opts = append(opts,
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyID, secretKey, "")))
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load config, %v", err)
	}

	return &cfg, nil
}

func validateKeys(keys []string) ([]string, error) {
	if len(keys) == 0 {
		return nil, errNoKeyProvided
	}
	if len(keys) > maxKeyCount {
		return nil, fmt.Errorf("maximum number of keys is %d, %d provided", maxKeyCount, len(keys))
	}

	truncatedKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		if strings.Contains(key, ",") {
			return nil, fmt.Errorf("commas are not allowed in keys (invalid key: %s)", key)
		}
		if len(key) > maxKeyLength {
			truncatedKeys = append(truncatedKeys, key[:maxKeyLength])
		} else {
			truncatedKeys = append(truncatedKeys, key)
		}
	}

	return truncatedKeys, nil
}
