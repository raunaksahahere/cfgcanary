// Package blob stores immutable, content-addressed objects in an
// S3-compatible store and issues short-lived presigned URLs for them.
//
// Objects live at <prefix>sha256/<hex>. The bucket is private; agents only
// ever see presigned GET URLs for versions their ring is entitled to
// (docs/invariants.md I2).
package blob

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// ErrNotFound is returned when an object does not exist.
var ErrNotFound = errors.New("blob not found")

// ErrCorrupt is returned when stored bytes do not match their digest.
var ErrCorrupt = errors.New("blob does not match its digest")

// Config describes the object store.
type Config struct {
	// Endpoint is host:port, without scheme.
	Endpoint  string
	Bucket    string
	Prefix    string
	Region    string
	AccessKey string
	SecretKey string
	Secure    bool
	// CAFile, when set, is the CA bundle used to verify the store's
	// certificate.
	CAFile string
	// PublicEndpoint, when set, is the host:port agents use; presigned URLs
	// are issued for it. Defaults to Endpoint.
	PublicEndpoint string
}

// Store is an S3-backed blob store.
type Store struct {
	cfg     Config
	client  *minio.Client
	presign *minio.Client
}

func newClient(cfg Config, endpoint string) (*minio.Client, error) {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.ResponseHeaderTimeout = 30 * time.Second
	if cfg.Secure {
		tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		if cfg.CAFile != "" {
			pem, err := os.ReadFile(cfg.CAFile)
			if err != nil {
				return nil, fmt.Errorf("object store CA: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, errors.New("object store CA: no certificates found")
			}
			tr.TLSClientConfig.RootCAs = pool
		}
	}
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	return minio.New(endpoint, &minio.Options{
		Creds:     credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure:    cfg.Secure,
		Region:    region, // set explicitly so presigning never needs a network call
		Transport: tr,
	})
}

// New connects to the store. It does not create the bucket; use EnsureBucket.
func New(cfg Config) (*Store, error) {
	if cfg.Endpoint == "" || cfg.Bucket == "" {
		return nil, errors.New("object store endpoint and bucket are required")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	c, err := newClient(cfg, cfg.Endpoint)
	if err != nil {
		return nil, err
	}
	p := c
	if cfg.PublicEndpoint != "" && cfg.PublicEndpoint != cfg.Endpoint {
		if p, err = newClient(cfg, cfg.PublicEndpoint); err != nil {
			return nil, err
		}
	}
	return &Store{cfg: cfg, client: c, presign: p}, nil
}

// EnsureBucket creates the bucket if it does not exist. Buckets are created
// private; cfgcanary never sets a bucket policy.
func (s *Store) EnsureBucket(ctx context.Context) error {
	ok, err := s.client.BucketExists(ctx, s.cfg.Bucket)
	if err != nil {
		return fmt.Errorf("object store: %w", err)
	}
	if ok {
		return nil
	}
	err = s.client.MakeBucket(ctx, s.cfg.Bucket, minio.MakeBucketOptions{Region: s.cfg.Region})
	if err != nil {
		if code := minio.ToErrorResponse(err).Code; code == "BucketAlreadyOwnedByYou" || code == "BucketAlreadyExists" {
			return nil
		}
		return fmt.Errorf("object store: %w", err)
	}
	return nil
}

// Key returns the object key for a digest.
func (s *Store) Key(sha string) string { return s.cfg.Prefix + "sha256/" + sha }

// Digest returns the hex SHA-256 of data.
func Digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Put uploads data under its digest. Re-uploading identical content writes
// the same bytes to the same key.
func (s *Store) Put(ctx context.Context, sha string, data []byte) error {
	if Digest(data) != sha {
		return ErrCorrupt
	}
	_, err := s.client.PutObject(ctx, s.cfg.Bucket, s.Key(sha), bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		ContentType:    "application/octet-stream",
		SendContentMd5: true,
	})
	if err != nil {
		return fmt.Errorf("object store put: %w", err)
	}
	return nil
}

// Get downloads an object, refusing more than max bytes, and verifies its
// digest.
func (s *Store) Get(ctx context.Context, sha string, max int64) ([]byte, error) {
	obj, err := s.client.GetObject(ctx, s.cfg.Bucket, s.Key(sha), minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("object store get: %w", err)
	}
	defer obj.Close()
	data, err := io.ReadAll(io.LimitReader(obj, max+1))
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("object store get: %w", err)
	}
	if int64(len(data)) > max || Digest(data) != sha {
		return nil, ErrCorrupt
	}
	return data, nil
}

// Exists reports whether the object is present.
func (s *Store) Exists(ctx context.Context, sha string) (bool, error) {
	_, err := s.client.StatObject(ctx, s.cfg.Bucket, s.Key(sha), minio.StatObjectOptions{})
	if err == nil {
		return true, nil
	}
	if minio.ToErrorResponse(err).Code == "NoSuchKey" {
		return false, nil
	}
	return false, fmt.Errorf("object store stat: %w", err)
}

// Delete removes an object. Deleting a missing object is not an error.
func (s *Store) Delete(ctx context.Context, sha string) error {
	err := s.client.RemoveObject(ctx, s.cfg.Bucket, s.Key(sha), minio.RemoveObjectOptions{})
	if err != nil && minio.ToErrorResponse(err).Code != "NoSuchKey" {
		return fmt.Errorf("object store delete: %w", err)
	}
	return nil
}

// Presign returns a GET URL valid for ttl. Signing is local; it makes no
// network call, so it works while the object store is unreachable.
func (s *Store) Presign(ctx context.Context, sha string, ttl time.Duration) (string, time.Time, error) {
	u, err := s.presign.PresignedGetObject(ctx, s.cfg.Bucket, s.Key(sha), ttl, url.Values{})
	if err != nil {
		return "", time.Time{}, fmt.Errorf("presign: %w", err)
	}
	return u.String(), time.Now().Add(ttl), nil
}

// List returns the digests of all stored objects.
func (s *Store) List(ctx context.Context) ([]string, error) {
	var out []string
	for o := range s.client.ListObjects(ctx, s.cfg.Bucket, minio.ListObjectsOptions{Prefix: s.cfg.Prefix + "sha256/", Recursive: true}) {
		if o.Err != nil {
			return nil, o.Err
		}
		out = append(out, strings.TrimPrefix(o.Key, s.cfg.Prefix+"sha256/"))
	}
	return out, nil
}

// Origin is the scheme://host[:port] agents should allow downloads from.
func (s *Store) Origin() string {
	scheme := "http"
	if s.cfg.Secure {
		scheme = "https"
	}
	ep := s.cfg.PublicEndpoint
	if ep == "" {
		ep = s.cfg.Endpoint
	}
	return scheme + "://" + ep
}
