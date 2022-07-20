// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// S3 storage backend saves files into S3 compatible storages
// For non-aws providers, endpoint must be provided
type S3 struct {
	Endpoint     string `protobuf:"bytes,1,opt,name=endpoint,proto3" json:"endpoint,omitempty"`
	Region       string `protobuf:"bytes,2,opt,name=region,proto3" json:"region,omitempty"`
	Bucket       string `protobuf:"bytes,3,opt,name=bucket,proto3" json:"bucket,omitempty"`
	Prefix       string `protobuf:"bytes,4,opt,name=prefix,proto3" json:"prefix,omitempty"`
	StorageClass string `protobuf:"bytes,5,opt,name=storage_class,json=storageClass,proto3" json:"storage_class,omitempty"`
	// server side encryption
	Sse                  string   `protobuf:"bytes,6,opt,name=sse,proto3" json:"sse,omitempty"`
	Acl                  string   `protobuf:"bytes,7,opt,name=acl,proto3" json:"acl,omitempty"`
	AccessKey            string   `protobuf:"bytes,8,opt,name=access_key,json=accessKey,proto3" json:"access_key,omitempty"`
	SecretAccessKey      string   `protobuf:"bytes,9,opt,name=secret_access_key,json=secretAccessKey,proto3" json:"secret_access_key,omitempty"`
	ForcePathStyle       bool     `protobuf:"varint,10,opt,name=force_path_style,json=forcePathStyle,proto3" json:"force_path_style,omitempty"`
	SseKmsKeyId          string   `protobuf:"bytes,11,opt,name=sse_kms_key_id,json=sseKmsKeyId,proto3" json:"sse_kms_key_id,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}


// S3BackendOptions contains options for s3 storage.
type S3BackendOptions struct {
	Endpoint              string `json:"endpoint" toml:"endpoint"`
	Region                string `json:"region" toml:"region"`
	StorageClass          string `json:"storage-class" toml:"storage-class"`
	Sse                   string `json:"sse" toml:"sse"`
	SseKmsKeyID           string `json:"sse-kms-key-id" toml:"sse-kms-key-id"`
	ACL                   string `json:"acl" toml:"acl"`
	AccessKey             string `json:"access-key" toml:"access-key"`
	SecretAccessKey       string `json:"secret-access-key" toml:"secret-access-key"`
	Provider              string `json:"provider" toml:"provider"`
	ForcePathStyle        bool   `json:"force-path-style" toml:"force-path-style"`
	UseAccelerateEndpoint bool   `json:"use-accelerate-endpoint" toml:"use-accelerate-endpoint"`
}

// GCSBackendOptions are options for configuration the GCS storage.
type GCSBackendOptions struct {
	Endpoint        string `json:"endpoint" toml:"endpoint"`
	StorageClass    string `json:"storage-class" toml:"storage-class"`
	PredefinedACL   string `json:"predefined-acl" toml:"predefined-acl"`
	CredentialsFile string `json:"credentials-file" toml:"credentials-file"`
}

// AzblobBackendOptions is the options for Azure Blob storage.
type AzblobBackendOptions struct {
	Endpoint    string `json:"endpoint" toml:"endpoint"`
	AccountName string `json:"account-name" toml:"account-name"`
	AccountKey  string `json:"account-key" toml:"account-key"`
	AccessTier  string `json:"access-tier" toml:"access-tier"`
}

// BackendOptions further configures the storage backend not expressed by the
// storage URL.
type BackendOptions struct {
	S3     S3BackendOptions     `json:"s3" toml:"s3"`
	GCS    GCSBackendOptions    `json:"gcs" toml:"gcs"`
	Azblob AzblobBackendOptions `json:"azblob" toml:"azblob"`
}

type isStorageBackend_Backend interface {
	isStorageBackend_Backend()
	MarshalTo([]byte) (int, error)
	Size() int
}

type StorageBackend struct {
	// Types that are valid to be assigned to Backend:
	//	*StorageBackend_Noop
	//	*StorageBackend_Local
	//	*StorageBackend_S3
	//	*StorageBackend_Gcs
	//	*StorageBackend_CloudDynamic
	//	*StorageBackend_Hdfs
	//	*StorageBackend_AzureBlobStorage
	Backend              isStorageBackend_Backend `protobuf_oneof:"backend"`
	XXX_NoUnkeyedLiteral struct{}                 `json:"-"`
	XXX_unrecognized     []byte                   `json:"-"`
	XXX_sizecache        int32                    `json:"-"`
}

func (m *StorageBackend) GetBackend() isStorageBackend_Backend {
	if m != nil {
		return m.Backend
	}
	return nil
}

// ParseRawURL parse raw url to url object.
func ParseRawURL(rawURL string) (*url.URL, error) {
	// In aws the secret key may contain '/+=' and '+' has a special meaning in URL.
	// Replace "+" by "%2B" here to avoid this problem.
	rawURL = strings.ReplaceAll(rawURL, "+", "%2B")
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	return u, nil
}

func ParseBackend(rawURL string, options *BackendOptions) (*StorageBackend, error) {
	fmt.Println("wangjian sql0 is", rawURL)
	rawURL = "s3://wangjian-test/test/test3"
	if len(rawURL) == 0 {
		return nil, errors.New("empty store is not allowed")
	}
	url, err := ParseRawURL(rawURL)
	if err != nil {
		return nil, err
	}
	switch url.Scheme {
	case "s3":
		if url.Host == "" {
			return nil, fmt.Errorf("please check the bucket name for s3 in %s", rawURL)
		}
		prefix := strings.Trim(url.Path, "/")
		s3 := &S3{Bucket: url.Host, Prefix: prefix}
		fmt.Println("wangjian sql0b is", s3, url.Host, prefix)
		if options == nil {
			options = &BackendOptions{S3: S3BackendOptions{ForcePathStyle: true}}
		}
		return &StorageBackend{Backend: nil}, nil
	case "gcs":
		return nil, fmt.Errorf("storage %s not support yet", url.Scheme)
	case "azure":
		return nil, fmt.Errorf("storage %s not support yet", url.Scheme)
	default:
		return nil, fmt.Errorf("storage %s not support yet", url.Scheme)
	}
}

// Apply apply s3 options on backuppb.S3.
func (options *S3BackendOptions) Apply(s3 *S3) error {
	if options.Endpoint != "" {
		url, err := url.Parse(options.Endpoint)
		if err != nil {
			return err
		}
		if url.Scheme == "" {
			return errors.New("scheme not found in endpoint")
		}
		if url.Host == "" {
			return errors.New("host not found in endpoint")
		}
	}
	// In some cases, we need to set ForcePathStyle to false.
	// Refer to: https://rclone.org/s3/#s3-force-path-style
	if options.Provider == "alibaba" || options.Provider == "netease" ||
		options.UseAccelerateEndpoint {
		options.ForcePathStyle = false
	}
	
	if options.AccessKey == "" && options.SecretAccessKey != "" {
		return errors.New("access_key not found")
	}
	if options.AccessKey != "" && options.SecretAccessKey == "" {
		return errors.New("secret_access_key not found")
	}

	s3.Endpoint = strings.TrimSuffix(options.Endpoint, "/")
	fmt.Println("wangjian sql3 is", s3.Endpoint, options.Endpoint, options.Region, options.StorageClass, options.Sse, options.SseKmsKeyID, options.ACL)
	s3.Region = options.Region
	// StorageClass, SSE and ACL are acceptable to be empty
	s3.StorageClass = options.StorageClass
	s3.Sse = options.Sse
	s3.SseKmsKeyId = options.SseKmsKeyID
	s3.Acl = options.ACL
	s3.AccessKey = "AKIAW2D4ZBGT5LF74CXS"  //options.AccessKey
	s3.SecretAccessKey = "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" //options.SecretAccessKey
	s3.ForcePathStyle = options.ForcePathStyle
	return nil
}

// ExternalFileReader represents the streaming external file reader.
type ExternalFileReader interface {
	io.ReadCloser
	io.Seeker
}

func loadFromExternal() error {
	b, err := ParseBackend("s3://wangjian-test/test/test3", nil)
	if err != nil {
		return err
	}
	fmt.Println("wangjian sql6 is", b)



	return nil
}

// S3Storage info for s3 storage.
type S3Storage struct {
	session *session.Session
	svc     s3iface.S3API
	options *S3
}

// RangeInfo represents the an HTTP Content-Range header value
// of the form `bytes [Start]-[End]/[Size]`.
type RangeInfo struct {
	// Start is the absolute position of the first byte of the byte range,
	// starting from 0.
	Start int64
	// End is the absolute position of the last byte of the byte range. This end
	// offset is inclusive, e.g. if the Size is 1000, the maximum value of End
	// would be 999.
	End int64
	// Size is the total size of the original file.
	Size int64
}

var contentRangeRegex = regexp.MustCompile(`bytes (\d+)-(\d+)/(\d+)$`)

// ParseRangeInfo parses the Content-Range header and returns the offsets.
func ParseRangeInfo(info *string) (ri RangeInfo, err error) {
	if info == nil || len(*info) == 0 {
		err = errors.New("ContentRange is empty")
		return
	}
	subMatches := contentRangeRegex.FindStringSubmatch(*info)
	if len(subMatches) != 4 {
		err = fmt.Errorf("invalid content range: '%s'", *info)
		return
	}

	ri.Start, err = strconv.ParseInt(subMatches[1], 10, 64)
	if err != nil {
		err = fmt.Errorf("invalid start offset value '%s' in ContentRange '%s'", subMatches[1], *info)
		return
	}
	ri.End, err = strconv.ParseInt(subMatches[2], 10, 64)
	if err != nil {
		err = fmt.Errorf("invalid end offset value '%s' in ContentRange '%s'", subMatches[2], *info)
		return
	}
	ri.Size, err = strconv.ParseInt(subMatches[3], 10, 64)
	if err != nil {
		err = fmt.Errorf("invalid size size value '%s' in ContentRange '%s'", subMatches[3], *info)
		return
	}
	return
}

// if endOffset > startOffset, should return reader for bytes in [startOffset, endOffset).
func (rs *S3Storage) open(
	ctx context.Context,
	path string,
	startOffset, endOffset int64,
) (io.ReadCloser, RangeInfo, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(rs.options.Bucket),
		Key:    aws.String(rs.options.Prefix + path),
	}

	// If we just open part of the object, we set `Range` in the request.
	// If we meant to open the whole object, not just a part of it,
	// we do not pass the range in the request,
	// so that even if the object is empty, we can still get the response without errors.
	// Then this behavior is similar to openning an empty file in local file system.
	isFullRangeRequest := false
	var rangeOffset *string
	switch {
	case endOffset > startOffset:
		// s3 endOffset is inclusive
		rangeOffset = aws.String(fmt.Sprintf("bytes=%d-%d", startOffset, endOffset-1))
	case startOffset == 0:
		// openning the whole object, no need to fill the `Range` field in the request
		isFullRangeRequest = true
	default:
		rangeOffset = aws.String(fmt.Sprintf("bytes=%d-", startOffset))
	}
	input.Range = rangeOffset
	result, err := rs.svc.GetObjectWithContext(ctx, input)
	if err != nil {
		return nil, RangeInfo{}, err
	}

	var r RangeInfo
	// Those requests without a `Range` will have no `ContentRange` in the response,
	// In this case, we'll parse the `ContentLength` field instead.
	if isFullRangeRequest {
		// We must ensure the `ContentLengh` has data even if for empty objects,
		// otherwise we have no places to get the object size
		if result.ContentLength == nil {
			return nil, RangeInfo{}, fmt.Errorf("open file '%s' failed. The S3 object has no content length", path)
		}
		objectSize := *(result.ContentLength)
		r = RangeInfo{
			Start: 0,
			End:   objectSize - 1,
			Size:  objectSize,
		}
	} else {
		r, err = ParseRangeInfo(result.ContentRange)
		if err != nil {
			return nil, RangeInfo{}, err
		}
	}

	if startOffset != r.Start || (endOffset != 0 && endOffset != r.End+1) {
		return nil, r, fmt.Errorf("open file '%s' failed, expected range: %s, got: %v",
			path, *rangeOffset, result.ContentRange)
	}

	return result.Body, r, nil
}

// s3ObjectReader wrap GetObjectOutput.Body and add the `Seek` method.
type s3ObjectReader struct {
	storage   *S3Storage
	name      string
	reader    io.ReadCloser
	pos       int64
	rangeInfo RangeInfo
	// reader context used for implement `io.Seek`
	// currently, lightning depends on package `xitongsys/parquet-go` to read parquet file and it needs `io.Seeker`
	// See: https://github.com/xitongsys/parquet-go/blob/207a3cee75900b2b95213627409b7bac0f190bb3/source/source.go#L9-L10
	ctx      context.Context
	retryCnt int
}

// Close implement the io.Closer interface.
func (r *s3ObjectReader) Close() error {
	return r.reader.Close()
}

const(
	maxErrorRetries = 3
	maxSkipOffsetByRead = 1 << 16 // 64KB
)


// Read implement the io.Reader interface.
func (r *s3ObjectReader) Read(p []byte) (n int, err error) {
	maxCnt := r.rangeInfo.End + 1 - r.pos
	if maxCnt > int64(len(p)) {
		maxCnt = int64(len(p))
	}
	n, err = r.reader.Read(p[:maxCnt])
	// TODO: maybe we should use !errors.Is(err, io.EOF) here to avoid error lint, but currently, pingcap/errors
	// doesn't implement this method yet.
	if err != nil && err != io.EOF && r.retryCnt < maxErrorRetries { //nolint:errorlint
		// if can retry, reopen a new reader and try read again
		end := r.rangeInfo.End + 1
		if end == r.rangeInfo.Size {
			end = 0
		}
		_ = r.reader.Close()

		newReader, _, err1 := r.storage.open(r.ctx, r.name, r.pos, end)
		if err1 != nil {
			log.Warn("open new s3 reader failed", zap.String("file", r.name), zap.Error(err1))
			return
		}
		r.reader = newReader
		r.retryCnt++
		n, err = r.reader.Read(p[:maxCnt])
	}

	r.pos += int64(n)
	return
}

// Seek implement the io.Seeker interface.
//
// Currently, tidb-lightning depends on this method to read parquet file for s3 storage.
func (r *s3ObjectReader) Seek(offset int64, whence int) (int64, error) {
	var realOffset int64
	switch whence {
	case io.SeekStart:
		realOffset = offset
	case io.SeekCurrent:
		realOffset = r.pos + offset
	case io.SeekEnd:
		realOffset = r.rangeInfo.Size + offset
	default:
		return 0, fmt.Errorf("Seek: invalid whence '%d'", whence)
	}
	if realOffset < 0 {
		return 0, fmt.Errorf("Seek in '%s': invalid offset to seek '%d'.", r.name, realOffset)
	}

	if realOffset == r.pos {
		return realOffset, nil
	} else if realOffset >= r.rangeInfo.Size {
		// See: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
		// because s3's GetObject interface doesn't allow get a range that matches zero length data,
		// so if the position is out of range, we need to always return io.EOF after the seek operation.

		// close current read and open a new one which target offset
		if err := r.reader.Close(); err != nil {
			log.L().Warn("close s3 reader failed, will ignore this error")
		}

		r.reader = io.NopCloser(bytes.NewReader(nil))
		r.pos = r.rangeInfo.Size
		return r.pos, nil
	}

	// if seek ahead no more than 64k, we discard these data
	if realOffset > r.pos && realOffset-r.pos <= maxSkipOffsetByRead {
		_, err := io.CopyN(io.Discard, r, realOffset-r.pos)
		if err != nil {
			return r.pos, err
		}
		return realOffset, nil
	}

	// close current read and open a new one which target offset
	err := r.reader.Close()
	if err != nil {
		return 0, err
	}

	newReader, info, err := r.storage.open(r.ctx, r.name, realOffset, 0)
	if err != nil {
		return 0, err
	}
	r.reader = newReader
	r.rangeInfo = info
	r.pos = realOffset
	return realOffset, nil
}

// Open a Reader by file path.
func (rs *S3Storage) Open(ctx context.Context, path string) (ExternalFileReader, error) {
	reader, r, err := rs.open(ctx, path, 0, 0)
	if err != nil {
		return nil, err
	}
	return &s3ObjectReader{
		storage:   rs,
		name:      path,
		reader:    reader,
		ctx:       ctx,
		rangeInfo: r,
	}, nil
}