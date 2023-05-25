package minio

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/minio/minio-go/v7/pkg/s3utils"
)

type MultipartUploader struct {
	BucketName        string
	ObjectName        string
	UploadID          string
	TotalPartsCount   int
	ObjectSize        int64
	PartSize          int64
	TotalUploadedSize int64
	partsInfo         map[int]ObjectPart
	crcByte           map[int][]byte
	client            *Client
	opts              *PutObjectOptions
	mux               sync.RWMutex
}

func (c *Client) NewUploadID(ctx context.Context, bucketName, objectName string, objectSize int64, opts *PutObjectOptions) (*MultipartUploader, error) {
	var err error
	// Input validation.
	if err = s3utils.CheckValidBucketName(bucketName); err != nil {
		return &MultipartUploader{}, err
	}
	if err = s3utils.CheckValidObjectName(objectName); err != nil {
		return &MultipartUploader{}, err
	}

	var totalPartsCount int
	var partSize int64

	// Calculate the optimal parts info for a given size.
	totalPartsCount, partSize, _, err = OptimalPartInfo(objectSize, opts.PartSize)
	if err != nil {
		return &MultipartUploader{}, err
	}

	if !opts.SendContentMd5 {
		if opts.UserMetadata == nil {
			opts.UserMetadata = make(map[string]string, 1)
		}
		opts.UserMetadata["X-Amz-Checksum-Algorithm"] = "CRC32C"
	}

	// Initiate a new multipart upload.
	uploadID, err := c.newUploadID(ctx, bucketName, objectName, *opts)
	if err != nil {
		return &MultipartUploader{}, err
	}
	delete(opts.UserMetadata, "X-Amz-Checksum-Algorithm")

	return &MultipartUploader{
		BucketName:        bucketName,
		ObjectName:        objectName,
		UploadID:          uploadID,
		client:            c,
		TotalPartsCount:   totalPartsCount,
		PartSize:          partSize,
		ObjectSize:        objectSize,
		TotalUploadedSize: 0,
		opts:              opts,
		partsInfo:         make(map[int]ObjectPart), // Initialize parts uploaded map.
		crcByte:           make(map[int][]byte),
	}, nil
}

// 取消并清除未完成的uploadID的多块上传
func (c *Client) CancelMultipartUploader(ctx context.Context, bucketName, objectName, uploadID string) error {
	return c.abortMultipartUpload(ctx, bucketName, objectName, uploadID)
}

// 分块上传
func (p *MultipartUploader) UploadPart(ctx context.Context, reader io.Reader, partNumber int, bodySize int64, bodySum string) error {
	// Create checksums
	// CRC32C is ~50% faster on AMD64 @ 30GB/s
	var crcBytes []byte
	customHeader := make(http.Header)

	cSum, err := base64.StdEncoding.DecodeString(bodySum)
	if err != nil {
		return errors.New("UploadPart body checksum value is invalid.")
	}

	var md5Base64 string
	if p.opts.SendContentMd5 {
		md5Base64 = bodySum
	} else {
		customHeader.Set("x-amz-checksum-crc32c", bodySum)
		crcBytes = append(crcBytes, cSum...)
	}

	// Update progress reader appropriately to the latest offset
	// as we read from the source.
	rd := newHook(reader, p.opts.Progress)

	// Proceed to upload the part.
	upp := uploadPartParams{
		bucketName:   p.BucketName,
		objectName:   p.ObjectName,
		uploadID:     p.UploadID,
		reader:       rd,
		partNumber:   partNumber,
		md5Base64:    md5Base64,
		size:         bodySize,
		sse:          p.opts.ServerSideEncryption,
		streamSha256: !p.opts.DisableContentSha256,
		customHeader: customHeader,
	}
	objPart, uerr := p.client.uploadPart(ctx, upp)
	if uerr != nil {
		return uerr
	}

	p.mux.Lock()
	// Save successfully uploaded part metadata.
	p.partsInfo[partNumber] = objPart

	p.crcByte[partNumber] = crcBytes
	p.mux.Unlock()
	// Save successfully uploaded size.
	atomic.AddInt64(&p.TotalUploadedSize, bodySize)

	return nil
}

func (p *MultipartUploader) complMultipartUpload() (*completeMultipartUpload, []byte, error) {
	p.mux.RLock()
	defer p.mux.RUnlock()
	crcBytes := make([]byte, 0, 4*p.TotalPartsCount)
	complMultipartUpload := &completeMultipartUpload{
		Parts: make([]CompletePart, 0, p.TotalPartsCount),
	}
	// Loop over total uploaded parts to save them in
	// Parts array before completing the multipart request.
	for i := 1; i <= p.TotalPartsCount; i++ {
		part, ok := p.partsInfo[i]
		if !ok {
			return nil, nil, errInvalidArgument(fmt.Sprintf("Missing part number %d", i))
		}
		crcBytes = append(crcBytes, p.crcByte[i]...)
		complMultipartUpload.Parts = append(complMultipartUpload.Parts, CompletePart{
			ETag:           part.ETag,
			PartNumber:     part.PartNumber,
			ChecksumCRC32:  part.ChecksumCRC32,
			ChecksumCRC32C: part.ChecksumCRC32C,
			ChecksumSHA1:   part.ChecksumSHA1,
			ChecksumSHA256: part.ChecksumSHA256,
		})
	}

	// Sort all completed parts.
	sort.Sort(completedParts(complMultipartUpload.Parts))

	return complMultipartUpload, crcBytes, nil
}

// crc32Pool

var crc32Pool = sync.Pool{
	New: func() interface{} {
		return crc32.New(crc32.MakeTable(crc32.Castagnoli))
	},
}

func getCrc32() hash.Hash32 {
	return crc32Pool.Get().(hash.Hash32)
}

func putCrc32(c32 hash.Hash32) {
	crc32Pool.Put(c32)
}

// 分块上传后完成
func (p *MultipartUploader) CompleteMultipartUpload(ctx context.Context) (*UploadInfo, error) {
	complMultipartUpload, crcBytes, err := p.complMultipartUpload()
	if err != nil {
		return nil, err
	}
	c32 := getCrc32()
	defer putCrc32(c32)

	opts := PutObjectOptions{}
	if len(crcBytes) > 0 {
		// Add hash of hashes.
		c32.Reset()
		c32.Write(crcBytes)
		opts.UserMetadata = map[string]string{"X-Amz-Checksum-Crc32c": base64.StdEncoding.EncodeToString(c32.Sum(nil))}
	}
	uploadInfo, err := p.client.completeMultipartUpload(ctx, p.BucketName, p.ObjectName, p.UploadID, *complMultipartUpload, opts)
	if err != nil {
		return &UploadInfo{}, err
	}

	uploadInfo.Size = p.TotalUploadedSize
	return &uploadInfo, nil
}
