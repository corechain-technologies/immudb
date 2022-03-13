/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package azure

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/codenotary/immudb/embedded/remotestorage"
)

type Storage struct {
	endpoint        string
	container       string
	prefix          string
	cred            azcore.TokenCredential
	containerClient *azblob.ContainerClient
}

var (
	ErrInvalidArguments = errors.New("invalid arguments")
	ErrInvalidResponse  = errors.New("invalid response code")
	ErrTooManyRedirects = errors.New("too many redirects")
)

func Open(
	endpoint string,
	container string,
	prefix string,
	cred azcore.TokenCredential,
) (remotestorage.Storage, error) {
	// azblob.NewContainerClient(endpoint, cred azcore.TokenCredential, options *azblob.ClientOptions)
	// Endpoint must always end with '/'
	endpoint = strings.TrimRight(endpoint, "/") + "/"

	// Bucket must have no '/' at all
	container = strings.Trim(container, "/")
	if strings.Contains(container, "/") {
		return nil, ErrInvalidArguments
	}

	// if prefix is not empty, it must end with '/'
	prefix = strings.Trim(prefix, "/")
	if prefix != "" {
		prefix = prefix + "/"
	}

	client, err := azblob.NewContainerClient(endpoint, cred, nil)
	if err != nil {
		return nil, err
	}

	return &Storage{
		endpoint:        endpoint,
		container:       container,
		prefix:          prefix,
		cred:            cred,
		containerClient: &client,
	}, nil
}

func (s *Storage) String() string {
	return "blob:" + s.endpoint
}

// Get opens a remote blob resource
func (s *Storage) Get(ctx context.Context, name string, offs, size int64) (io.ReadCloser, error) {
	if offs < 0 || size == 0 {
		return nil, ErrInvalidArguments
	}
	if strings.HasPrefix(name, "/") || strings.HasSuffix(name, "/") {
		return nil, ErrInvalidArguments
	}

	client := s.containerClient.NewBlobClient(name)
	_bytes := make([]byte, size)

	err := client.DownloadBlobToBuffer(ctx, offs, size, _bytes, azblob.HighLevelDownloadFromBlobOptions{})
	if err != nil {
		return nil, err
	}

	return &metricsCountingReadCloser{
		r: io.NopCloser(bytes.NewBuffer(_bytes)),
		c: metricsDownloadBytes,
	}, nil
}

// Put writes a remote blob resource
func (s *Storage) Put(ctx context.Context, name string, fileName string) error {
	if strings.HasPrefix(name, "/") || strings.HasSuffix(name, "/") {
		return ErrInvalidArguments
	}

	fl, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer fl.Close()

	client := s.containerClient.NewBlockBlobClient(name)

	resp, err := client.UploadFileToBlockBlob(ctx, fl, azblob.HighLevelUploadToBlockBlobOption{})
	if err != nil {
		return err
	}

	resp.Body.Close()
	return nil
}

// Exists checks if a remove resource exists and can be read.
// Note that due to an asynchronous nature of cloud storage,
// a resource stored with the Put method may not be immediately accessible.
func (s *Storage) Exists(ctx context.Context, name string) (bool, error) {
	if strings.HasPrefix(name, "/") || strings.HasSuffix(name, "/") {
		return false, ErrInvalidArguments
	}

	client := s.containerClient.NewBlobClient(name)

	_, err := client.GetProperties(ctx, &azblob.GetBlobPropertiesOptions{})
	var respErr *azblob.StorageError
	if err != nil {
		if errors.As(err, &respErr) {
			if respErr.ErrorCode == azblob.StorageErrorCodeBlobNotFound {
				return false, nil
			}
		}
		return false, err
	}

	return true, nil
}

func (s *Storage) ListEntries(ctx context.Context, path string) ([]remotestorage.EntryInfo, []string, error) {
	if path != "" {
		if !strings.HasSuffix(path, "/") ||
			strings.Contains(path, "//") {
			return nil, nil, ErrInvalidArguments
		}
	}

	str := s.prefix + path
	pager := s.containerClient.ListBlobsHierarchy("/", &azblob.ContainerListBlobHierarchySegmentOptions{Prefix: &str})

	entries := []remotestorage.EntryInfo{}
	subPaths := []string{}

	for pager.NextPage(ctx) {
		resp := pager.PageResponse()

		for _, v := range resp.ContainerListBlobHierarchySegmentResult.Segment.BlobPrefixes {
			subPaths = append(subPaths, *v.Name)
		}

		for _, v := range resp.ContainerListBlobHierarchySegmentResult.Segment.BlobItems {
			entries = append(entries, remotestorage.EntryInfo{
				Name: *v.Name,
				Size: *v.Properties.ContentLength,
			})
		}
	}

	if !sort.SliceIsSorted(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name }) ||
		!sort.StringsAreSorted(subPaths) {
		return nil, nil, ErrInvalidResponse
	}

	return entries, subPaths, nil
}

var _ remotestorage.Storage = (*Storage)(nil)
