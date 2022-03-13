// zbuild azure_blob_storage

package azure

import (
	"context"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/stretchr/testify/require"
)

func TestSimpleUpload(t *testing.T) {
	cred, err := azidentity.NewAzureCLICredential(nil)
	if err != nil {
		t.Fatal(err)
	}

	s, err := Open(
		"https://paymentworksdevstorage.blob.core.windows.net/immudb",
		"decrypted",
		"",
		cred,
	)
	require.NoError(t, err)

	ctx := context.Background()

	// Reader is wrapped to ensure it's not recognized as the in-memory buffer.
	// Standard http lib in golang detect Content-Length headers for bytes.Buffer readers.
	fl, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	fmt.Fprintf(fl, "Hello world")
	fl.Close()

	err = s.Put(ctx, "test1", fl.Name())
	require.NoError(t, err)
}
