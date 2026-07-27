package console

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeleteStorage_NilPool(t *testing.T) {
	err := DeleteStorage(context.Background(), nil)
	require.Error(t, err)
}

func TestImportStorageObjects_Empty(t *testing.T) {
	require.NoError(t, ImportStorageObjects(context.Background(), nil, nil))
}
