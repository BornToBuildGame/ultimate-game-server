package fleet

import (
	"sync"

	"github.com/google/uuid"
)

// LocalFmCallbackHandler is an in-process Create callback registry.
type LocalFmCallbackHandler struct {
	callbackRegistry sync.Map
}

// NewLocalFmCallbackHandler builds a callback handler.
func NewLocalFmCallbackHandler() *LocalFmCallbackHandler {
	return &LocalFmCallbackHandler{}
}

// GenerateCallbackId returns a new callback id.
func (fch *LocalFmCallbackHandler) GenerateCallbackId() string {
	return uuid.NewString()
}

// SetCallback stores a callback by id.
func (fch *LocalFmCallbackHandler) SetCallback(callbackId string, fn FmCreateCallbackFn) {
	fch.callbackRegistry.Store(callbackId, fn)
}

// InvokeCallback runs and removes a stored callback.
func (fch *LocalFmCallbackHandler) InvokeCallback(callbackId string, status FmCreateStatus, instanceInfo *InstanceInfo, sessionInfo []*SessionInfo, metadata map[string]any, err error) {
	callback, ok := fch.callbackRegistry.LoadAndDelete(callbackId)
	if !ok || callback == nil {
		return
	}
	fn := callback.(FmCreateCallbackFn)
	fn(status, instanceInfo, sessionInfo, metadata, err)
}
