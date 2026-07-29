package runtime

import (
	"testing"

	"github.com/dop251/goja"
	"github.com/stretchr/testify/require"
	lua "github.com/yuin/gopher-lua"
)


func TestBatch3_GoRuntime(t *testing.T) {
	m := &GoRuntimeModule{}

	// Crypto & Bcrypt
	sha, err := m.CryptoHash("sha256", "hello")
	require.NoError(t, err)
	require.NotEmpty(t, sha)

	hmacVal, err := m.CryptoHmacHash("sha256", "key", "data")
	require.NoError(t, err)
	require.NotEmpty(t, hmacVal)

	hash, err := m.BcryptHash("secret")
	require.NoError(t, err)
	require.True(t, m.BcryptCompare(hash, "secret"))
	require.False(t, m.BcryptCompare(hash, "wrong"))

	// UUID
	id := m.UuidV4()
	require.NotEmpty(t, id)

	// LocalCache
	m.LocalCacheSet("foo", "bar", 10)
	val, ok := m.LocalCacheGet("foo")
	require.True(t, ok)
	require.Equal(t, "bar", val)
}

func TestBatch3_LuaBindings(t *testing.T) {
	L := lua.NewState()
	defer L.Close()

	nk := &mockRuntimeModule{}
	nkTable := L.NewTable()
	mapLuaNKBatch3(L, nkTable, nk)
	L.SetGlobal("nk", nkTable)

	script := `
		local hash = nk.crypto_hash("sha256", "hello")
		assert(hash == "hash", "hash mismatch")

		local pass = nk.bcrypt_hash("secret")
		assert(pass == "hashed", "bcrypt mismatch")

		local cmp = nk.bcrypt_compare("hashed", "secret")
		assert(cmp == true, "bcrypt compare mismatch")

		local id = nk.uuid_v4()
		assert(id ~= nil, "uuid nil")

		nk.localcache_set("key", "val", 10)
	`
	err := L.DoString(script)
	require.NoError(t, err)
}

func TestBatch3_JSBindings(t *testing.T) {
	vm := goja.New()
	nk := &mockRuntimeModule{}
	nkObj := vm.NewObject()
	mapJSNKBatch3(vm, nkObj, nk)
	_ = vm.Set("nk", nkObj)

	script := `
		var hash = nk.cryptoHash("sha256", "hello");
		if (hash !== "hash") throw new Error("hash mismatch");

		var pass = nk.bcryptHash("secret");
		if (pass !== "hashed") throw new Error("bcrypt mismatch");

		var cmp = nk.bcryptCompare("hashed", "secret");
		if (!cmp) throw new Error("bcrypt compare mismatch");

		var id = nk.uuidV4();
		if (!id) throw new Error("uuid missing");

		nk.localCacheSet("key", "val", 10);
	`
	_, err := vm.RunString(script)
	require.NoError(t, err)
}
