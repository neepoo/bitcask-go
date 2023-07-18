package key_dir

import "errors"

var ErrKeyIsNil = errors.New("key is nil")

// Index - keydir interface
// NOTE: nil isn't a valid key
type Index interface {
	// Get - get keydir value by key
	Get(key []byte) (*MemValue, error)
	// Set - set keydir value by key
	Set(key []byte, value *MemValue) error
	// Del - delete keydir value by key
	Del(key []byte) error
}

func checkKey(k []byte) error {
	if k == nil {
		return ErrKeyIsNil
	}
	return nil
}
