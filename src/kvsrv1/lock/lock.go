package lock

import (
	"fmt"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

// define lock state constants
const (
	LockStateLocked   = "locked"
	LockStateUnlocked = "unlocked"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk

	// store lock state, name and version
	state string
	name  string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, name: l, state: LockStateUnlocked}

	// Initialize the lock state in the key-value store
	lk.ck.Put(l, lk.state, 0)

	return lk
}

func (lk *Lock) Acquire() {
	// Get the lock status from kv cache
	status, version, err := lk.ck.Get(lk.name)

	if err == rpc.OK && status == LockStateUnlocked {
		// Lock is available, acquire it
		putErr := lk.ck.Put(lk.name, LockStateLocked, version)
		if putErr == rpc.OK {
			lk.state = LockStateLocked
			return
		}
	}

	for {
		time.Sleep(10 * time.Millisecond)
		status, version, err = lk.ck.Get(lk.name)

		if err == rpc.OK && status == LockStateUnlocked {
			putErr := lk.ck.Put(lk.name, LockStateLocked, version)
			if putErr == rpc.OK {
				lk.state = LockStateLocked
				break
			}
		}
	}
}

func (lk *Lock) Release() {
	// Get the lock status from kv cache
	status, version, err := lk.ck.Get(lk.name)
	if err == rpc.OK {
		if status == LockStateLocked {
			// Release the lock
			lk.state = LockStateUnlocked
			lk.ck.Put(lk.name, lk.state, version)
		}
	} else {
		fmt.Printf("Error releasing lock: %v\n", err)
	}
}
