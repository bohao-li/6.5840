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
	state   string
	name    string
	version rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, name: l, state: LockStateUnlocked, version: 0}

	// Initialize the lock state in the key-value store
	lk.ck.Put(l, lk.state, lk.version)

	return lk
}

func (lk *Lock) Acquire() {
	// Get the lock status from kv cache
	status, version, err := lk.ck.Get(lk.name)
	if err == rpc.OK {
		if status == LockStateLocked {
			// wait until the lock is released, with 1s interval
			for status == LockStateLocked {
				time.Sleep(1 * time.Second)
				status, version, _ = lk.ck.Get(lk.name)
			}
		}
		// Lock is available, acquire it
		lk.state = LockStateLocked
		lk.version = version + 1
		lk.ck.Put(lk.name, lk.state, lk.version)
	} else {
		fmt.Printf("Error acquiring lock: %v\n", err)
	}
}

func (lk *Lock) Release() {
	// Get the lock status from kv cache
	status, version, err := lk.ck.Get(lk.name)
	if err == rpc.OK {
		if status == LockStateLocked {
			// Release the lock
			lk.state = LockStateUnlocked
			lk.version = version + 1
			lk.ck.Put(lk.name, lk.state, lk.version)
		}
	} else {
		fmt.Printf("Error releasing lock: %v\n", err)
	}
}
