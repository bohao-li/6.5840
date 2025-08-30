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
	uid   string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, name: l, state: LockStateUnlocked, uid: kvtest.RandValue(8)}

	// Initialize the lock state in the key-value store
	lk.ck.Put(l, lk.state, 0)

	return lk
}

func (lk *Lock) Acquire() {
	for {
		status, version, err := lk.ck.Get(lk.name)
		if !(err == rpc.OK) {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if status == LockStateUnlocked {
			putErr := lk.ck.Put(lk.name, fmt.Sprintf("%s:%s", LockStateLocked, lk.uid), version)
			if putErr == rpc.OK {
				lk.state = LockStateLocked
				fmt.Printf("Lock acquired: %s, version: %d\n", lk.name, version)
				break
			}
		} else if status == fmt.Sprintf("%s:%s", LockStateLocked, lk.uid) {
			break
		}

		// print the results from lk.ck.Get
		fmt.Printf("Lock %s status: %s, version: %d, error: %v\n", lk.name, status, version, err)
		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Get the lock status from kv cache
	for {
		status, version, err := lk.ck.Get(lk.name)
		if err == rpc.OK {
			if len(status) >= len(LockStateLocked) && status[:len(LockStateLocked)] == LockStateLocked {
				putErr := lk.ck.Put(lk.name, LockStateUnlocked, version)
				if putErr == rpc.OK {
					lk.state = LockStateUnlocked
					fmt.Printf("Lock released: %s, version: %d\n", lk.name, version)
					return
				}
			} else if status == LockStateUnlocked {
				return
			}
		}
		fmt.Printf("Error releasing lock %s: %v, version: %d, status: %s, retrying...\n", lk.name, err, version, status)
		time.Sleep(100 * time.Millisecond)
	}
}
