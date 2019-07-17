// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Modified by Alexander Lyon to use request-local contexts

package store

import (
	"context"
	"fmt"
	recipe "go.etcd.io/etcd/contrib/recipes"
	"strings"

	v3 "go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

// PriorityQueue implements a multi-reader, multi-writer distributed queue.
type PriorityQueue struct {
	client *v3.Client
	key    string
}

// NewPriorityQueue creates an etcd priority queue.
func NewPriorityQueue(client *v3.Client, key string) *PriorityQueue {
	return &PriorityQueue{client, key + "/"}
}

// Enqueue puts a value into a queue with a given priority.
func (q *PriorityQueue) Enqueue(ctx context.Context, val string, pr uint16) error {
	prefix := fmt.Sprintf("%s%05d", q.key, pr)
	return newSequentialKV(ctx, q.client, prefix, val)
}

// Dequeue returns Enqueue()'d items in FIFO order. If the
// queue is empty, Dequeue blocks until items are available.
func (q *PriorityQueue) Dequeue(ctx context.Context) (string, error) {
	resp, err := q.client.Get(ctx, q.key, v3.WithFirstKey()...)
	if err != nil {
		return "", err
	}

	kv, err := claimFirstKey(ctx, q.client, resp.Kvs)
	if err != nil {
		return "", err
	} else if kv != nil {
		return string(kv.Value), nil
	} else if resp.More {
		// missed some items, retry to read in more
		return q.Dequeue(ctx)
	}

	// nothing to dequeue; wait on items
	ev, err := recipe.WaitPrefixEvents(
		q.client,
		q.key,
		resp.Header.Revision,
		[]mvccpb.Event_EventType{mvccpb.PUT})
	if err != nil {
		return "", err
	}

	ok, err := deleteRevKey(ctx, q.client, string(ev.Kv.Key), ev.Kv.ModRevision)
	if err != nil {
		return "", err
	} else if !ok {
		return q.Dequeue(ctx)
	}
	return string(ev.Kv.Value), err
}

// newSequentialKV allocates a new sequential key <prefix>/nnnnn with a given
// prefix and value. Note: a bookkeeping node __<prefix> is also allocated.
func newSequentialKV(ctx context.Context, kv v3.KV, prefix, val string) error {
	resp, err := kv.Get(ctx, prefix, v3.WithLastKey()...)
	if err != nil {
		return err
	}

	// add 1 to last key, if any
	newSeqNum := 0
	if len(resp.Kvs) != 0 {
		fields := strings.Split(string(resp.Kvs[0].Key), "/")
		_, serr := fmt.Sscanf(fields[len(fields)-1], "%d", &newSeqNum)
		if serr != nil {
			return err
		}
		newSeqNum++
	}
	newKey := fmt.Sprintf("%s/%016d", prefix, newSeqNum)

	// base prefix key must be current (i.e., <=) with the server update;
	// the base key is important to avoid the following:
	// N1: LastKey() == 1, start txn.
	// N2: new Key 2, new Key 3, Delete Key 2
	// N1: txn succeeds allocating key 2 when it shouldn't
	baseKey := "__" + prefix

	// current revision might contain modification so +1
	cmp := v3.Compare(v3.ModRevision(baseKey), "<", resp.Header.Revision+1)
	reqPrefix := v3.OpPut(baseKey, "")
	reqnewKey := v3.OpPut(newKey, val)

	txn := kv.Txn(ctx)
	txnresp, err := txn.If(cmp).Then(reqPrefix, reqnewKey).Commit()
	if err != nil {
		return err
	}
	if !txnresp.Succeeded {
		return newSequentialKV(ctx, kv, prefix, val)
	}
	return nil
}

func claimFirstKey(ctx context.Context, kv v3.KV, kvs []*mvccpb.KeyValue) (*mvccpb.KeyValue, error) {
	for _, k := range kvs {
		ok, err := deleteRevKey(ctx, kv, string(k.Key), k.ModRevision)
		if err != nil {
			return nil, err
		} else if ok {
			return k, nil
		}
	}
	return nil, nil
}

// deleteRevKey deletes a key by revision, returning false if key is missing
func deleteRevKey(ctx context.Context, kv v3.KV, key string, rev int64) (bool, error) {
	cmp := v3.Compare(v3.ModRevision(key), "=", rev)
	req := v3.OpDelete(key)
	txnresp, err := kv.Txn(ctx).If(cmp).Then(req).Commit()
	if err != nil {
		return false, err
	} else if !txnresp.Succeeded {
		return false, nil
	}
	return true, nil
}
