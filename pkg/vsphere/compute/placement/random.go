// Copyright 2018 VMware, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package placement

import (
	"math/rand"

	"github.com/vmware/govmomi/object"
	"github.com/vmware/vic/pkg/trace"
	"github.com/vmware/vic/pkg/vsphere/compute"
	"github.com/vmware/vic/pkg/vsphere/session"
)

// RandomHostPolicy chooses a random host on which to power-on a VM.
type RandomHostPolicy struct{}

// NewRandomHostPolicy returns a RandomHostPolicy instance.
func NewRandomHostPolicy() *RandomHostPolicy {
	return &RandomHostPolicy{}
}

// CheckHost always returns false in a RandomHostPolicy.
func (p *RandomHostPolicy) CheckHost(op trace.Operation, sess *session.Session) bool {
	return false
}

// RecommendHost recommends a random host on which to place a newly created VM.
func (p *RandomHostPolicy) RecommendHost(op trace.Operation, sess *session.Session, hosts []*object.HostSystem) ([]*object.HostSystem, error) {
	if hosts == nil {
		rp := compute.NewResourcePool(op, sess, sess.Pool.Reference())

		cls, err := rp.GetCluster(op)
		if err != nil {
			return nil, err
		}

		hosts, err = cls.Hosts(op)
		if err != nil {
			return nil, err
		}
	}

	// shuffle hosts
	for i := range hosts {
		j := rand.Intn(i + 1)
		hosts[i], hosts[j] = hosts[j], hosts[i]
	}

	return hosts, nil
}
