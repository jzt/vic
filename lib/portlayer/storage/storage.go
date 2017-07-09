// Copyright 2016 VMware, Inc. All Rights Reserved.
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

package storage

import (
	"context"
	"io"
	"net/url"
	"sync"

	log "github.com/Sirupsen/logrus"

	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/view"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/vic/lib/archive"
	"github.com/vmware/vic/pkg/trace"
	"github.com/vmware/vic/pkg/vsphere/extraconfig"
	"github.com/vmware/vic/pkg/vsphere/session"
)

var once sync.Once

func create(ctx context.Context, session *session.Session, pool *object.ResourcePool) error {
	var err error

	mngr := view.NewManager(session.Vim25())

	// Create view of VirtualMachine objects under the VCH's resource pool
	Config.ContainerView, err = mngr.CreateContainerView(ctx, pool.Reference(), []string{"VirtualMachine"}, true)
	if err != nil {
		return err
	}
	return nil
}

// Init intializes the storage layer configuration
func Init(ctx context.Context, session *session.Session, pool *object.ResourcePool, source extraconfig.DataSource, _ extraconfig.DataSink) error {
	defer trace.End(trace.Begin(""))

	var err error

	once.Do(func() {
		// Grab the storage layer config blobs from extra config
		extraconfig.Decode(source, &Config)
		log.Debugf("Decoded VCH config for storage: %#v", Config)

		err = create(ctx, session, pool)
	})
	return err
}

// Resolver defines methods for mapping ids to URLS, and urls to owners of that device
type Resolver interface {
	// URL returns a url to the data source representing `id`
	URL(op trace.Operation, id string) (*url.URL, error)
	// Owners returns a list of VMs that are using the resource specified by `url`
	Owners(op trace.Operation, url *url.URL, filter func(vm *mo.VirtualMachine) bool) ([]*mo.VirtualMachine, error)
}

// Store defines the methods that a store can perform
type Store interface {
	NewDataSource(id string) (*DataSource, error)
	Resolver
}

// DataSource defines the methods for importing and exporting data to/from a data source
type DataSource interface {
	// Import writes `data` to the data source associated with this Archiver
	Import(op trace.Operation, filterspec *archive.FilterSpec, data io.ReadCloser) error
	// Export reads data from the associated data source and returns it as a tar archive
	Export(op trace.Operation, filterspec *archive.FilterSpec, data bool) (io.ReadCloser, error)
	// Source returns the mechanism by which the data source is accessed
	// Examples:
	//     vmdk mounted locally: *os.File
	//     nfs volume:  XDR-client
	//     via guesttools:  tar stream
	Source() interface{}
}
