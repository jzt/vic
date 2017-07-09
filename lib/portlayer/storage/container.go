// Copyright 2017 VMware, Inc. All Rights Reserved.
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
	"errors"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/types"
	"github.com/vmware/vic/lib/archive"
	"github.com/vmware/vic/pkg/trace"
	"github.com/vmware/vic/pkg/vsphere/datastore"
	"github.com/vmware/vic/pkg/vsphere/disk"
	"github.com/vmware/vic/pkg/vsphere/session"
	"github.com/vmware/vic/pkg/vsphere/vm"
)

// TODO(jzt): move this to a more appropriate location
type vmdk struct {
	dm *disk.Manager
	ds *datastore.Helper
	s  *session.Session
}

func (v *vmdk) Mount(op trace.Operation, uri *url.URL) (string, error) {
	if uri.Scheme != "ds" {
		return "", errors.New("vmdk path must be a datastore url with \"ds\" scheme")
	}

	dsPath, _ := datastore.PathFromString(uri.Path)
	return v.dm.AttachAndMount(op, dsPath)
}

func (v *vmdk) LockedVMDKFilter(vm *mo.VirtualMachine) bool {
	return vm.Runtime.PowerState == types.VirtualMachinePowerStatePoweredOn
}

// MountDataSource implements the DataSource interface for mounted devices
type MountDataSource struct {
	path *os.File
}

// Source returns the data source associated with the DataSource
func (m *MountDataSource) Source() interface{} {
	return m.path
}

// Import writes `data` to the data source associated with this DataSource
func (m *MountDataSource) Import(op trace.Operation, spec *archive.FilterSpec, data io.ReadCloser) error {
	return errors.New("Not implemented")
}

// Export reads data from the associated data source and returns it as a tar archive
func (m *MountDataSource) Export(op trace.Operation, spec *archive.FilterSpec, data bool) (io.ReadCloser, error) {
	fi, err := m.path.Stat()
	if err != nil {
		return nil, err
	}

	if !fi.IsDir() {
		return nil, errors.New("Path must be a directory")
	}
	return archive.Diff(op, m.path.Name(), "", spec, data)
}

// ContainerStore stores container storage information
type ContainerStore struct {
	vmdk
}

// NewContainerStore creates and returns a new container store
func NewContainerStore(op trace.Operation, s *session.Session) (*ContainerStore, error) {
	dm, err := disk.NewDiskManager(op, s, Config.ContainerView)
	if err != nil {
		return nil, err
	}

	cs := &ContainerStore{
		vmdk: vmdk{
			dm: dm,
			//ds: ds,
			s: s,
		},
	}
	return cs, nil
}

// URL converts the id of a resource to a URL
func (c *ContainerStore) URL(op trace.Operation, id string) (*url.URL, error) {
	dsPath, err := c.dm.DiskFinder(op, func(filename string) bool {
		return strings.HasSuffix(filename, id+".vmdk")
	})
	if err != nil {
		return nil, err
	}

	return &url.URL{
		Scheme: "ds",
		Path:   dsPath,
	}, nil
}

// Owners returns a list of VMs that are using the resource specified by `url`
func (c *ContainerStore) Owners(op trace.Operation, url *url.URL, filter func(vm *mo.VirtualMachine) bool) ([]*vm.VirtualMachine, error) {
	if url.Scheme != "ds" {
		return nil, errors.New("vmdk path must be a datastore url with \"ds\" scheme")
	}

	dsPath, _ := datastore.PathFromString(url.Path)
	config := disk.NewPersistentDisk(dsPath)

	return c.dm.InUse(op, config, c.LockedVMDKFilter)
}

// NewDataSource creates and returns an DataSource associated with container storage
func (c *ContainerStore) NewDataSource(op trace.Operation, id string) (*MountDataSource, error) {
	uri, _ := c.URL(op, id)
	mountPath, err := c.Mount(op, uri)
	if err == nil {
		return c.newDataSource(mountPath)
	}

	// check for vmdk locked error here
	// if not, something's wrong
	// if so, its locked and we need to find the owners below

	// online - Owners() should filter out the appliance VM
	owners, _ := c.Owners(op, uri, c.LockedVMDKFilter)
	if len(owners) == 0 {
		return nil, errors.New("Unavailable")
	}

	// TODO(jzt): tweak this when online export is available
	for _, o := range owners {
		// o is a VM
		_, _ = c.newOnlineDataSource(o)
		// if a != nil && a.available() {
		// 	return a, nil
		// }
	}

	return nil, errors.New("Unavailable")
}

func (c *ContainerStore) newDataSource(mountPath string) (*MountDataSource, error) {
	f, err := os.Open(mountPath)
	if err != nil {
		return nil, err
	}

	return &MountDataSource{
		path: f,
	}, nil
}

func (c *ContainerStore) newOnlineDataSource(vm *vm.VirtualMachine) (DataSource, error) {
	return nil, nil
}
