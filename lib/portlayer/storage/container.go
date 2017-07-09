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
	"net/url"
	"os"
	"strings"

	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/types"
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

func (v *vmdk) Mount(op trace.Operation, uri *url.URL) (string, func(), error) {
	if uri.Scheme != "ds" {
		return "", errors.New("vmdk path must be a datastore url with \"ds\" scheme")
	}

	dsPath, err := datastore.PathFromString(uri.Path)
	if err != nil {
		return nil, nil, err
	}

	cleanFunc := func() {
		if err := v.dm.UnmountAndDetach(op, dsPath); err != nil {
			op.Errorf("Error cleaning up disk: %s", err.Error())
		}
	}
	mountPath, err := v.dm.AttachAndMount(op, dsPath)
	return mountPath, cleanFunc, err
}

func (v *vmdk) LockedVMDKFilter(vm *mo.VirtualMachine) bool {
	return vm.Runtime.PowerState == types.VirtualMachinePowerStatePoweredOn
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
func (c *ContainerStore) NewDataSource(op trace.Operation, id string) (DataSource, error) {
	uri, err := c.URL(op, id)
	if err != nil {
		return nil, err
	}

	mountPath, cleanFunc, err := c.Mount(op, uri)
	if err == nil {
		return c.newDataSource(mountPath, cleanFunc)
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

func (c *ContainerStore) newDataSource(mountPath string, cleanFunc func()) (DataSource, error) {
	f, err := os.Open(mountPath)
	if err != nil {
		return nil, err
	}

	return &MountDataSource{
		Path:  f,
		Clean: cleanFunc,
	}, nil
}

func (c *ContainerStore) newOnlineDataSource(vm *vm.VirtualMachine) (DataSource, error) {
	return nil, nil
}
