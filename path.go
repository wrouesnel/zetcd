// Copyright 2016 CoreOS, Inc.
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

package zetcd

import (
	//"path"
	"fmt"
	"strings"
	"strconv"
	"github.com/golang/glog"
)

const (
	ListKeyPrefix string = "/zk/ver/"
)

func mkPath(zkPath string) string {
	//p := path.Clean(zkPath)
	p := zkPath
	if p[0] != '/' {
		p = "/" + p
	}
	depth := 0
	for i := 0; i < len(p); i++ {
		if p[i] == '/' {
			depth++
		}
	}

	path := fmt.Sprintf("%2.2X%s",byte(depth), p)

	glog.V(10).Infof("mkPath(%v) = %v", zkPath, path)
	return path
}

func incPath(zetcdPath string) string {
	b := []byte(zetcdPath)
	b[0]++
	return string(b)
}

//func getListPfx(p string) string {
//	var listPfx string
//
//	if len(p) != 3 {
//		// Decode the hex byte
//		splitStrings := strings.Split(p, "/")
//		depth, _ := strconv.ParseUint(splitStrings[0], 16, 8)
//
//		searchP := fmt.Sprintf("%2.2X%s",byte(depth), p[2:])
//
//		listPfx = ListKeyPrefix + searchP + "/"
//	} else {
//		listPfx = ListKeyPrefix + p
//	}
//
//	glog.V(10).Infof("getListPfx(%v) = %v", p, listPfx)
//	return listPfx
//}

// getListPfx : this returns the prefix key in etcd neede to list subkeys of a
// a key. *As a result* - it decodes the num-children component of the path,
// increments it by 1 (because the actual child keys are one level down) and
// returns that.
func getListPfx(p string) string {
	var listPfx string

	if len(p) != 3 {
		// Decode the hex byte
		splitStrings := strings.Split(p, "/")
		depth, _ := strconv.ParseUint(splitStrings[0], 16, 8)
		// Increment the depth to return the right listing path for the children
		searchP := fmt.Sprintf("%2.2X%s/",byte(depth + 1), p[2:])

		listPfx = ListKeyPrefix + searchP
	} else {
		listPfx = ListKeyPrefix + p
	}

	glog.V(10).Infof("getListPfx(%v) = %v", p, listPfx)
	return listPfx
}

func getWatchPfx(p string) string {
	var listPfx string

	if len(p) != 3 {
		// Decode the hex byte
		splitStrings := strings.Split(p, "/")
		depth, _ := strconv.ParseUint(splitStrings[0], 16, 8)
		// Increment the depth to return the right listing path for the children
		searchP := fmt.Sprintf("%2.2X%s/",byte(depth + 1), p[2:])

		listPfx = ListKeyPrefix + searchP
	} else {
		listPfx = ListKeyPrefix + p
	}

	glog.V(10).Infof("getWatchPfx(%v) = %v", p, listPfx)
	return listPfx
}