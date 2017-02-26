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

package main

import (
	"fmt"
	"os"
	"path"
	"sort"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"gopkg.in/alecthomas/kingpin.v2"
	"strings"
)

var (
	acl = zk.WorldACL(zk.PermAll)
)

type watchCmd struct {
	key string
}

func (c *watchCmd) run(pctx *kingpin.ParseContext) {

}

type lsCmd struct {
	key string
}

func (c *lsCmd) run(pctx *kingpin.ParseContext) {

}

type rmCmd struct {
	key string
}

func (c *rmCmd) run(pctx *kingpin.ParseContext) {

}

type setCmd struct {
	key   string
	value string
}

func (c *setCmd) run(pctx *kingpin.ParseContext) {

}

type getCmd struct {
	key string
}

func (c *getCmd) run(pctx *kingpin.ParseContext) {

}

type putCmd struct {
	key   string
	value string
	flag  int32
}

func (c *putCmd) run(pctx *kingpin.ParseContext) {

}

func main() {
	// User input
	server := new(string)
	key := new(string)
	value := new(string)
	keyFlags := new(int32)

	// Calculated state
	zkConn := new(*zk.Conn)
	zkConnectTimeout := new(time.Duration)

	app := kingpin.New("zkctl", "Zookeeper CLI tool")
	app.Flag("timeout", "timeout for connecting to the server (comma-separate for cluster)").Default("1s").DurationVar(zkConnectTimeout)
	app.Flag("zkaddr", "address of zookeeper server").Default("127.0.0.1:2181").StringVar(server)
	app.Action(func (pctx *kingpin.ParseContext) error {
		var err error
		serverList := strings.Split(*server, ",")
		*zkConn, _, err = zk.Connect(serverList, *zkConnectTimeout)
		return err
	})

	{
		cmd := app.Command("watch", "watch for changes on a key")
		cmd.Arg("key", "key to start watching").Default("/").StringVar(key)
		cmd.Action(func (pctx *kingpin.ParseContext) error {
			watch(*zkConn, *key)
			return nil
		})
	}

	{
		cmd := app.Command("ls", "list child keys")
		cmd.Arg("key", "key to start watching").Default("/").StringVar(key)
		cmd.Action(func (pctx *kingpin.ParseContext) error {
			return ls(*zkConn, *key)
		})
	}

	{
		cmd := app.Command("rm", "delete key")
		cmd.Arg("key", "key to start watching").StringVar(key)
		cmd.Action(func (pctx *kingpin.ParseContext) error {
			return rm(*zkConn, *key)
		})
	}

	{
		cmd := app.Command("get", "get key value")
		cmd.Arg("key", "key to start watching").StringVar(key)
		cmd.Action(func (pctx *kingpin.ParseContext) error {
			return get(*zkConn, *key)
		})
	}

	{
		cmd := app.Command("set", "set key value")
		cmd.Arg("key", "key to start watching").StringVar(key)
		cmd.Arg("value", "value to set").StringVar(value)
		cmd.Action(func (pctx *kingpin.ParseContext) error {
			return set(*zkConn, *key, *value)
		})
	}

	{
		cmd := app.Command("put", "put a new key")
		cmd.Arg("key", "key to start watching").StringVar(key)
		cmd.Arg("value", "value to set").StringVar(value)
		cmd.Action(func (pctx *kingpin.ParseContext) error {
			return put(*zkConn, *key, *value, *keyFlags)
		})
	}

	{
		cmd := app.Command("eput", "put an ephemeral key")
		cmd.Arg("key", "key to start watching").StringVar(key)
		cmd.Arg("value", "value to set").StringVar(value)
		cmd.PreAction(func (pctx *kingpin.ParseContext) error {
			*keyFlags = zk.FlagEphemeral
			return nil
		})

		cmd.Action(func (pctx *kingpin.ParseContext) error {
			return put(*zkConn, *key, *value, *keyFlags)
		})
	}

	{
		cmd := app.Command("sput", "put a sequenced key")
		cmd.Arg("key", "key to start watching").StringVar(key)
		cmd.Arg("value", "value to set").StringVar(value)

		cmd.PreAction(func (pctx *kingpin.ParseContext) error {
			*keyFlags = zk.FlagSequence
			return nil
		})

		cmd.Action(func (pctx *kingpin.ParseContext) error {
			return put(*zkConn, *key, *value, *keyFlags)
		})
	}

	kingpin.MustParse(app.Parse(os.Args[1:]))
}

func watch(c *zk.Conn, dir string) {
	fmt.Println("watch dir", dir)
	children, stat, ch, err := c.ChildrenW(dir)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v %+v\n", children, stat)
	e := <-ch
	fmt.Printf("%+v\n", e)
}

func ls(c *zk.Conn, dir string) error {
	fmt.Println("ls dir", dir)
	children, stat, err := c.Children(dir)
	if err != nil {
		return err
	}
	sort.Sort(sort.StringSlice(children))
	fmt.Println("Children:")
	for _, c := range children {
		fmt.Printf("%s (%s)\n", path.Clean(dir+"/"+c), c)
	}
	fmt.Printf("Stat: %+v\n", stat)
	return nil
}

func put(c *zk.Conn, path, data string, fl int32) error {
	// TODO: descriptive acls
	_, err := c.Create(path, []byte(data), fl, acl)
	return err
}

func set(c *zk.Conn, path, data string) error {
	_, err := c.Set(path, []byte(data), -1)
	return err
}

func rm(c *zk.Conn, path string) error {
	return c.Delete(path, -1)
}

func get(c *zk.Conn, path string) error {
	dat, st, err := c.Get(path)
	if err == nil {
		fmt.Println(dat)
		fmt.Printf("Stat:\n%+v\n", st)
	}
	return err
}
