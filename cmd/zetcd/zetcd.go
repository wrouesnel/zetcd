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
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"os"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/transport"
	"github.com/coreos/zetcd"
	"github.com/coreos/zetcd/xchk"
	"github.com/coreos/zetcd/zk"
	"golang.org/x/net/context"
)

type personality struct {
	authf zetcd.AuthFunc
	zkf   zetcd.ZKFunc
	ctx   context.Context
}

func getTlsConfig(etcdCertFile string, etcdKeyFile string, etcdCaFile string) (*tls.Config, error) {
	tlsInfo := transport.TLSInfo{
		CertFile:      etcdCertFile,
		KeyFile:       etcdKeyFile,
		TrustedCAFile: etcdCaFile,
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("ERROR: %s", err)
	}
	return tlsConfig, nil
}

func newZKSecureEtcd(etcdAddr string, tlsConfig *tls.Config) (p personality) {
	// talk to the etcd3 server
	cfg := etcd.Config{Endpoints: []string{etcdAddr}, TLS: tlsConfig}
	c, err := etcd.New(cfg)
	if err != nil {
		panic(err)
	}
	p.authf = zetcd.NewAuth(c)
	p.zkf = zetcd.NewZK(c)
	p.ctx = c.Ctx()
	return p
}

func newZKEtcd(etcdAddr string) (p personality) {
	// talk to the etcd3 server
	cfg := etcd.Config{Endpoints: []string{etcdAddr}}
	c, err := etcd.New(cfg)
	if err != nil {
		panic(err)
	}
	p.authf = zetcd.NewAuth(c)
	p.zkf = zetcd.NewZK(c)
	p.ctx = c.Ctx()
	return p
}

func newBridge(bridgeAddr string) (p personality) {
	// proxy to zk server
	p.authf = zk.NewAuth([]string{bridgeAddr})
	p.zkf = zk.NewZK()
	p.ctx = context.Background()
	return p
}

func newOracle(etcdAddr, bridgeAddr, oracle string) (p personality) {
	var cper, oper personality
	switch oracle {
	case "zk":
		cper, oper = newZKEtcd(etcdAddr), newBridge(bridgeAddr)
	case "etcd":
		oper, cper = newZKEtcd(etcdAddr), newBridge(bridgeAddr)
	default:
		fmt.Println("oracle expected etcd or zk, got", oracle)
		os.Exit(1)
	}
	p.authf = xchk.NewAuth(cper.authf, oper.authf)
	p.zkf = xchk.NewZK(cper.zkf, oper.zkf)
	p.ctx = cper.ctx
	return p
}

func main() {
	etcdAddr := flag.String("endpoint", "", "etcd3 client address")
	etcdCertFile := flag.String("certfile", "", "etcd3 cert file")
	etcdKeyFile := flag.String("keyfile", "", "etcd3 key file")
	etcdCaFile := flag.String("cafile", "", "etcd3 ca file")
	zkaddr := flag.String("zkaddr", "", "address for serving zookeeper clients")
	oracle := flag.String("oracle", "", "oracle zookeeper server address")
	bridgeAddr := flag.String("zkbridge", "", "bridge zookeeper server address")

	flag.Parse()
	fmt.Println("Running zetcd proxy")

	if len(*zkaddr) == 0 {
		fmt.Println("expected -zkaddr")
		os.Exit(1)
	}

	// listen on zookeeper server port
	ln, err := net.Listen("tcp", *zkaddr)
	if err != nil {
		os.Exit(1)
	}

	var p personality
	serv := zetcd.Serve
	switch {
	case *oracle != "":
		if len(*etcdAddr) == 0 || len(*bridgeAddr) == 0 {
			fmt.Println("expected -endpoint and -zkbridge")
			os.Exit(1)
		}
		p = newOracle(*etcdAddr, *bridgeAddr, *oracle)
		serv = zetcd.ServeSerial
	case len(*etcdAddr) != 0 && len(*bridgeAddr) != 0:
		fmt.Println("expected -endpoint or -zkbridge but not both")
		os.Exit(1)
	case len(*etcdAddr) != 0:
		if len(*etcdCertFile) != 0 && len(*etcdKeyFile) != 0 && len(*etcdCaFile) != 0 {
			tlsConfig, _ := getTlsConfig(*etcdCertFile, *etcdKeyFile, *etcdCaFile)
			p = newZKSecureEtcd(*etcdAddr, tlsConfig)
		} else {
			p = newZKEtcd(*etcdAddr)
		}
	case len(*bridgeAddr) != 0:
		p = newBridge(*bridgeAddr)
	default:
		fmt.Println("expected -endpoint or -zkbridge")
		os.Exit(1)
	}

	serv(p.ctx, ln, p.authf, p.zkf)
}
