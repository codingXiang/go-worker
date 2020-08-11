package main

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"time"
)

func main() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:32773", "localhost:32769", "localhost:32772"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Println("connect failed, err:", err)
		return
	}

	fmt.Println("connect succ")
	defer cli.Close()
	kv := clientv3.NewKV(cli)
	_, e := kv.Delete(context.TODO(), "/ops/conf/database_url")
	if e != nil {
		panic(e)
	}
	_, err = kv.Put(context.TODO(), "/ops/conf/database/url", "localhost")
	if err != nil {
		panic(err)
	}
	_, err = kv.Put(context.TODO(), "/ops/conf/database/port", "3306")
	if err != nil {
		panic(err)
	}

	rangeResp, err := kv.Get(context.TODO(), "/ops/conf/", clientv3.WithPrefix())

	for _, value := range rangeResp.Kvs {
		fmt.Println(string(value.Key), string(value.Value))
	}
}
