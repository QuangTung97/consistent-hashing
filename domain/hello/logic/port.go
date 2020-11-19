package logic

import "sharding/domain/hello"

type Port struct {
}

var _ hello.Port = &Port{}

func NewPort() *Port {
	return &Port{}
}
