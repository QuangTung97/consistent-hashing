package logic

import "sharding/domain/hello"

type Port struct {
}

var _ hello.IPort = &Port{}

func NewPort() *Port {
	return &Port{}
}
