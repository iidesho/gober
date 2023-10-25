package local

import "github.com/cantara/gober/discovery"

type local struct {
}

func New() discovery.Discoverer {
	return local{}
}

func (l local) Servers() []string {
	return []string{"localhost"}
}
