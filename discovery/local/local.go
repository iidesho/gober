package local

import "github.com/iidesho/gober/discovery"

type local struct {
}

func New() discovery.Discoverer {
	return local{}
}

func (l local) Servers() []string {
	return []string{"localhost"}
}
