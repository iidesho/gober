package discovery

type Discoverer interface {
	Servers() []string
	Self() string
}
