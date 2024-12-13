package state_test

import (
	"fmt"
	"testing"

	"github.com/iidesho/gober/state"
)

type st struct {
	NumIPVS int
	NumGSLB int
}

type lowRiskST struct {
	*st
}

func (c lowRiskST) Done(d st) (bool, error) {
	if c.NumIPVS < d.NumIPVS {
		return false, nil
	}
	if c.NumGSLB < d.NumGSLB {
		return false, nil
	}
	return true, nil
}

func (c lowRiskST) Execute(d st) (state.State[st], error) {
	if c.NumIPVS < d.NumIPVS {
		c.NumIPVS++
	}
	if c.NumGSLB < d.NumGSLB {
		c.NumGSLB++
	}
	return c, nil
}

func (c lowRiskST) String() string {
	return fmt.Sprint(c.st)
}

type highRiskST struct {
	*st
}

func (c highRiskST) Done(d st) (bool, error) {
	if c.NumIPVS < d.NumIPVS {
		return false, nil
	}
	if c.NumGSLB < d.NumGSLB {
		return false, nil
	}
	return true, nil
}

const (
	ipvsRiskLimit = 3
	gslbRiskLimit = 1
)

func (c highRiskST) isHighRisk() bool {
	if c.NumGSLB >= gslbRiskLimit && c.NumIPVS >= ipvsRiskLimit {
		return false
	}
	return true
}

func (c highRiskST) Execute(d st) (state.State[st], error) {
	if c.NumIPVS < ipvsRiskLimit && c.NumIPVS < d.NumIPVS {
		c.NumIPVS++
		if c.isHighRisk() {
			return c, nil
		}
	}
	if c.NumGSLB < gslbRiskLimit && c.NumGSLB < d.NumGSLB {
		c.NumGSLB++
		if c.isHighRisk() {
			return c, nil
		}
	}
	return lowRiskST(c), nil
}

func (c highRiskST) String() string {
	return fmt.Sprint(c.st)
}

func TestState(t *testing.T) {
	d := st{
		NumIPVS: 10,
		NumGSLB: 2,
	}
	c := state.State[st](highRiskST{
		&st{
			NumIPVS: 1,
			NumGSLB: 0,
		},
	})
	fmt.Println(d, c)
	for done, _ := c.Done(d); !done; done, _ = c.Done(d) {
		c, _ = c.Execute(d)
		fmt.Println(c)
	}
}
