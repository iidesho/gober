package saga

import (
	"io"
	"time"

	"github.com/gofrs/uuid"
	"github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/bcts"
	"github.com/iidesho/gober/consensus/contenious"
)

var log = sbragi.WithLocalScope(sbragi.LevelInfo)

type Action[BT any, T bcts.ReadWriter[BT]] struct {
	Handler  Handler[BT, T]
	cons     contenious.Consensus
	aborted  <-chan contenious.ConsID
	approved <-chan contenious.ConsID
	Id       string
	Type     string
}

type Story[BT any, T bcts.ReadWriter[BT]] struct {
	Name    string
	Actions []Action[BT, T]
}

func (a *Action[BT, T]) ReadBytes(r io.Reader) error {
	err := bcts.ReadSmallString(r, &a.Id)
	if err != nil {
		return err
	}
	err = bcts.ReadSmallString(r, &a.Type)
	if err != nil {
		return err
	}
	return nil
}

func (a Action[BT, T]) WriteBytes(w io.Writer) error {
	err := bcts.WriteSmallString(w, a.Id)
	if err != nil {
		return err
	}
	err = bcts.WriteSmallString(w, a.Type)
	if err != nil {
		return err
	}
	return nil
}

type sagaValue[BT bcts.Writer, T bcts.ReadWriter[BT]] struct {
	v      BT
	status status
}

func (s *sagaValue[BT, T]) ReadBytes(r io.Reader) error {
	err := s.status.ReadBytes(r)
	if err != nil {
		return err
	}
	s.v, err = bcts.ReadReader[BT, T](r)
	return err
}

func (s sagaValue[BT, T]) WriteBytes(w io.Writer) error {
	err := s.status.WriteBytes(w)
	if err != nil {
		return err
	}
	return s.v.WriteBytes(w)
}

type status struct {
	stepDone  string
	retryFrom string
	duration  time.Duration
	state     State
	err       error
	consID    contenious.ConsID
	id        uuid.UUID
}

func (s status) WriteBytes(w io.Writer) error {
	log.Trace("writing", "s", s)
	err := bcts.WriteStaticBytes(w, s.id[:])
	if err != nil {
		return err
	}
	err = bcts.WriteStaticBytes(w, s.consID[:])
	if err != nil {
		return err
	}
	err = bcts.WriteTinyString(w, s.stepDone)
	if err != nil {
		return err
	}
	err = bcts.WriteTinyString(w, s.retryFrom)
	if err != nil {
		return err
	}
	err = bcts.WriteInt64(w, s.duration)
	if err != nil {
		return err
	}
	err = s.state.WriteBytes(w)
	if err != nil {
		return err
	}
	return nil
}

func (s *status) ReadBytes(r io.Reader) error {
	err := bcts.ReadStaticBytes(r, s.id[:])
	if err != nil {
		return err
	}
	err = bcts.ReadStaticBytes(r, s.consID[:])
	if err != nil {
		return err
	}
	err = bcts.ReadTinyString(r, &s.stepDone)
	if err != nil {
		return err
	}
	err = bcts.ReadTinyString(r, &s.retryFrom)
	if err != nil {
		return err
	}
	err = bcts.ReadInt64(r, &s.duration)
	if err != nil {
		return err
	}
	err = s.state.ReadBytes(r)
	if err != nil {
		return err
	}
	return nil
}

type State uint8

const (
	StateInvalid State = iota
	StatePending
	StateWorking
	StateSuccess
	StateRetryable
	StateFailed
	StatePaniced
	StateSagaFailed
	StateSagaSucceeded
)

func (s State) String() string {
	switch s {
	case StatePending:
		return "pending"
	case StateWorking:
		return "working"
	case StateSuccess:
		return "success"
	case StateFailed:
		return "failed"
	case StateRetryable:
		return "retryable"
	case StatePaniced:
		return "paniced"
	default:
		return "invalid"
	}
}

func (s State) WriteBytes(w io.Writer) error {
	log.Trace("writing", "s", s)
	return bcts.WriteUInt8(w, s)
}

func (s *State) ReadBytes(r io.Reader) error {
	return bcts.ReadUInt8(r, s)
}

type states []State

func (s states) WriteBytes(w io.Writer) error {
	log.Trace("writing", "s", s)
	err := bcts.WriteUInt32(w, uint32(len(s)))
	if err != nil {
		return err
	}
	for _, s := range s {
		err = s.WriteBytes(w)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *states) ReadBytes(r io.Reader) error {
	var l uint32
	err := bcts.ReadUInt32(r, &l)
	if err != nil {
		return err
	}
	*s = make(states, l)
	for i := range l {
		err = (*s)[i].ReadBytes(r)
		if err != nil {
			return err
		}
	}
	return nil
}
