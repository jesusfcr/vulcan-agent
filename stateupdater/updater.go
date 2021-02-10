package stateupdater

import "encoding/json"

const (
	StatusCreated      = "CREATED"
	StatusQueued       = "QUEUED"
	StatusAssigned     = "ASSIGNED"
	StatusRunning      = "RUNNING"
	StatusTimeout      = "TIMEOUT"
	StatusAborted      = "ABORTED"
	StatusPurging      = "PURGING"
	StatusKilled       = "KILLED"
	StatusFailed       = "FAILED"
	StatusFinished     = "FINISHED"
	StatusMalformed    = "MALFORMED"
	StatusInconclusive = "INCONCLUSIVE"
)

// CheckState defines the all the possible fields of the states
// sent to the check state queue.
type CheckState struct {
	ID       string   `json:"id" validate:"required"`
	Status   *string  `json:"status" validate:"required"`
	AgentID  *string  `json:"agent_id" validate:"required"`
	Report   *string  `json:"report,omitempty"`
	Raw      *string  `json:"raw,omitempty"`
	Progress *float32 `json:"progress,omitempty"`
}

type QueueWriter interface {
	Write(body string) error
}

type Updater struct {
	qw QueueWriter
}

func New(qw QueueWriter) *Updater {
	return &Updater{qw}
}

func (u *Updater) UpdateState(s CheckState) error {
	body, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return u.qw.Write(string(body))
}
