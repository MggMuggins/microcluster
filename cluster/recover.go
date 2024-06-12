package cluster

import (
	"fmt"

	"github.com/canonical/go-dqlite"
	dqliteClient "github.com/canonical/go-dqlite/client"
)

type Member struct {
	// dqlite.NodeInfo fields
	DqliteID uint64 `json:"id" yaml:"id"`
	Address  string `json:"address" yaml:"address"`
	Role     string `json:"role" yaml:"role"`

	Name string `json:"name" yaml:"name"`
}

func (m Member) ToNodeInfo() (*dqlite.NodeInfo, error) {
	var role dqliteClient.NodeRole
	switch m.Role {
	case "voter":
		role = dqliteClient.Voter
	case "stand-by":
		role = dqliteClient.StandBy
	case "spare":
		role = dqliteClient.Spare
	default:
		return nil, fmt.Errorf("invalid dqlite role %q", m.Role)
	}

	return &dqlite.NodeInfo{
		ID:      m.DqliteID,
		Role:    role,
		Address: m.Address,
	}, nil
}
