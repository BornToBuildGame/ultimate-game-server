package runtime

import (
	"ultimate-game-server/internal/fleet"
)

func (i *goInitializer) RegisterFleetManager(fm fleet.Manager) error {
	if fm == nil || i.nk == nil {
		return nil
	}
	if grm, ok := i.nk.(*GoRuntimeModule); ok {
		grm.SetFleetManager(fm)
	}
	return nil
}
