package runtime

import (
	"github.com/BornToBuildGame/ultimate-game-server/internal/fleet"
)

func (i *goInitializer) RegisterFleetManager(fm FleetManager) error {
	if fm == nil || i.nk == nil {
		return nil
	}
	if init, ok := fm.(fleet.Initializer); ok {
		handler := fleet.NewLocalFmCallbackHandler()
		_ = init.Init(i.nk, handler)
	}
	if grm, ok := i.nk.(*GoRuntimeModule); ok {
		grm.SetFleetManager(fm)
	}
	return nil
}

func (i *goInitializer) RegisterPurchaseNotificationApple(fn PurchaseNotificationAppleHandler) error {
	i.registry.RegisterPurchaseNotificationApple(fn)
	return nil
}
func (i *goInitializer) RegisterPurchaseNotificationGoogle(fn PurchaseNotificationGoogleHandler) error {
	i.registry.RegisterPurchaseNotificationGoogle(fn)
	return nil
}
func (i *goInitializer) RegisterSubscriptionNotificationApple(fn SubscriptionNotificationAppleHandler) error {
	i.registry.RegisterSubscriptionNotificationApple(fn)
	return nil
}
func (i *goInitializer) RegisterSubscriptionNotificationGoogle(fn SubscriptionNotificationGoogleHandler) error {
	i.registry.RegisterSubscriptionNotificationGoogle(fn)
	return nil
}
