//go:build !darwin

package resourcecontrol

func newPlatformProvider(config ProviderConfig) Provider {
	return config.Runtime
}
