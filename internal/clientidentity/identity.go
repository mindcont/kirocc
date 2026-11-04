package clientidentity

import "context"

type contextKey struct{}

// WithDeviceID associates an authenticated client device ID with a request.
func WithDeviceID(ctx context.Context, deviceID string) context.Context {
	return context.WithValue(ctx, contextKey{}, deviceID)
}

// DeviceID returns the authenticated device ID attached to a request.
func DeviceID(ctx context.Context) string {
	value, _ := ctx.Value(contextKey{}).(string)
	return value
}
