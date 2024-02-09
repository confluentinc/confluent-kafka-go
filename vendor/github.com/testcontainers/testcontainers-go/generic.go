package testcontainers

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrReuseEmptyName = errors.New("with reuse option a container name mustn't be empty")
)

// GenericContainerRequest represents parameters to a generic container
type GenericContainerRequest struct {
	ContainerRequest              // embedded request for provider
	Started          bool         // whether to auto-start the container
	ProviderType     ProviderType // which provider to use, Docker if empty
	Logger           Logging      // provide a container specific Logging - use default global logger if empty
	Reuse            bool         // reuse an existing container if it exists or create a new one. a container name mustn't be empty
}

// GenericNetworkRequest represents parameters to a generic network
type GenericNetworkRequest struct {
	NetworkRequest              // embedded request for provider
	ProviderType   ProviderType // which provider to use, Docker if empty
}

// GenericNetwork creates a generic network with parameters
func GenericNetwork(ctx context.Context, req GenericNetworkRequest) (Network, error) {
	provider, err := req.ProviderType.GetProvider()
	if err != nil {
		return nil, err
	}
	network, err := provider.CreateNetwork(ctx, req.NetworkRequest)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to create network", err)
	}

	return network, nil
}

// GenericContainer creates a generic container with parameters
func GenericContainer(ctx context.Context, req GenericContainerRequest) (Container, error) {
	if req.Reuse && req.Name == "" {
		return nil, ErrReuseEmptyName
	}

	logging := req.Logger
	if logging == nil {
		logging = Logger
	}
	provider, err := req.ProviderType.GetProvider(WithLogger(logging))
	if err != nil {
		return nil, err
	}

	var c Container
	if req.Reuse {
		c, err = provider.ReuseOrCreateContainer(ctx, req.ContainerRequest)
	} else {
		c, err = provider.CreateContainer(ctx, req.ContainerRequest)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: failed to create container", err)
	}

	if req.Started && !c.IsRunning() {
		if err := c.Start(ctx); err != nil {
			return c, fmt.Errorf("%w: failed to start container", err)
		}
	}
	return c, nil
}

// GenericProvider represents an abstraction for container and network providers
type GenericProvider interface {
	ContainerProvider
	NetworkProvider
}
