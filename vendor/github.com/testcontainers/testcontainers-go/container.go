package testcontainers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/pkg/archive"
	"github.com/docker/go-connections/nat"

	"github.com/testcontainers/testcontainers-go/wait"
)

// DeprecatedContainer shows methods that were supported before, but are now deprecated
// Deprecated: Use Container
type DeprecatedContainer interface {
	GetHostEndpoint(ctx context.Context, port string) (string, string, error)
	GetIPAddress(ctx context.Context) (string, error)
	LivenessCheckPorts(ctx context.Context) (nat.PortSet, error)
	Terminate(ctx context.Context) error
}

// ContainerProvider allows the creation of containers on an arbitrary system
type ContainerProvider interface {
	CreateContainer(context.Context, ContainerRequest) (Container, error)        // create a container without starting it
	ReuseOrCreateContainer(context.Context, ContainerRequest) (Container, error) // reuses a container if it exists or creates a container without starting
	RunContainer(context.Context, ContainerRequest) (Container, error)           // create a container and start it
	Health(context.Context) error
	Config() TestContainersConfig
}

// Container allows getting info about and controlling a single container instance
type Container interface {
	GetContainerID() string                                         // get the container id from the provider
	Endpoint(context.Context, string) (string, error)               // get proto://ip:port string for the first exposed port
	PortEndpoint(context.Context, nat.Port, string) (string, error) // get proto://ip:port string for the given exposed port
	Host(context.Context) (string, error)                           // get host where the container port is exposed
	MappedPort(context.Context, nat.Port) (nat.Port, error)         // get externally mapped port for a container port
	Ports(context.Context) (nat.PortMap, error)                     // get all exposed ports
	SessionID() string                                              // get session id
	IsRunning() bool
	Start(context.Context) error                 // start the container
	Stop(context.Context, *time.Duration) error  // stop the container
	Terminate(context.Context) error             // terminate the container
	Logs(context.Context) (io.ReadCloser, error) // Get logs of the container
	FollowOutput(LogConsumer)
	StartLogProducer(context.Context) error
	StopLogProducer() error
	Name(context.Context) (string, error)                        // get container name
	State(context.Context) (*types.ContainerState, error)        // returns container's running state
	Networks(context.Context) ([]string, error)                  // get container networks
	NetworkAliases(context.Context) (map[string][]string, error) // get container network aliases for a network
	Exec(ctx context.Context, cmd []string) (int, io.Reader, error)
	ContainerIP(context.Context) (string, error) // get container ip
	CopyToContainer(ctx context.Context, fileContent []byte, containerFilePath string, fileMode int64) error
	CopyDirToContainer(ctx context.Context, hostDirPath string, containerParentPath string, fileMode int64) error
	CopyFileToContainer(ctx context.Context, hostFilePath string, containerFilePath string, fileMode int64) error
	CopyFileFromContainer(ctx context.Context, filePath string) (io.ReadCloser, error)
}

// ImageBuildInfo defines what is needed to build an image
type ImageBuildInfo interface {
	GetContext() (io.Reader, error)   // the path to the build context
	GetDockerfile() string            // the relative path to the Dockerfile, including the fileitself
	ShouldPrintBuildLog() bool        // allow build log to be printed to stdout
	ShouldBuildImage() bool           // return true if the image needs to be built
	GetBuildArgs() map[string]*string // return the environment args used to build the from Dockerfile
}

// FromDockerfile represents the parameters needed to build an image from a Dockerfile
// rather than using a pre-built one
type FromDockerfile struct {
	Context        string             // the path to the context of of the docker build
	ContextArchive io.Reader          // the tar archive file to send to docker that contains the build context
	Dockerfile     string             // the path from the context to the Dockerfile for the image, defaults to "Dockerfile"
	BuildArgs      map[string]*string // enable user to pass build args to docker daemon
	PrintBuildLog  bool               // enable user to print build log
}

type ContainerFile struct {
	HostFilePath      string
	ContainerFilePath string
	FileMode          int64
}

// ContainerRequest represents the parameters used to get a running container
type ContainerRequest struct {
	FromDockerfile
	Image           string
	Entrypoint      []string
	Env             map[string]string
	ExposedPorts    []string // allow specifying protocol info
	Cmd             []string
	Labels          map[string]string
	Mounts          ContainerMounts
	Tmpfs           map[string]string
	RegistryCred    string
	WaitingFor      wait.Strategy
	Name            string // for specifying container name
	Hostname        string
	ExtraHosts      []string
	Privileged      bool                // for starting privileged container
	Networks        []string            // for specifying network names
	NetworkAliases  map[string][]string // for specifying network aliases
	NetworkMode     container.NetworkMode
	Resources       container.Resources
	Files           []ContainerFile // files which will be copied when container starts
	User            string          // for specifying uid:gid
	SkipReaper      bool            // indicates whether we skip setting up a reaper for this
	ReaperImage     string          // alternative reaper image
	AutoRemove      bool            // if set to true, the container will be removed from the host when stopped
	AlwaysPullImage bool            // Always pull image
	ImagePlatform   string          // ImagePlatform describes the platform which the image runs on.
	Binds           []string
	ShmSize         int64 // Amount of memory shared with the host (in bytes)
}

type (
	// ProviderType is an enum for the possible providers
	ProviderType int

	// GenericProviderOptions defines options applicable to all providers
	GenericProviderOptions struct {
		Logger         Logging
		DefaultNetwork string
	}

	// GenericProviderOption defines a common interface to modify GenericProviderOptions
	// These options can be passed to GetProvider in a variadic way to customize the returned GenericProvider instance
	GenericProviderOption interface {
		ApplyGenericTo(opts *GenericProviderOptions)
	}

	// GenericProviderOptionFunc is a shorthand to implement the GenericProviderOption interface
	GenericProviderOptionFunc func(opts *GenericProviderOptions)
)

func (f GenericProviderOptionFunc) ApplyGenericTo(opts *GenericProviderOptions) {
	f(opts)
}

// possible provider types
const (
	ProviderDocker ProviderType = iota // Docker is default = 0
	ProviderPodman
)

// GetProvider provides the provider implementation for a certain type
func (t ProviderType) GetProvider(opts ...GenericProviderOption) (GenericProvider, error) {
	opt := &GenericProviderOptions{
		Logger: Logger,
	}

	for _, o := range opts {
		o.ApplyGenericTo(opt)
	}

	switch t {
	case ProviderDocker:
		providerOptions := append(Generic2DockerOptions(opts...), WithDefaultBridgeNetwork(Bridge))
		provider, err := NewDockerProvider(providerOptions...)
		if err != nil {
			return nil, fmt.Errorf("%w, failed to create Docker provider", err)
		}
		return provider, nil
	case ProviderPodman:
		providerOptions := append(Generic2DockerOptions(opts...), WithDefaultBridgeNetwork(Podman))
		provider, err := NewDockerProvider(providerOptions...)
		if err != nil {
			return nil, fmt.Errorf("%w, failed to create Docker provider", err)
		}
		return provider, nil
	}
	return nil, errors.New("unknown provider")
}

// Validate ensures that the ContainerRequest does not have invalid parameters configured to it
// ex. make sure you are not specifying both an image as well as a context
func (c *ContainerRequest) Validate() error {
	validationMethods := []func() error{
		c.validateContextAndImage,
		c.validateContextOrImageIsSpecified,
		c.validateMounts,
	}

	var err error
	for _, validationMethod := range validationMethods {
		err = validationMethod()
		if err != nil {
			return err
		}
	}

	return nil
}

// GetContext retrieve the build context for the request
func (c *ContainerRequest) GetContext() (io.Reader, error) {
	if c.ContextArchive != nil {
		return c.ContextArchive, nil
	}

	buildContext, err := archive.TarWithOptions(c.Context, &archive.TarOptions{})
	if err != nil {
		return nil, err
	}

	return buildContext, nil
}

// GetBuildArgs returns the env args to be used when creating from Dockerfile
func (c *ContainerRequest) GetBuildArgs() map[string]*string {
	return c.FromDockerfile.BuildArgs
}

// GetDockerfile returns the Dockerfile from the ContainerRequest, defaults to "Dockerfile"
func (c *ContainerRequest) GetDockerfile() string {
	f := c.FromDockerfile.Dockerfile
	if f == "" {
		return "Dockerfile"
	}

	return f
}

func (c *ContainerRequest) ShouldBuildImage() bool {
	return c.FromDockerfile.Context != "" || c.FromDockerfile.ContextArchive != nil
}

func (c *ContainerRequest) ShouldPrintBuildLog() bool {
	return c.FromDockerfile.PrintBuildLog
}

func (c *ContainerRequest) validateContextAndImage() error {
	if c.FromDockerfile.Context != "" && c.Image != "" {
		return errors.New("you cannot specify both an Image and Context in a ContainerRequest")
	}

	return nil
}

func (c *ContainerRequest) validateContextOrImageIsSpecified() error {
	if c.FromDockerfile.Context == "" && c.FromDockerfile.ContextArchive == nil && c.Image == "" {
		return errors.New("you must specify either a build context or an image")
	}

	return nil
}

func (c *ContainerRequest) validateMounts() error {
	targets := make(map[string]bool, len(c.Mounts))

	for idx := range c.Mounts {
		m := c.Mounts[idx]
		targetPath := m.Target.Target()
		if targets[targetPath] {
			return fmt.Errorf("%w: %s", ErrDuplicateMountTarget, targetPath)
		} else {
			targets[targetPath] = true
		}
	}
	return nil
}
