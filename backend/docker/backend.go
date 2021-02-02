package docker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/adevinta/dockerutils"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/lestrrat-go/backoff"

	"github.com/adevinta/vulcan-agent/backend"
	"github.com/adevinta/vulcan-agent/config"
	"github.com/adevinta/vulcan-agent/log"
)

const (
	defaultDockerIfaceName = "docker0"
	abortTimeout           = 5 * time.Second //seconds
)

type DockerClient interface {
	Create(ctx context.Context, cfg dockerutils.RunConfig, name string) (contID string, err error)
	ContainerStop(ctx context.Context, containerID string, timeout *time.Duration) error
	ContainerRemove(ctx context.Context, containerID string, options types.ContainerRemoveOptions) error
	ContainerStart(ctx context.Context, containerID string, options types.ContainerStartOptions) error
	ContainerWait(ctx context.Context, containerID string) (int64, error)
	ContainerLogs(ctx context.Context, container string, options types.ContainerLogsOptions) (io.ReadCloser, error)
	Pull(ctx context.Context, imageRef string) error
}

type Backend struct {
	config    config.RegistryConfig
	agentAddr string
	checkVars backend.CheckVars
	log       log.Logger
	cli       DockerClient //DockerClient
}

func New(log log.Logger, cfg config.RegistryConfig, agentAddr string, vars backend.CheckVars) (*Backend, error) {
	envCli, err := client.NewEnvClient()
	if err != nil {
		return &Backend{}, err
	}

	cli := dockerutils.NewClient(envCli)
	if cfg.Server != "" {
		err = cli.Login(context.Background(), cfg.Server, cfg.User, cfg.Pass)
		if err != nil {
			return nil, err
		}
	}
	return &Backend{
		config:    cfg,
		agentAddr: agentAddr,
		log:       log,
		checkVars: vars,
		cli:       cli,
	}, nil
}

// Run starts executing a check as a local container and returns a channel that
// will contain the result of the execution when it finishes.
func (b *Backend) Run(ctx context.Context, params backend.RunParams) (<-chan backend.RunResult, error) {
	if b.config.Server != "" {
		err := b.pullWithBackoff(ctx, params.Image)
		if err != nil {
			return nil, err
		}
	}

	var res = make(chan backend.RunResult)
	go b.run(ctx, params, res)
	return res, nil
}

func (b *Backend) run(ctx context.Context, params backend.RunParams, res chan<- backend.RunResult) {
	cfg := b.getRunConfig(params)
	contID, err := b.cli.Create(ctx, cfg, params.CheckID)
	if err != nil {
		res <- backend.RunResult{Error: err}
		return
	}
	defer func() {
		removeOpts := types.ContainerRemoveOptions{Force: true}
		removeErr := b.cli.ContainerRemove(context.Background(), contID, removeOpts)
		if removeErr != nil {
			b.log.Errorf("error removing container %s: %v", params.CheckID, err)
		}
	}()
	err = b.cli.ContainerStart(
		ctx, contID, cfg.ContainerStartOptions,
	)
	if err != nil {
		err := fmt.Errorf("error starting container for check %s: %w", params.CheckID, err)
		res <- backend.RunResult{Error: err}
		return
	}
	_, err = b.cli.ContainerWait(ctx, contID)
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		err := fmt.Errorf("error running container for check %s: %w", params.CheckID, err)
		res <- backend.RunResult{Error: err}
		return
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		timeout := abortTimeout
		b.cli.ContainerStop(context.Background(), contID, &timeout)
		res <- backend.RunResult{Error: err}
		return
	}
	logOpts := types.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	}
	r, err := b.cli.ContainerLogs(ctx, contID, logOpts)
	if err != nil {
		err := fmt.Errorf("error getting logs for check %s: %w", params.CheckID, err)
		res <- backend.RunResult{Error: err}
		return
	}
	defer r.Close()
	out, err := readContainerLogs(r)
	if err != nil {
		err := fmt.Errorf("error reading logs for check %s: %w", params.CheckID, err)
		res <- backend.RunResult{Error: err}
		return
	}
	res <- backend.RunResult{Output: out}
}

func (b Backend) pullWithBackoff(ctx context.Context, image string) error {
	backoffPolicy := backoff.NewExponential(
		backoff.WithInterval(time.Duration(b.config.BackoffInterval*int(time.Second))),
		backoff.WithMaxRetries(b.config.BackoffMaxRetries),
		backoff.WithJitterFactor(b.config.BackoffJitterFactor),
	)
	bState, cancel := backoffPolicy.Start(context.Background())
	defer cancel()

	for backoff.Continue(bState) {
		err := b.cli.Pull(ctx, image)
		if err == nil {
			return nil
		}
		b.log.Errorf("Error pulling Docker image %s", image)
	}
	return errors.New("backoff retry exceeded pulling Docker image")
}

// getRunConfig will generate a docker.RunConfig for a given job.
// It will inject the check options and target as environment variables.
// It will return the generated docker.RunConfig.
func (b *Backend) getRunConfig(params backend.RunParams) dockerutils.RunConfig {
	b.log.Debugf("fetching check variables from configuration %+v", params.RequiredVars)
	vars := dockerVars(params.RequiredVars, b.checkVars)
	return dockerutils.RunConfig{
		ContainerConfig: &container.Config{
			Hostname: params.CheckID,
			Image:    params.Image,
			Env: append([]string{
				fmt.Sprintf("%s=%s", backend.CheckIDVar, params.CheckID),
				fmt.Sprintf("%s=%s", backend.ChecktypeNameVar, params.CheckTypeName),
				fmt.Sprintf("%s=%s", backend.ChecktypeVersionVar, params.ChecktypeVersion),
				fmt.Sprintf("%s=%s", backend.CheckTargetVar, params.Target),
				fmt.Sprintf("%s=%s", backend.CheckAssetTypeVar, params.AssetType),
				fmt.Sprintf("%s=%s", backend.CheckOptionsVar, params.Options),
				fmt.Sprintf("%s=%s", backend.AgentAddressVar, b.agentAddr),
			},
				vars...,
			),
		},
		HostConfig:            &container.HostConfig{},
		NetConfig:             &network.NetworkingConfig{},
		ContainerStartOptions: types.ContainerStartOptions{},
	}
}

// dockerVars assigns the required environment variables in a format supported by Docker.
func dockerVars(requiredVars []string, envVars map[string]string) []string {
	var dockerVars []string

	for _, requiredVar := range requiredVars {
		dockerVars = append(dockerVars, fmt.Sprintf("%s=%s", requiredVar, envVars[requiredVar]))
	}

	return dockerVars
}

// getAgentAddr returns the current address of the agent API from the Docker network.
// It will also return any errors encountered while doing so.
func getAgentAddr(port, ifaceName string) (string, error) {
	connAddr, err := net.ResolveTCPAddr("tcp", port)
	if err != nil {
		return "", err
	}
	if ifaceName == "" {
		ifaceName = defaultDockerIfaceName
	}
	iface, err := net.InterfaceByName(ifaceName)
	if err != nil {
		return "", err
	}

	addrs, err := iface.Addrs()
	if err != nil {
		return "", err
	}

	for _, addr := range addrs {
		ip, _, err := net.ParseCIDR(addr.String())
		if err != nil {
			return "", err
		}

		// Check if it is IPv4.
		if ip.To4() != nil {
			connAddr.IP = ip
			return connAddr.String(), nil
		}
	}

	return "", errors.New("failed to determine Docker agent IP address")
}

func readContainerLogs(r io.ReadCloser) ([]byte, error) {
	bout, berr := &bytes.Buffer{}, &bytes.Buffer{}
	_, err := stdcopy.StdCopy(bout, berr, r)
	if err != nil {
		return nil, err
	}
	outContent := bout.Bytes()
	errContent := berr.Bytes()
	contents := [][]byte{}
	contents = append(contents, outContent)
	contents = append(contents, errContent)
	out := bytes.Join(contents, []byte("\n"))
	return out, nil
}
