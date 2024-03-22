package graph

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/airbusgeo/geocube-ingester/service"
	"github.com/airbusgeo/geocube-ingester/service/log"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"

	"go.uber.org/zap/zapcore"
)

type dockerManager struct {
	Client         *client.Client
	Envs           []string
	VolumesToMount []string
	AuthConfig     string //encode base64
	LogFilter      *DockerLogFilter
}

type DockerConfig struct {
	Envs             []string
	RegistryServer   string // "https://europe-west1-docker.pkg.dev" for gcs for example
	RegistryUserName string // _json_key for gcs
	RegistryPassword string // service account for gcs
	VolumesToMount   string // List of volumes to mount (comma separated)
}

// SetFlags configures flag for a docker config
// Returns dockerEnvs as string, comma sep.
//
// cfg := DockerConfig{}
// dockerEnvsStr := cfg.Flags()
//
// flag.Parse()
//
//	if *dockerEnvsStr != "" {
//			cfg.Envs = strings.Split(*dockerEnvsStr, ",")
//		}
func (cfg *DockerConfig) SetFlags() *string {
	// Docker processing Images connection
	flag.StringVar(&cfg.RegistryUserName, "docker-registry-username", "_json_key", "username to authentication on private registry")
	flag.StringVar(&cfg.RegistryPassword, "docker-registry-password", "", "password to authentication on private registry")
	flag.StringVar(&cfg.RegistryServer, "docker-registry-server", "", "address of server to authenticate on private registry (e.g. https://europe-west1-docker.pkg.dev)")
	flag.StringVar(&cfg.VolumesToMount, "docker-mount-volumes", "", "list of volumes to mount on the docker (comma separated)")

	return flag.String("docker-envs", "", "docker variable env key white list (comma sep) ")
}

func NewDockerManager(ctx context.Context, config DockerConfig) (DockerManager, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return nil, fmt.Errorf("failed to create new docker client: %w", err)
	}

	var encodedAuthLogin string
	if config.RegistryUserName != "" && config.RegistryPassword != "" && config.RegistryServer != "" {
		log.Logger(ctx).Info("register to container registry...")
		auth := types.AuthConfig{
			Username:      config.RegistryUserName,
			Password:      config.RegistryPassword,
			ServerAddress: config.RegistryPassword,
		}

		bAuth, err := json.Marshal(&auth)
		if err != nil {
			return nil, err
		}

		encodedAuthLogin = base64.StdEncoding.EncodeToString(bAuth)
	}

	d := dockerManager{
		Client:     cli,
		Envs:       config.Envs,
		AuthConfig: encodedAuthLogin,
		LogFilter:  &DockerLogFilter{},
	}
	if len(config.VolumesToMount) > 0 {
		d.VolumesToMount = strings.Split(config.VolumesToMount, ",")
	}

	if err := d.Ping(ctx, 5*time.Minute); err != nil {
		return nil, fmt.Errorf("NewDockerManager: %w", err)
	}

	return &d, nil
}

type DockerManager interface {
	Process(ctx context.Context, cmd, workdir string, args []string, envs []string) error
}

func (d *dockerManager) Ping(ctx context.Context, timeout time.Duration) error {
	var err error
	ctx, cnl := context.WithTimeout(ctx, timeout)
	defer cnl()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("failed to found docker daemon: %w", err)
		default:
			if _, err = d.Client.Ping(ctx); err == nil {
				log.Logger(ctx).Info("docker daemon is started")
				return nil
			}
			log.Logger(ctx).Info("Waiting for docker daemon...")
			time.Sleep(5 * time.Second)
		}
	}
}

func (d *dockerManager) Process(ctx context.Context, workdir, cmd string, args []string, envs []string) error {
	if err := d.Ping(ctx, time.Minute); err != nil {
		return fmt.Errorf("Process: %w", err)
	}

	imageInfo, err := d.localImageInfo(ctx, cmd)
	if err != nil {
		log.Logger(ctx).Info("pulling image " + cmd)
		if imageInfo, err = d.pullImage(ctx, cmd); err != nil {
			return fmt.Errorf("Process: %w", err)
		}
	}
	log.Logger(ctx).Info(cmd + " pulled")
	var availableEnvs []string
	for _, env := range envs {
		for _, wlEnv := range d.Envs {
			if strings.HasPrefix(env, wlEnv) {
				availableEnvs = append(availableEnvs, env)
			}
		}
	}

	var volumeToMount []mount.Mount
	volumeToMount = append(volumeToMount, mount.Mount{
		Type:     mount.TypeBind,
		Source:   workdir,
		Target:   workdir,
		ReadOnly: false,
	})

	for _, volume := range d.VolumesToMount {
		volumeToMount = append(volumeToMount, mount.Mount{
			Type:     mount.TypeBind,
			Source:   volume,
			Target:   volume,
			ReadOnly: true,
		})
	}

	containerConfig := &container.Config{
		Image:        imageInfo.ID,
		Cmd:          args,
		AttachStdout: true,
		WorkingDir:   workdir,
		Env:          availableEnvs,
	}

	hostConfig := &container.HostConfig{
		Mounts: volumeToMount,
	}

	createdContainer, err := d.Client.ContainerCreate(ctx, containerConfig, hostConfig, nil, nil, "")
	if err != nil {
		return fmt.Errorf("failed to create %s container: %w", cmd, err)
	}

	if err = d.runContainer(ctx, createdContainer.ID); err != nil {
		return fmt.Errorf("failed to run %s container: %w", cmd, err)
	}

	defer func() {
		if err = d.Client.ContainerStop(ctx, createdContainer.ID, nil); err != nil {
			log.Logger(ctx).Sugar().Warnf("failed to stop container: %s", createdContainer.ID)
		}

		if err = d.Client.ContainerRemove(ctx, createdContainer.ID, types.ContainerRemoveOptions{}); err != nil {
			log.Logger(ctx).Sugar().Warnf("failed to remove container: %s", createdContainer.ID)
		}
	}()

	return nil
}

func (d *dockerManager) pullImage(ctx context.Context, image string) (types.ImageSummary, error) {
	imagePullRc, err := d.Client.ImagePull(ctx, image, types.ImagePullOptions{
		RegistryAuth: d.AuthConfig,
	})
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "timeout") {
			err = service.MakeTemporary(err)
		}
		return types.ImageSummary{}, fmt.Errorf("failed to pull image %s: %w", image, err)
	}

	defer imagePullRc.Close()
	imagePullb, err := io.ReadAll(imagePullRc)
	if err != nil {
		log.Logger(ctx).Sugar().Errorf("failed to read image pull information: %w", err)
	} else {
		log.Logger(ctx).Sugar().Debugf(string(imagePullb))
	}
	return d.localImageInfo(ctx, image)
}

func (d *dockerManager) localImageInfo(ctx context.Context, image string) (types.ImageSummary, error) {
	filter := filters.NewArgs()
	filter.Add("reference", image)

	images, err := d.Client.ImageList(ctx, types.ImageListOptions{
		All:     false,
		Filters: filter,
	})
	if err != nil {
		return types.ImageSummary{}, service.MakeTemporary(fmt.Errorf("failed to list image %s: %w", image, err))
	}

	if len(images) < 1 {
		return types.ImageSummary{}, service.MakeTemporary(fmt.Errorf("not found: %s", image))
	}

	return images[0], nil
}

func (d *dockerManager) runContainer(ctx context.Context, containerID string) error {
	if err := d.Client.ContainerStart(ctx, containerID, types.ContainerStartOptions{}); err != nil {
		return fmt.Errorf("failed to start container: %w", err)
	}

	containerLogs, err := d.Client.ContainerLogs(ctx, containerID, types.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
		Details:    false,
		Timestamps: false,
	})
	if err != nil {
		return fmt.Errorf("failed to retrieve logs")
	}

	logwg := sync.WaitGroup{}
	logwg.Add(1)
	go func() {
		defer logwg.Done()
		defer containerLogs.Close()
		d.logLines(ctx, containerLogs)
	}()

	logwg.Wait()

	statusCh, errCh := d.Client.ContainerWait(ctx, containerID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			return err
		}
	case exit := <-statusCh:
		if exit.StatusCode == 1 {
			return d.LogFilter.WrapError(fmt.Errorf("an error occured"))
		}
	}

	return nil
}

// sendLines
func (d *dockerManager) logLines(ctx context.Context, sr io.Reader) {
	r := bufio.NewReader(sr)
	insideTooLongLine := false
	for {
		line, err := r.ReadSlice('\n')
		if !insideTooLongLine && len(line) >= 8 {
			line = line[8:] // stream is multiplexed: remove header
		}
		if err == io.EOF {
			if !insideTooLongLine && len(line) > 0 {
				d.log(ctx, string(line))
			}
			return
		}
		if insideTooLongLine {
			if err == nil {
				//reset
				insideTooLongLine = false
			}
		} else {
			if err == bufio.ErrBufferFull {
				d.log(ctx, fmt.Sprintf("%s ...[Message clipped]", line))
				insideTooLongLine = true
			} else {
				if len(line) > 0 {
					d.log(ctx, string(line))
				}
			}
		}
	}
}

func (d *dockerManager) log(ctx context.Context, msg string) {
	var level zapcore.Level
	if d.LogFilter != nil {
		var ignore bool
		if msg, level, ignore = d.LogFilter.Filter(msg, zapcore.DebugLevel); ignore {
			return
		}
	}
	logger := log.Logger(ctx)
	if ce := logger.Check(level, msg); ce != nil {
		ce.Write()
	}
}
