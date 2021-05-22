package remote

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pyroscope-io/pyroscope/pkg/agent"
	"github.com/pyroscope-io/pyroscope/pkg/agent/upstream"
)

var (
	ErrCloudTokenRequired = errors.New("Please provide an authentication token. You can find it here: https://pyroscope.io/cloud")
	cloudHostnameSuffix   = "pyroscope.cloud"
)

type Remote struct {
	cfg    RemoteConfig
	jobs   chan *upstream.UploadJob
	done   chan struct{}
	client *http.Client
	wg     sync.WaitGroup

	Logger agent.Logger
}

type RemoteConfig struct {
	AuthToken              string
	UpstreamThreads        int
	UpstreamAddress        string
	UpstreamRequestTimeout time.Duration
}

func New(cfg RemoteConfig, logger agent.Logger) (*Remote, error) {
	remote := &Remote{
		cfg:  cfg,
		jobs: make(chan *upstream.UploadJob, 100),
		done: make(chan struct{}),
		client: &http.Client{
			Transport: &http.Transport{
				MaxConnsPerHost: cfg.UpstreamThreads,
			},
			Timeout: cfg.UpstreamRequestTimeout,
		},
		Logger: logger,
	}

	// parse the upstream address
	u, err := url.Parse(cfg.UpstreamAddress)
	if err != nil {
		return nil, err
	}

	// authorize the token first
	if cfg.AuthToken == "" && requiresAuthToken(u) {
		return nil, ErrCloudTokenRequired
	}

	// start goroutines for uploading profile data
	remote.start()

	return remote, nil
}

func (s *Remote) start() {
	for i := 0; i < s.cfg.UpstreamThreads; i++ {
		s.wg.Add(1)
		go s.handleJobs()
	}
}

func (s *Remote) Stop() {
	if s.done != nil {
		close(s.done)
	}

	// wait for upload goroutines exit
	s.wg.Wait()
}

func (s *Remote) Upload(job *upstream.UploadJob) {
	select {
	case s.jobs <- job:
	default:
		s.Logger.Errorf("upload queue is full, drop the profile job")
	}
}

func (s *Remote) uploadProfile(job *upstream.UploadJob) error {
	u, err := url.Parse(s.cfg.UpstreamAddress)
	if err != nil {
		return fmt.Errorf("url parse: %v", err)
	}

	q := u.Query()
	q.Set("name", job.Name)
	q.Set("startTime", strconv.Itoa(int(job.StartTime.Unix())))
	q.Set("endTime", strconv.Itoa(int(job.EndTime.Unix())))
	q.Set("spyName", job.SpyName)
	q.Set("sampleRate", strconv.Itoa(int(job.SampleRate)))
	q.Set("units", job.Units)
	q.Set("aggregationType", job.AggregationType)

	u.Path = path.Join(u.Path, "/ingest")
	u.RawQuery = q.Encode()

	s.Logger.Infof("uploading at %s", u.String())
	// new a request for the job
	request, err := http.NewRequest("POST", u.String(), bytes.NewReader(job.Trie.Bytes()))
	if err != nil {
		return fmt.Errorf("new http request: %v", err)
	}
	request.Header.Set("Content-Type", "binary/octet-stream+trie")
	if s.cfg.AuthToken != "" {
		request.Header.Set("Authorization", "Bearer "+s.cfg.AuthToken)
	}

	// do the request and get the response
	response, err := s.client.Do(request)
	if err != nil {
		return fmt.Errorf("do http request: %v", err)
	}
	defer response.Body.Close()

	// read all the response body
	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("read response body: %v", err)
	}
	s.Logger.Infof("response data: %v", string(data))

	return nil
}

// handle the jobs
func (u *Remote) handleJobs() {
	defer u.wg.Done()
	for {
		select {
		case <-u.done:
			return
		case job := <-u.jobs:
			u.safeUpload(job)
		}
	}
}

func requiresAuthToken(u *url.URL) bool {
	return strings.HasSuffix(u.Host, cloudHostnameSuffix)
}

// do safe upload
func (s *Remote) safeUpload(job *upstream.UploadJob) {
	defer func() {
		if r := recover(); r != nil {
			s.Logger.Errorf("recover stack: %v", debug.Stack())
		}
	}()

	// update the profile data to server
	if err := s.uploadProfile(job); err != nil {
		s.Logger.Errorf("upload profile: %v", err)
	}
}
