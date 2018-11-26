package exporter

import (
	"fmt"
	"net"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/elasticache"
	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"
)

const EngineRedis = "redis"

// SDConfig is the configuration for EC2 based service discovery.
type SDConfig struct {
	Region          string
	AccessKey       string
	SecretKey       string
	Profile         string
	RoleARN         string
	RefreshInterval model.Duration
}

func NewSDConfig(awsRegion, awsAccessKey, awsSecretKey, awsProfile, awsRoleARN, awsRefreshInterval string) (*SDConfig, error) {
	config := &SDConfig{
		Region:          awsRegion,
		AccessKey:       awsAccessKey,
		SecretKey:       awsSecretKey,
		Profile:         awsProfile,
		RoleARN:         awsRoleARN,
		RefreshInterval: model.Duration(600 * time.Second),
	}

	if awsRefreshInterval != "" {
		if err := config.RefreshInterval.Set(awsRefreshInterval); err != nil {
			return nil, fmt.Errorf("%s is not a valid aws_refresh_interval value", awsRefreshInterval)
		}
	}

	if config.Region == "" {
		sess, err := session.NewSession()
		if err != nil {
			return nil, err
		}
		metadata := ec2metadata.New(sess)
		region, err := metadata.Region()
		if err != nil {
			return nil, fmt.Errorf("AWS ElasticCache redis configuration requires a region , %s", err)
		}
		config.Region = region
	}

	return config, nil
}

type Discovery struct {
	aws      *aws.Config
	interval time.Duration
	profile  string
	roleARN  string
}

func NewDiscovery(conf *SDConfig) *Discovery {
	creds := credentials.NewStaticCredentials(conf.AccessKey, string(conf.SecretKey), "")
	if conf.AccessKey == "" && conf.SecretKey == "" {
		creds = nil
	}

	return &Discovery{
		aws: &aws.Config{
			Region:      &conf.Region,
			Credentials: creds,
		},
		profile:  conf.Profile,
		roleARN:  conf.RoleARN,
		interval: time.Duration(conf.RefreshInterval),
	}
}

// Run implements the Discoverer interface.
func (d *Discovery) Run(exp *Exporter) {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if addrs, passwords, aliases, err := d.refresh(); err != nil {
				exp.UpdateRedis(addrs, passwords, aliases)
			} else {
				log.Errorf("fail to discovery redis instance of %s", err)
			}
		}
	}
}

// Init Get an initial set right away..
func (d *Discovery) Init() ([]string, []string, []string, error) {
	return d.refresh()
}

func (d *Discovery) refresh() ([]string, []string, []string, error) {
	sess, err := session.NewSessionWithOptions(session.Options{
		Config:  *d.aws,
		Profile: d.profile,
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("could not create aws session: %s", err)
	}

	var elasticaches *elasticache.ElastiCache
	if d.roleARN != "" {
		creds := stscreds.NewCredentials(sess, d.roleARN)
		elasticaches = elasticache.New(sess, &aws.Config{Credentials: creds})
	} else {
		elasticaches = elasticache.New(sess)
	}

	showCacheNodeInfo := true
	input := &elasticache.DescribeCacheClustersInput{
		ShowCacheNodeInfo: &showCacheNodeInfo,
	}

	output, err := elasticaches.DescribeCacheClusters(input)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("could not describe cache clusters: %s", err)
	}

	var addrs []string
	var passwords []string
	var aliases []string

	if len(output.CacheClusters) > 0 {
		for _, cluster := range output.CacheClusters {
			if *cluster.Engine != EngineRedis {
				continue
			}

			for _, node := range cluster.CacheNodes {
				addr := fmt.Sprintf("redis://%s", net.JoinHostPort(*node.Endpoint.Address, fmt.Sprintf("%d", *node.Endpoint.Port)))
				addrs = append(addrs, addr)
				passwords = append(passwords, "")
				aliases = append(aliases, "")
			}

		}

	}

	return addrs, passwords, aliases, nil
}
