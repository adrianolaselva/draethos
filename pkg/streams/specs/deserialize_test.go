package specs

import (
	"testing"
)

func TestShouldDeserializeYamlWithSuccessful(t *testing.T) {
	data, err := StreamDeserialize([]byte(YamlTest))
	if err != nil {
		t.Errorf("failed to deserialize yaml: %v", err)
	}

	if data.Stream.Port != "9999" {
		t.Errorf("failed to deserialize [Port]")
	}

	if data.Stream.HealthCheck.Endpoint != "/health" {
		t.Errorf("failed to deserialize [HealthCheck.Endpoint]")
	}

	if data.Stream.Metrics.Endpoint != "/metrics" {
		t.Errorf("failed to deserialize [Metrics.Endpoint]")
	}

	if data.Stream.Instance.Source.Type != "kafka" {
		t.Errorf("failed to deserialize [Instances.Source.Type]")
	}

	if data.Stream.Instance.Source.SourceSpecs.Topic != "topic_test_1" {
		t.Errorf("failed to deserialize [Instances.Source.SourceSpecs.Topic]")
	}

	if data.Stream.Instance.Target.Type != "gcloudstorage" {
		t.Errorf("failed to deserialize [Instances.Target.Type]")
	}

	if data.Stream.Instance.Target.TargetSpecs.Bucket != "topic_test_1" {
		t.Errorf("failed to deserialize [Instances.Target.TargetSpecs.Bucket]")
	}

	if data.Stream.Instance.Target.TargetSpecs.Codec != "jsonl" {
		t.Errorf("failed to deserialize [Instances.Target.TargetSpecs.Bucket]")
	}

	if data.Stream.Instance.Target.TargetSpecs.Prefix != "/topic_test_1/year=%{YEAR}/month=%{MONTH}/day=%{DAY}/hour=%{HOUR}/" {
		t.Errorf("failed to deserialize [Instances.Target.TargetSpecs.Bucket]")
	}

	if data.Stream.Instance.Target.TargetSpecs.BatchSize != 1000 {
		t.Errorf("failed to deserialize [Instances.Target.TargetSpecs.Bucket]")
	}
}

const (
	YamlTest = `stream:
  port: 9999
  healthCheck:
    endpoint: /health
  metrics:
    endpoint: /metrics
  instances:
    - source:
        type: kafka
        specs:
          topic: topic_test_1
          configurations:
            groupId: '${KAFKA_GROUP_ID}'
            bootstrapServers: '${KAFKA_BOOTSTRAP_SERVERS}'
            autoOffsetReset: 'beginning'
            autoCreate: true
            numPartitions: 5
            numReplicationFactor: 1
      target:
        type: gcloudstorage
        specs:
          bucket: topic_test_1
          prefix: '/topic_test_1/year=%{YEAR}/month=%{MONTH}/day=%{DAY}/hour=%{HOUR}/'
          codec: jsonl
          batchSize: 1000
          flushInMilliseconds: 100000
      dlq:
        type: kafka
        specs:
          topic: topic_test_1_dlq
          configurations:
            bootstrapServers: '${KAFKA_BOOTSTRAP_SERVERS}'
            autoCreate: true
            numPartitions: 5
            numReplicationFactor: 1
`
)
