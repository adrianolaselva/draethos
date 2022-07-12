package target

import (
	"draethos.io.com/internal/interfaces"
	"draethos.io.com/pkg/streams/specs"
	"gopkg.in/yaml.v2"
)

type yamlCodec struct {
	targetSpec specs.Target
}

func NewYamlCodec() interfaces.CodecInterface {
	return yamlCodec{}
}

func (yamlCodec) Deserialize(content []byte) (map[string]interface{}, error) {
	var data map[string]interface{}
	if err := yaml.Unmarshal(content, &data); err != nil {
		return nil, err
	}

	return data, nil
}

func (yamlCodec) Serialize(content map[string]interface{}) ([]byte, error) {
	data, err := yaml.Marshal(content)
	if err != nil {
		return nil, err
	}

	return data, nil
}
