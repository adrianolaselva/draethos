package target

import (
	"draethos.io.com/core/interfaces"
	"draethos.io.com/pkg/streams/specs"
	"encoding/json"
)

type jsonCodec struct {
	targetSpec specs.Target
}

func NewJsonCodec() interfaces.CodecInterface {
	return jsonCodec{}
}

func (jsonCodec) Deserialize(content []byte) (map[string]interface{}, error) {
	var data map[string]interface{}
	if err := json.Unmarshal(content, &data); err != nil {
		return nil, err
	}

	return data, nil
}

func (jsonCodec) Serialize(content map[string]interface{}) ([]byte, error) {
	data, err := json.Marshal(content)
	if err != nil {
		return nil, err
	}

	return data, nil
}
