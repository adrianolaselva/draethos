package target

import (
	"draethos.io.com/internal/interfaces"
	"draethos.io.com/pkg/streams/specs"
	"encoding/xml"
)

type xmlCodec struct {
	targetSpec specs.Target
}

func NewXmlCodec() interfaces.CodecInterface {
	return xmlCodec{}
}

func (xmlCodec) Deserialize(content []byte) (map[string]interface{}, error) {
	var data map[string]interface{}
	if err := xml.Unmarshal(content, &data); err != nil {
		return nil, err
	}

	return data, nil
}

func (xmlCodec) Serialize(content map[string]interface{}) ([]byte, error) {
	data, err := xml.Marshal(content)
	if err != nil {
		return nil, err
	}

	return data, nil
}
