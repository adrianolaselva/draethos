package specs

import "gopkg.in/yaml.v2"

func StreamDeserialize(content []byte) (*Stream, error) {
	var streamSpec Stream
	if err := yaml.Unmarshal(content, &streamSpec); err != nil {
		return nil, err
	}

	return &streamSpec, nil
}
