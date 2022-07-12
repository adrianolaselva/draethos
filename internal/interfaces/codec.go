package interfaces

type CodecInterface interface {
	Deserialize(content []byte) (map[string]interface{}, error)
	Serialize(content map[string]interface{}) ([]byte, error)
}
