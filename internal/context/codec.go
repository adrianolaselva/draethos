package context

import (
	target2 "draethos.io.com/internal/codec"
	"draethos.io.com/internal/interfaces"
	"go.uber.org/zap"
)

const (
	JsonCodec = "json"
	YamlCodec = "yaml"
	XmlCodec  = "xml"
)

func NewCodecContext(codec string) interfaces.CodecInterface {
	switch codec {
	case JsonCodec:
		return target2.NewJsonCodec()
	case YamlCodec:
		return target2.NewYamlCodec()
	case XmlCodec:
		return target2.NewYamlCodec()
	default:
		zap.S().Infof("%s codec not defined, using %s as standard", codec, JsonCodec)
		return target2.NewJsonCodec()
	}
}
