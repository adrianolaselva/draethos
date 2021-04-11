package context

import (
	target "draethos.io.com/core/codec"
	"draethos.io.com/core/interfaces"
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
		return target.NewJsonCodec()
	case YamlCodec:
		return target.NewYamlCodec()
	case XmlCodec:
		return target.NewYamlCodec()
	default:
		zap.S().Infof("%s codec not defined, using %s as standard", codec, JsonCodec)
		return target.NewJsonCodec()
	}
}
