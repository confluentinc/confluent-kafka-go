package kafka

type ByteArraySerializer struct {}

func (*ByteArraySerializer) Serialize(_ *string, datum interface{}) ([]byte, error) {
	return datum.([]byte), nil
}

func (*ByteArraySerializer) Configure(conf ConfigMap, _ bool) (ConfigMap, error) {
	return conf, nil
}

func (*ByteArraySerializer) Deserialize(_ *string, data []byte) (interface{}, error) {
	return data, nil
}

func (*ByteArraySerializer) Close() {}