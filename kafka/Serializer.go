package kafka

type Serializer interface{
	Configure(configs ConfigMap, iskey bool) (ConfigMap, error)
	Serialize(topic *string, datum interface{}) ([]byte, error)
	Close()
}

type Deserializer interface{
	Configure(configs ConfigMap, iskey bool) (ConfigMap, error)
	Deserialize(topic *string, data []byte) (interface{}, error)
	Close()
}

//type ExtendedSerializer interface{
//	Configure(configs ConfigMap,header *Header, iskey bool) (ConfigMap, error)
//	Serialize(topic *string, datum interface{}) ([]byte, error)
//	Close()
//}
//
//type ExtendedDeserializer interface{
//	Configure(configs ConfigMap, iskey bool) (ConfigMap, error)
//	Deserialize(topic *string, header *Header, data []byte) (interface{}, error)
//	Close()
//}