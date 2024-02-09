package parser

func getMapString(m map[string]interface{}, key string) (string, error) {
	val, ok := m[key]
	if !ok {
		return "", NewRequiredMapKeyError(key)
	}
	typedVal, ok := val.(string)
	if !ok {
		return "", NewWrongMapValueTypeError(key, "string", val)
	}
	return typedVal, nil
}

func getMapArray(m map[string]interface{}, key string) ([]interface{}, error) {
	val, ok := m[key]
	if !ok {
		return nil, NewRequiredMapKeyError(key)
	}
	typedVal, ok := val.([]interface{})
	if !ok {
		return nil, NewWrongMapValueTypeError(key, "array", val)
	}
	return typedVal, nil
}

func getMapFloat(m map[string]interface{}, key string) (float64, error) {
	val, ok := m[key]
	if !ok {
		return 0, NewRequiredMapKeyError(key)
	}
	typedVal, ok := val.(float64)
	if !ok {
		return 0, NewWrongMapValueTypeError(key, "number", val)
	}
	return typedVal, nil
}
