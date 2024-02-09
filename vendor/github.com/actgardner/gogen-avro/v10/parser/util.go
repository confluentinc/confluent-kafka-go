package parser

func interfaceSliceToStringSlice(iSlice []interface{}) ([]string, bool) {
	var ok bool
	stringSlice := make([]string, len(iSlice))
	for i, v := range iSlice {
		stringSlice[i], ok = v.(string)
		if !ok {
			return nil, false
		}
	}
	return stringSlice, true
}

// Insert all the records from m2 into m1, unless the key already exists in m1
func mergeMaps(m1, m2 map[string]interface{}) map[string]interface{} {
	for k, v := range m2 {
		if _, ok := m1[k]; !ok {
			m1[k] = v
		}
	}
	return m1
}
