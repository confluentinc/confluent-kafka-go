package avro

import (
	"io/ioutil"
	"github.com/rnpridgeon/avro"
)

func Load(pathname string) (Schema, error) {
	buff, err := ioutil.ReadFile(pathname)
	if err != nil {
		panic(err)
	}
	return avro.ParseSchema(string(buff))
}

func Parse(schema string) (Schema, error) {
		return avro.ParseSchema(schema)
}

