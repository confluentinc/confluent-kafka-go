package kafka

import (
	"log"
	"testing"
)

var schemaz = `
        {
          "type": "record",
          "name": "LongList",
          "fields" : [
            {"name": "next", "type": ["null", "LongList"], "default": null}
          ]
        }
`

var conf = &ConfigMap{
	"schema.registry.url" : "https://127.0.0.1:8082",
	"ssl.ca.location" : "/Users/ryan/git_home/confluent/confluent-kafka-python/certs/ca-cert",
	"ssl.disable.endpoint.verification": true,
	"ssl.certificate.location": "/Users/ryan/git_home/confluent/confluent-kafka-python/certs/_client.pem" ,
	"ssl.key.location": "/Users/ryan/git_home/confluent/confluent-kafka-python/certs/_client.key",
	"max.schemas.per.subject": 2,
}

func TestClient_Register(t *testing.T) {
	handle, _ := NewCachedSchemaRegistryClient(conf)
	for i:= 0; i < 10; i++ {
		id, err := handle.Register("test", schemaz)
		if err != nil {
			log.Print(err)
			t.Fail()
		}
		log.Println(id)
	}
}

func TestClient_GetById(t *testing.T) {
	handle, _ := NewCachedSchemaRegistryClient(conf)
	s, err := handle.GetById(1)
	if err != nil {
		log.Print(err)
		t.Failed()
	}
	log.Println(s)
}

//func TestGetBySubjectAndId(t *testing.T) {
//	conf := &ConfigMap{}
//	handle, _ := NewCachedSchemaRegistryClient(conf)
//	s, _:= handle.GetById(1)
//	log.Println(s)
//}

func TestClient_GetLatestSchemaMetadataGetLatestSchemaMetadata(t *testing.T) {
	handle, _ := NewCachedSchemaRegistryClient(conf)
	s, err := handle.GetLatestSchemaMetadata("test")
	if err != nil {
		log.Print(err)
		t.Failed()
	}
	log.Printf("%+v\n", s)
}

func TestClient_GetSchemaMetadata(t *testing.T) {
	handle, _ := NewCachedSchemaRegistryClient(conf)
	s, err := handle.GetSchemaMetadata("test", 1)
	if err != nil {
		log.Print(err)
		t.Failed()
	}
	log.Printf("%+v\n", s)
}

func TestClient_GetVersion(t *testing.T) {
	handle, _ := NewCachedSchemaRegistryClient(conf)
	s, err := handle.GetVersion("test", schemaz)
	if err != nil {
		log.Print(err)
		t.Failed()
	}
	log.Printf("%+v\n", s)
}

func TestClient_GetAllVersions(t *testing.T) {
	handle, _ := NewCachedSchemaRegistryClient(conf)
	s, err := handle.GetAllVersions("test")
	if err != nil {
		log.Print(err)
		t.Failed()
	}
	log.Println(s)
}

func TestClient_TestCompatibility(t *testing.T) {
	handle, _ := NewCachedSchemaRegistryClient(conf)
	s, err := handle.TestCompatibility("test", 1, schemaz)
	if err != nil {
		log.Print(err)
		t.Failed()
	}
	log.Println(s)
}

func TestClient_UpdateCompatibility(t *testing.T) {
	handle, _ := NewCachedSchemaRegistryClient(conf)
	s, err := handle.UpdateCompatibility("test",NONE)
	if err != nil {
		log.Print(err)
		t.Failed()
	}
	log.Println(s)
}

func TestClient_GetCompatibility(t *testing.T) {
	handle, _ := NewCachedSchemaRegistryClient(conf)
	s, err := handle.GetCompatibility("test")
	if err != nil {
		log.Print(err)
		t.Failed()
	}
	log.Println(s)
}

func TestClient_GetAllSubjects(t *testing.T) {
	handle, _ := NewCachedSchemaRegistryClient(conf)
	s, err := handle.GetAllSubjects()
	if err != nil {
		log.Print(err)
		t.Failed()
	}
	log.Println(s)
}

func TestClient_GetId(t *testing.T) {
	handle, _ := NewCachedSchemaRegistryClient(conf)
	s, err := handle.GetId("test", schemaz)
	if err != nil {
		log.Print(err)
		t.Failed()
	}
	log.Println(s)
}

func TestClient_DeleteSubject(t *testing.T) {
	handle, _ := NewCachedSchemaRegistryClient(conf)
	s, err := handle.DeleteSubject("test")
	if err != nil {
		log.Print(err)
		t.Failed()
	}
	log.Println(s)
}

func TestClient_Register2(t *testing.T) {
	handle, _ := NewCachedSchemaRegistryClient(conf)

	id, err := handle.Register("test", schemaz)
	if err != nil {
		log.Print(err)
		t.Failed()
	}
	log.Println(id)
}

func TestClient_DeleteSchemaVersion(t *testing.T) {
	handle, _ := NewCachedSchemaRegistryClient(conf)
	s, err := handle.DeleteSchemaVersion("test", 3)
	if err != nil {
		log.Print(err)
		t.Failed()
	}
	log.Println(s)
}

func TestClient_UpdateDefaultCompatibility(t *testing.T) {
	handle, _ := NewCachedSchemaRegistryClient(conf)
	s, err := handle.UpdateDefaultCompatibility(BACKWARD)
	if err != nil {
		log.Print(err)
		t.Failed()
	}
	log.Println(s)
}

func TestClient_GetDefaultCompatibility(t *testing.T) {
	handle, _ := NewCachedSchemaRegistryClient(conf)
	s, err := handle.GetDefaultCompatibility()
	if err != nil {
		log.Print(err)
		t.Failed()
	}
	log.Println(s)
}