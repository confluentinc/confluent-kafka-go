package schemaregistry

import (
	"errors"
	"net/url"
)

type schemaServiceHandler interface {
	register(subject string, normalize bool, metadata *SchemaMetadata) (err error)
	getBySubjectAndID(subject string, id int, metadata *SchemaMetadata) (err error)
	getID(subject string, normalize bool, metadata *SchemaMetadata) (err error)
	getLatestSchemaMetadata(subject string, metadata *SchemaMetadata) (err error)
	getSchemaMetadata(subject string, version int, metadata *SchemaMetadata) (err error)
	getAllVersions(subject string, versions *[]int) (err error)
	getVersionFromSchema(subject string, normalize bool, metadata *SchemaMetadata) (err error)
	getAllSubjects(subjects *[]string) (err error)
	deleteSubject(subject string, permanent bool, deleted *[]int) (err error)
	deleteSubjectVersion(subject string, version int, permanent bool, deleted *int) (err error)

	getCompatibility(subject string, compatibility *compatibilityLevel) (err error)
	updateCompatibility(subject string, update *compatibilityLevel) (err error)
	testCompatibility(subject string, version int, metadata *SchemaMetadata, compatibilityValue *compatibilityValue) (err error)
	getDefaultCompatibility(compatibility *compatibilityLevel) (err error)
	updateDefaultCompatibility(update *compatibilityLevel) (err error)
}

type restServiceHandler struct {
	*restService
}
type awsGlueServiceHandler struct {
	*awsGlueService
}

//Confluent implementations
func (rh restServiceHandler) register(subject string, normalize bool, metadata *SchemaMetadata) (err error) {
	return rh.handleRequest(newRequest("POST", versionNormalize, &metadata, url.PathEscape(subject), normalize), &metadata)
}

func (rh restServiceHandler) getBySubjectAndID(subject string, id int, metadata *SchemaMetadata) (err error) {
	if len(subject) > 0 {
		return rh.handleRequest(newRequest("GET", schemasBySubject, nil, id, url.QueryEscape(subject)), metadata)
	} else {
		return rh.handleRequest(newRequest("GET", schemas, nil, id), &metadata)
	}
}

func (rh restServiceHandler) getID(subject string, normalize bool, metadata *SchemaMetadata) (err error) {
	return rh.restService.handleRequest(newRequest("POST", subjectsNormalize, &metadata, url.PathEscape(subject), normalize), &metadata)
}

func (rh restServiceHandler) getLatestSchemaMetadata(subject string, metadata *SchemaMetadata) (err error) {
	return rh.restService.handleRequest(newRequest("GET", versions, nil, url.PathEscape(subject), "latest"), metadata)
}

func (rh restServiceHandler) getSchemaMetadata(subject string, version int, metadata *SchemaMetadata) (err error) {
	return rh.restService.handleRequest(newRequest("GET", versions, nil, url.PathEscape(subject), version), &metadata)
}

func (rh restServiceHandler) getAllVersions(subject string, versions *[]int) (err error) {
	return rh.restService.handleRequest(newRequest("GET", version, nil, url.PathEscape(subject)), versions)
}

func (rh restServiceHandler) getVersionFromSchema(subject string, normalize bool, metadata *SchemaMetadata) (err error) {
	return rh.restService.handleRequest(newRequest("POST", subjectsNormalize, &metadata, url.PathEscape(subject), normalize), &metadata)
}

func (rh restServiceHandler) getAllSubjects(subjects *[]string) (err error) {
	return rh.restService.handleRequest(newRequest("GET", subject, nil), subjects)
}

func (rh restServiceHandler) deleteSubject(subject string, permanent bool, deleted *[]int) (err error) {
	return rh.restService.handleRequest(newRequest("DELETE", subjectsDelete, nil, url.PathEscape(subject), permanent), deleted)
}

func (rh restServiceHandler) deleteSubjectVersion(subject string, version int, permanent bool, deleted *int) (err error) {
	return rh.restService.handleRequest(newRequest("DELETE", versionsDelete, nil, url.PathEscape(subject), version, permanent), deleted)
}

func (rh restServiceHandler) getCompatibility(subject string, compatibility *compatibilityLevel) (err error) {
	return rh.restService.handleRequest(newRequest("GET", subjectConfig, nil, url.PathEscape(subject)), &compatibility)
}

func (rh restServiceHandler) updateCompatibility(subject string, update *compatibilityLevel) (err error) {
	return rh.restService.handleRequest(newRequest("PUT", subjectConfig, update, url.PathEscape(subject)), update)
}

func (rh restServiceHandler) testCompatibility(subject string, version int, metadata *SchemaMetadata, compatibilityValue *compatibilityValue) (err error) {
	return rh.restService.handleRequest(newRequest("POST", compatibility, &metadata, url.PathEscape(subject), version), compatibilityValue)
}

func (rh restServiceHandler) getDefaultCompatibility(compatibility *compatibilityLevel) (err error) {
	return rh.restService.handleRequest(newRequest("GET", config, nil), &compatibility)
}

func (rh restServiceHandler) updateDefaultCompatibility(update *compatibilityLevel) (err error) {
	return rh.restService.handleRequest(newRequest("PUT", config, update), update)
}

//AWS Glue implementations
func (gh awsGlueServiceHandler) register(subject string, normalize bool, metadata *SchemaMetadata) (err error) {
	return gh.awsGlueService.registerSchema(subject, metadata)
}

func (gh awsGlueServiceHandler) getBySubjectAndID(subject string, id int, metadata *SchemaMetadata) (err error) {
	if len(subject) == 0 {
		return errors.New("subject cannot be empty for a glue schema query")
	}
	return gh.awsGlueService.getLatestBySubject(subject, metadata)
}

func (gh awsGlueServiceHandler) getID(subject string, normalize bool, metadata *SchemaMetadata) (err error) {
	return gh.awsGlueService.getId(subject, metadata)
}

func (gh awsGlueServiceHandler) getLatestSchemaMetadata(subject string, metadata *SchemaMetadata) (err error) {
	return gh.awsGlueService.getLatestBySubject(subject, metadata)
}

func (gh awsGlueServiceHandler) getSchemaMetadata(subject string, version int, metadata *SchemaMetadata) (err error) {
	return gh.getBySubjectAndVersion(subject, version, metadata)
}

func (gh awsGlueServiceHandler) getAllVersions(subject string, versions *[]int) (err error) {
	return gh.awsGlueService.getAllAvailableVersions(subject, versions)
}

func (gh awsGlueServiceHandler) getVersionFromSchema(subject string, normalize bool, metadata *SchemaMetadata) (err error) {
	return gh.awsGlueService.getVersionByDefinition(subject, metadata)
}

func (gh awsGlueServiceHandler) getAllSubjects(subjects *[]string) (err error) {
	return gh.awsGlueService.listSchemas(subjects)
}

func (gh awsGlueServiceHandler) deleteSubject(subject string, permanent bool, deleted *[]int) (err error) {
	return gh.awsGlueService.deleteSchema(subject, deleted)
}

func (gh awsGlueServiceHandler) deleteSubjectVersion(subject string, version int, permanent bool, deleted *int) (err error) {
	return gh.awsGlueService.deleteSchemaVersion(subject, version, deleted)
}

func (gh awsGlueServiceHandler) getCompatibility(subject string, compatibility *compatibilityLevel) (err error) {
	return gh.awsGlueService.getSchemaCompatibility(subject, compatibility)
}

func (gh awsGlueServiceHandler) updateCompatibility(subject string, update *compatibilityLevel) (err error) {
	return gh.awsGlueService.updateCompatibility(subject, update)
}

func (gh awsGlueServiceHandler) testCompatibility(subject string, version int, metadata *SchemaMetadata, compatibilityValue *compatibilityValue) (err error) {
	return gh.awsGlueService.isCompatible(subject, version, metadata, compatibilityValue)
}

func (gh awsGlueServiceHandler) getDefaultCompatibility(compatibility *compatibilityLevel) (err error) {
	return gh.awsGlueService.getDefaultCompatibility(compatibility)
}

func (gh awsGlueServiceHandler) updateDefaultCompatibility(update *compatibilityLevel) (err error) {
	return gh.awsGlueService.updateDefaultCompatibility(update)
}
