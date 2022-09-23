package schemaregistry

import (
	"context"
	"errors"
	"hash/fnv"
	"log"
	"os"

	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
)

type compatiblityMapping struct {
	awsToConfluent map[string]string
	confluentToAws map[string]string
}

type awsGlueService struct {
	registryName string
	context      *context.Context
	client       *glue.Client
	mappings     *compatiblityMapping
}

// newAwsGlueClient create new aws glue client */
func newAwsGlueService(conf *Config) *awsGlueService {

	mappings := &compatiblityMapping{
		awsToConfluent: map[string]string{},
		confluentToAws: map[string]string{},
	}
	mappings.initCompatibiiltyMappings()

	ctx := context.TODO()
	cfg, err := awsConfig.LoadDefaultConfig(ctx, awsConfig.WithRegion(conf.AwsRegion))

	if err != nil {
		log.Fatal("Failed to create aws glue client", err)
		os.Exit(1)
	}

	return &awsGlueService{
		registryName: conf.RegistryName,
		context:      &ctx,
		client:       glue.NewFromConfig(cfg),
		mappings:     mappings,
	}

}

func (as *awsGlueService) registerSchema(subject string, metadata *SchemaMetadata) (err error) {

	metadata.ID = hash(subject)
	getInput := glue.GetSchemaInput{
		SchemaId: &types.SchemaId{
			RegistryName: &as.registryName,
			SchemaName:   &subject,
		},
	}

	getOutput, err := as.client.GetSchema(*as.context, &getInput)
	if err == nil {
		metadata.Version = int(getOutput.LatestSchemaVersion)
		return nil
	}

	createInput := glue.CreateSchemaInput{
		DataFormat:       types.DataFormat(metadata.SchemaType),
		SchemaName:       &subject,
		Compatibility:    types.CompatibilityBackward,
		RegistryId:       &types.RegistryId{RegistryName: &as.registryName},
		SchemaDefinition: &metadata.Schema,
	}

	createOutput, err := as.client.CreateSchema(*as.context, &createInput)
	if err != nil {
		log.Fatal("Failed to register schema", err)
		return err
	}
	metadata.Version = int(createOutput.LatestSchemaVersion)
	return nil
}

func (as *awsGlueService) getLatestBySubject(subject string, metadata *SchemaMetadata) (err error) {
	getInput := &glue.GetSchemaVersionInput{
		SchemaId: &types.SchemaId{
			RegistryName: &as.registryName,
			SchemaName:   &subject,
		},
		SchemaVersionNumber: &types.SchemaVersionNumber{
			LatestVersion: true,
		},
	}

	return getSchema(as, getInput, metadata, subject)
}

func (as *awsGlueService) getId(subject string, metadata *SchemaMetadata) (err error) {
	metadata.ID = hash(subject) //AWS Glue schemas don't have a built in numerical id, use hashcode
	return nil
}

func (as *awsGlueService) getBySubjectAndVersion(subject string, version int, metadata *SchemaMetadata) (err error) {
	getInput := &glue.GetSchemaVersionInput{
		SchemaId: &types.SchemaId{
			RegistryName: &as.registryName,
			SchemaName:   &subject,
		},
		SchemaVersionNumber: &types.SchemaVersionNumber{
			VersionNumber: int64(version),
		},
	}

	return getSchema(as, getInput, metadata, subject)
}

func (as *awsGlueService) getAllAvailableVersions(subject string, versions *[]int) (err error) {
	listInput := &glue.ListSchemaVersionsInput{
		SchemaId: &types.SchemaId{
			RegistryName: &as.registryName,
			SchemaName:   &subject,
		},
	}

	listOutput, err := as.client.ListSchemaVersions(*as.context, listInput)
	if err != nil {
		log.Fatal("Failed to fetch list of schema versions", err)
		return err
	}
	for _, item := range listOutput.Schemas {
		if item.Status == types.SchemaVersionStatusAvailable {
			*versions = append(*versions, int(item.VersionNumber))
		}
	}
	return nil
}

func (as *awsGlueService) getVersionByDefinition(subject string, metadata *SchemaMetadata) (err error) {

	schemaDefInput := &glue.GetSchemaByDefinitionInput{
		SchemaDefinition: &metadata.Schema,
		SchemaId: &types.SchemaId{
			RegistryName: &as.registryName,
			SchemaName:   &subject,
		},
	}

	schemaDefOutput, err := as.client.GetSchemaByDefinition(*as.context, schemaDefInput)
	if err != nil {
		log.Fatal("Failed to version by the schema definition", err)
		return err
	}

	getVersionInput := &glue.GetSchemaVersionInput{
		SchemaVersionId: schemaDefOutput.SchemaVersionId,
	}
	return getSchema(as, getVersionInput, metadata, subject)
}

func (as *awsGlueService) listSchemas(subjects *[]string) (err error) {
	listInput := &glue.ListSchemasInput{
		RegistryId: &types.RegistryId{
			RegistryName: &as.registryName,
		},
	}

	listOutput, err := as.client.ListSchemas(*as.context, listInput)
	if err != nil {
		log.Fatal("Failed to list schemas", err)
		return err
	}
	for _, schema := range listOutput.Schemas {
		*subjects = append(*subjects, *schema.SchemaName)
	}
	return nil
}

func (as *awsGlueService) deleteSchema(subject string, deleted *[]int) (err error) {
	as.getAllAvailableVersions(subject, deleted)

	deleteInput := &glue.DeleteSchemaInput{
		SchemaId: &types.SchemaId{
			RegistryName: &as.registryName,
			SchemaName:   &subject,
		},
	}
	_, err = as.client.DeleteSchema(*as.context, deleteInput)
	if err != nil {
		log.Fatal("Failed to delete schema", err)
		return err
	}
	return nil
}

func (as *awsGlueService) deleteSchemaVersion(subject string, version int, deleted *int) (err error) {
	*deleted = version
	strVersion := string(rune(version))
	deleteInput := &glue.DeleteSchemaVersionsInput{
		SchemaId: &types.SchemaId{
			RegistryName: &as.registryName,
			SchemaName:   &subject,
		},
		Versions: &strVersion,
	}
	_, err = as.client.DeleteSchemaVersions(*as.context, deleteInput)
	if err != nil {
		log.Fatal("Failed to delete schema", err)
		return err
	}
	return nil
}

func (as *awsGlueService) getSchemaCompatibility(subject string, compatibility *compatibilityLevel) (err error) {
	getInput := &glue.GetSchemaInput{
		SchemaId: &types.SchemaId{
			RegistryName: &as.registryName,
			SchemaName:   &subject,
		},
	}
	getOutput, err := as.client.GetSchema(*as.context, getInput)
	if err != nil {
		log.Fatal("Failed to fetch schema compatibility", err)
		return err
	}
	var c Compatibility = -1
	(&c).ParseString(as.mappings.awsToConfluent[string(getOutput.Compatibility)])
	(*compatibility).Compatibility = c
	return nil
}

func (as *awsGlueService) isCompatible(subject string, version int, metadata *SchemaMetadata, compatibilityValue *compatibilityValue) (err error) {
	return errors.New("comparing compatiblity of schema and a specific version currently not supported in aws glue library")
}

//AWS Glue api has some limitations and changes will apply to latest version only
func (as *awsGlueService) updateCompatibility(subject string, update *compatibilityLevel) (err error) {
	newCompatibility := update.CompatibilityUpdate
	updateInput := &glue.UpdateSchemaInput{
		SchemaId: &types.SchemaId{
			RegistryName: &as.registryName,
			SchemaName:   &subject,
		},
		Compatibility:       types.Compatibility(as.mappings.confluentToAws[newCompatibility.String()]),
		SchemaVersionNumber: &types.SchemaVersionNumber{LatestVersion: true},
	}
	_, err = as.client.UpdateSchema(*as.context, updateInput)
	if err != nil {
		log.Fatal("Failed to update schema compatibility", err)
		return err
	}
	update.Compatibility = newCompatibility
	return nil
}

func (as *awsGlueService) getDefaultCompatibility(compatibility *compatibilityLevel) (err error) {
	var c Compatibility = -1
	(&c).ParseString(as.mappings.awsToConfluent[string(types.CompatibilityBackward)])
	(*compatibility).Compatibility = c
	return nil
}

func (as *awsGlueService) updateDefaultCompatibility(update *compatibilityLevel) (err error) {
	return errors.New("default compatibility update not supported in aws glue")
}

func getSchema(as *awsGlueService, input *glue.GetSchemaVersionInput, metadata *SchemaMetadata, schemaName string) (err error) {
	output, err := as.client.GetSchemaVersion(*as.context, input)
	if err != nil {
		log.Fatal("Failed to fetch schema by id and version", err)
		return err
	}

	metadata.ID = hash(schemaName)
	metadata.Schema = *output.SchemaDefinition
	metadata.SchemaType = string(output.DataFormat)
	metadata.Version = int(output.VersionNumber)

	return nil
}

func hash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

func (cm *compatiblityMapping) initCompatibiiltyMappings() {

	cm.awsToConfluent["DISABLED"] = ""
	cm.awsToConfluent["NONE"] = "NONE"
	cm.awsToConfluent["BACKWARD"] = "BACKWARD"
	cm.awsToConfluent["BACKWARD_ALL"] = "BACKWARD_TRANSITIVE"
	cm.awsToConfluent["FORWARD"] = "FORWARD"
	cm.awsToConfluent["FORWARD_ALL"] = "FORWARD_TRANSITIVE"
	cm.awsToConfluent["FULL"] = "FULL"
	cm.awsToConfluent["FULL_ALL"] = "FULL_TRANSITIVE"

	cm.confluentToAws[""] = "DISABLED"
	cm.confluentToAws["NONE"] = "NONE"
	cm.confluentToAws["BACKWARD"] = "BACKWARD"
	cm.confluentToAws["BACKWARD_TRANSITIVE"] = "BACKWARD_ALL"
	cm.confluentToAws["FORWARD"] = "FORWARD"
	cm.confluentToAws["FORWARD_TRANSITIVE"] = "FORWARD_ALL"
	cm.confluentToAws["FULL"] = "FULL"
	cm.confluentToAws["FULL_TRANSITIVE"] = "FULL_ALL"
}
