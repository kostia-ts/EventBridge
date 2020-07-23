const { CLI, CREATE_REGISTRY, CREATE_SCHEMA, UPDATE_REGISTRY, UPDATE_SCHEMA } = require('./cliConstants');

function getRegistryCreateCLIStatement({ modelMetadata, isUpdateScript }) {
	const data = {
		Description: modelMetadata.EBSRregistryDescription,
    	RegistryName: modelMetadata.ESBRregistry,
		...(!isUpdateScript && {Tags: mapTags(modelMetadata.EBSRRegistryTags)})
	};
	const createRegistryStatement = `${CLI} ${
    	isUpdateScript ? UPDATE_REGISTRY : CREATE_REGISTRY
  	} '${JSON.stringify(data, null, 2)}'`;
	return createRegistryStatement;
}

function getSchemaCreateCLIStatement({ openAPISchema, modelMetadata, isUpdateScript }) {
	const data = {
		Content: JSON.stringify(openAPISchema).replace(/},"/g,'}, "'),
		Description: modelMetadata.EBSRschemaDescription,
    	RegistryName: modelMetadata.ESBRregistry,
    	SchemaName: modelMetadata.modelName,
    	...(!isUpdateScript && {Tags: mapTags(modelMetadata.EBSRSchemaTags)}),
    	Type: modelMetadata.EBSRschemaType
	};
	const createSchemaStatement = `${CLI} ${
    	isUpdateScript ? UPDATE_SCHEMA : CREATE_SCHEMA
  	} '${JSON.stringify(data, null, 2)}'`;
	return createSchemaStatement;
}

function mapTags(tags) {
	if (!tags) return;
	return tags.reduce((acc, tag) => {
		acc[tag.EBSRtagKey] = tag.EBSRtagValue;
		return acc;
	}, {});
}

module.exports = {
	getRegistryCreateCLIStatement,
	getSchemaCreateCLIStatement
}