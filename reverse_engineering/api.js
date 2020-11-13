'use strict'

const aws = require('aws-sdk');
const fs = require('fs');
const https = require('https');
const commonHelper = require('./helpers/commonHelper');
const dataHelper = require('./helpers/dataHelper');
const errorHelper = require('./helpers/errorHelper');
const adaptJsonSchema = require('./helpers/adaptJsonSchema/adaptJsonSchema');
const resolveExternalDefinitionPathHelper = require('./helpers/resolveExternalDefinitionPathHelper');
const validationHelper = require('../forward_engineering/helpers/validationHelper');

this.schemasInstance = null;

module.exports = {
	connect: async (connectionInfo, logger, cb, app) => {
		const { accessKeyId, secretAccessKey, region, certAuthorityPath } = connectionInfo;
		const certAuthority = getCertificateAuthority(certAuthorityPath);
		const httpOptions = certAuthority ? {
			httpOptions: {
				agent: new https.Agent({
					rejectUnauthorized: true,
					ca: [certAuthority]
				})}
			} : {};
		aws.config.update({ accessKeyId, secretAccessKey, region, ...httpOptions });
        const schemasInstance = new aws.Schemas({apiVersion: '2019-12-02'});
		cb(schemasInstance);
	},

	disconnect: function(connectionInfo, cb){
		cb();
	},

	testConnection: function(connectionInfo, logger, cb, app) {
		logInfo('Test connection', connectionInfo, logger);
		const connectionCallback = async (schemasInstance) => {
			try {
				await schemasInstance.listRegistries().promise();
				cb();
			} catch (err) {
				logger.log('error', { message: err.message, stack: err.stack, error: err }, 'Connection failed');
				cb(err);
			}
		};

		this.connect(connectionInfo, logger, connectionCallback, app);
	},

	getDbCollectionsNames: function(connectionInfo, logger, cb, app) {
		const connectionCallback = async (schemasInstance) => {
            this.schemasInstance = schemasInstance;
			try {
				const registries = await listRegistries(schemasInstance);
				const registrySchemas = registries.map(async registry => {
					const schemas = await listSchemas(schemasInstance, registry.RegistryName);
					const schemaNames = schemas.map(({ SchemaName }) => SchemaName);
					const dbCollectionsChildrenCount = schemas.reduce((acc, { SchemaName, VersionCount }) => {
						acc[SchemaName] = VersionCount;
						return acc;
					}, {});
					return {
						dbName: registry.RegistryName,
						dbCollections: schemaNames,
						isEmpty: schemaNames.length === 0,
						dbCollectionsChildrenCount
					};
				});
				const result = await Promise.all(registrySchemas);
				cb(null, result);
			} catch(err) {
				logger.log(
					'error',
					{ message: err.message, stack: err.stack, error: err },
					'Retrieving databases and tables information'
				);
				cb({ message: err.message, stack: err.stack });
			}
		};

		logInfo('Retrieving databases and tables information', connectionInfo, logger);
		this.connect(connectionInfo, logger, connectionCallback, app);
	},

	getDbCollectionsData: function(data, logger, cb, app) {
		logger.log('info', data, 'Retrieving schema', data.hiddenKeys);
		
		const { collectionData } = data;
		const registries = collectionData.dataBaseNames;
        const schemas = collectionData.collections;
        const registryName = registries[0];
		const schemaName = schemas[registryName][0];
		const schemaVersion = collectionData.collectionVersion[registryName] &&
			collectionData.collectionVersion[registryName][schemaName];

		const getSchema = async () => {
			try {
				const registryData = await this.schemasInstance.describeRegistry({ RegistryName: registryName }).promise();
                const schemaData = await this.schemasInstance
                  .describeSchema({
                    RegistryName: registryName,
                    SchemaName: schemaName,
                    SchemaVersion: schemaVersion
                  })
                  .promise();
				const openAPISchema = JSON.parse(schemaData.Content);
				const { modelData, modelContent, definitions } = convertOpenAPISchemaToHackolade(openAPISchema);
				const eventbridgeModelLevelData = {
					ESBRregistry: registryName,
					EBSRregistryDescription: registryData.Description,
					EBSRschemaDescription: schemaData.Description,
					ESBRschemaARN: schemaData.SchemaArn,
					ESBRregistryARN: registryData.RegistryArn,
					EBSRschemaVersion: schemaData.SchemaVersion,
					EBSRRegistryTags: mapEBSRTags(registryData.Tags),
					EBSRSchemaTags: mapEBSRTags(schemaData.Tags),
					EBSRversionCreatedDate: schemaData.VersionCreatedDate,
					EBSRlastModified: schemaData.LastModified,
					modelName: schemaName
				};
				const modelLevelData = { ...modelData, ...eventbridgeModelLevelData };
                const modelDefinitions = JSON.parse(definitions);
				const packagesData = mapPackageData(modelContent);
				if (packagesData.length === 0) {
					packagesData[0] = { modelDefinitions };
				} else {
					packagesData[0] = { ...packagesData[0], modelDefinitions };
				}

                cb(null, packagesData, modelLevelData);
			} catch(err) {
				logger.log(
					'error',
					{ message: err.message, stack: err.stack, error: err },
					'Retrieving databases and tables information'
				);
				cb({ message: err.message, stack: err.stack });
			}
		};

		getSchema();
    },
    
	reFromFile(data, logger, callback) {
        commonHelper.getFileData(data.filePath).then(fileData => {
            return getOpenAPISchema(fileData, data.filePath);
        }).then(openAPISchema => {
            const fieldOrder = data.fieldInference.active;
            return handleOpenAPIData(openAPISchema, fieldOrder);
        }).then(reversedData => {
            return callback(null, reversedData.hackoladeData, reversedData.modelData, [], 'multipleSchema');
        }, ({ error, openAPISchema }) => {
			if (!openAPISchema) {
				return this.handleErrors(error, logger, callback);
			}

			validationHelper.validate(filterSchema(openAPISchema), { resolve: { external: false }})
				.then((messages) => {
					if (!Array.isArray(messages) || !messages.length) {
						this.handleErrors(error, logger, callback);
					}

					const message = `${messages[0].label}: ${messages[0].title}`;
					const errorData = error.error || {};

					this.handleErrors(errorHelper.getValidationError({ stack: errorData.stack, message }), logger, callback);
				})
				.catch(err => {
					this.handleErrors(error, logger, callback);
				});
		}).catch(errorObject => {
            this.handleErrors(errorObject, logger, callback);
		});
	},

	handleErrors(errorObject, logger, callback) {
		const { error, title } = errorObject;
		const handledError =  commonHelper.handleErrorObject(error, title);
		logger.log('error', handledError, title);
		callback(handledError);
	},

    adaptJsonSchema(data, logger, callback) {
        logger.log('info', 'Adaptation of JSON Schema started...', 'Adapt JSON Schema');
        try {
            const jsonSchema = JSON.parse(data.jsonSchema);

            const adaptedJsonSchema = adaptJsonSchema(jsonSchema);

            logger.log('info', 'Adaptation of JSON Schema finished.', 'Adapt JSON Schema');

            callback(null, {
                jsonSchema: JSON.stringify(adaptedJsonSchema)
            });
        } catch(e) {
            callback(commonHelper.handleErrorObject(e, 'Adapt JSON Schema'), data);
        }
    },

	resolveExternalDefinitionPath(data, logger, callback) {
		resolveExternalDefinitionPathHelper.resolvePath(data, callback);
	},

	getDBCollectionVersions(data, logger, callback) {
		const getSchemaVersions = async () => {
			const { containerName, entityName } = data;
			try {
				const schemaVersionsResponse = await this.schemasInstance
					.listSchemaVersions({
						RegistryName: containerName,
						SchemaName: entityName,
					})
					.promise();
				const schemaVersions = schemaVersionsResponse.SchemaVersions.map(({ SchemaVersion }) => ({ name: SchemaVersion }));
				callback(null, { collectionVersions: schemaVersions });
			} catch (err) {
				logger.log(
					'error',
					{ message: err.message, stack: err.stack, error: err },
					'Retrieving schema versions'
				);
				callback({ message: err.message, stack: err.stack });
			}
		}

		getSchemaVersions();
	}
};

const convertOpenAPISchemaToHackolade = (openAPISchema, fieldOrder) => {
    const modelData = dataHelper.getModelData(openAPISchema);
	const components = openAPISchema.components;
    const definitions = dataHelper.getComponents(openAPISchema.components, fieldOrder);
	const callbacksComponent = components && components.callbacks;
    const modelContent = dataHelper.getModelContent(openAPISchema.paths || {}, fieldOrder, callbacksComponent);
    return { modelData, modelContent, definitions };
};

const getOpenAPISchema = (data, filePath) => new Promise((resolve, reject) => {
    const { extension, fileName } = commonHelper.getPathData(data, filePath);

    try {
        const openAPISchemaWithModelName = dataHelper.getOpenAPIJsonSchema(data, fileName, extension);
        const isValidOpenAPISchema = dataHelper.validateOpenAPISchema(openAPISchemaWithModelName);

        if (isValidOpenAPISchema) {
            return resolve(openAPISchemaWithModelName);
        } else {
            return reject({ error: errorHelper.getValidationError(new Error('Selected file is not a valid OpenAPI 3.0.2 schema')) });
        }
    } catch (error) {
        return reject({ error: errorHelper.getParseError(error) });
    }
});

const handleOpenAPIData = (openAPISchema, fieldOrder) => new Promise((resolve, reject) => {
    try {
        const convertedData = convertOpenAPISchemaToHackolade(openAPISchema, fieldOrder);
        const { modelData, modelContent, definitions } = convertedData;
        const hackoladeData = modelContent.containers.reduce((accumulator, container) => {
            const currentEntities = modelContent.entities[container.name];
            return [
                ...accumulator, 
                ...currentEntities.map(entity => {
                    const packageData = {
                        objectNames: {
                            collectionName: entity.collectionName
                        },
                        doc: {
                            dbName: container.name,
                            collectionName: entity.collectionName,
                            modelDefinitions: definitions,
                            bucketInfo: container
                        },
                        jsonSchema: JSON.stringify(entity)
                    };
                    return packageData;
                })
            ];
        }, []);
		if (hackoladeData.length) {
			return resolve({ hackoladeData, modelData });
		}

		return resolve({
			hackoladeData: [{
				objectNames: {},
				doc: { modelDefinitions: definitions }
			}],
			modelData
		});
    } catch (error) {
        return reject({ error: errorHelper.getConvertError(error), openAPISchema });
    }
});

const filterSchema = schema => {
	delete schema.modelName;

	return schema;
};

const logInfo = (step, connectionInfo, logger) => {
	logger.clear();
	logger.log('info', connectionInfo, 'connectionInfo', connectionInfo.hiddenKeys);
};

const mapEBSRTags = (tags = {}) => {
	return Object.entries(tags).reduce((acc, [key, value]) => {
		return acc.concat({ EBSRtagKey: key, EBSRtagValue: value });
	}, []);
}

const mapPackageData = (data) => {
	return Object.entries(data.entities).reduce((acc, [containerName, containerEntities]) => {
		const entities = containerEntities.map(entity => {
			const { collectionName, properties, ...entityLevel } = entity;
			const { name, bucketInfo } = data.containers.find(item => item.name === containerName);
			const entityPackage = {
				dbName: containerName,
				collectionName,
				bucketInfo,
				entityLevel,
				documents: [],
				validation: {
					jsonSchema: { properties }
				}
			};
			return entityPackage;
		});
		return acc.concat(...entities);
	}, []);
}

const listRegistries = async (schemasInstance) => {
	let { NextToken, Registries } = await schemasInstance.listRegistries().promise();
	const registries = [...Registries];
	let nextToken = NextToken;
	while (nextToken) {
		const { NextToken, Registries } = await schemasInstance.listRegistries({ NextToken: nextToken }).promise();
		registries.push(...Registries);
		nextToken = NextToken;
	}
	return registries;
}

const listSchemas = async (schemasInstance, registryName) => {
	const { NextToken, Schemas } = await schemasInstance.listSchemas({ RegistryName: registryName }).promise();
	const schemas = [...Schemas];
	let nextToken = NextToken;
	while (nextToken) {
		const { NextToken, Schemas } =
			await schemasInstance.listSchemas({ RegistryName: registryName, NextToken: nextToken }).promise();
		schemas.push(...Schemas);
		nextToken = NextToken;
	}
	return schemas;
}

const getCertificateAuthority = path => {
	if (!path) {
		return Promise.resolve('');
	}

	return new Promise(resolve => {
		fs.readFile(path, 'utf8', (err, data) => {
			if (err) {
				resolve('');
			}
			resolve(data);
		});
	});
};
