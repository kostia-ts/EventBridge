const aws = require('../reverse_engineering/node_modules/aws-sdk');
const validationHelper = require('./helpers/validationHelper');
const getInfo = require('./helpers/infoHelper');
const { getPaths } = require('./helpers/pathHelper');
const getComponents = require('./helpers/componentsHelpers');
const commonHelper = require('./helpers/commonHelper');
const { getServers } = require('./helpers/serversHelper');
const getExtensions = require('./helpers/extensionsHelper');
const { getRegistryCreateCLIStatement, getSchemaCreateCLIStatement } = require('./helpers/awsCLIHelpers/awsCLIHelper');
const { getApiStatements, getItemUpdateParamiters } = require('./helpers/awsCLIHelpers/applyToInstanceHelper');

module.exports = {
	generateModelScript(data, logger, cb) {
		try {
			const {
				dbVersion,
				externalDocs: modelExternalDocs,
				tags: modelTags,
				security: modelSecurity,
				servers: modelServers,
				...modelMetadata
			} = data.modelData[0];

			const containersIdsFromCallbacks = commonHelper.getContainersIdsForCallbacks(data);

			const info = getInfo(data.modelData[0]);
			const servers = getServers(modelServers);
			const paths = getPaths(data.containers, containersIdsFromCallbacks);
			const components = getComponents(data);
			const security = commonHelper.mapSecurity(modelSecurity);
			const tags = commonHelper.mapTags(modelTags);
			const externalDocs = commonHelper.mapExternalDocs(modelExternalDocs);

			const openApiSchema = {
				openapi: '3.0.0',
				info,
				servers,
				paths,
				components,
				security,
				tags,
				externalDocs
			};
			const extensions = getExtensions(data.modelData[0].scopesExtensions);

			const resultSchema = Object.assign({}, openApiSchema, extensions);
			let schema = addCommentsSigns(JSON.stringify(resultSchema, null, 2), 'json');
			schema = removeCommentLines(schema);

			const script = buildAWSCLIScript(modelMetadata, JSON.parse(schema));
			return cb(null, script);
		} catch (err) {
			logger.log('error', { error: err }, 'OpenAPI FE Error');
			cb(err);
		}
	},

	validate(data, logger, cb) {
		const { script, targetScriptOptions } = data;
		try {
			const { schema } = getApiStatements(script);
			let openAPISchema = JSON.parse(schema.Content);

			validationHelper.validate(openAPISchema)
				.then((messages) => {
					cb(null, messages);
				})
				.catch(err => {
					cb(err.message);
				});
		} catch (e) {
			logger.log('error', { error: e }, 'EventBridge Schema Validation Error');

			cb(e.message);
		}
	},

	async applyToInstance(data, logger, callback, app) {
		const NOT_FOUND_RESPONSE_CODE = 'NotFoundException';
		const BAD_REQUEST_RESPONSE_CODE = 'BadRequestException';
		const NOTHING_TO_UPDATE_RESPONSE_MESSAGE = 'Invalid request. Please provide at least one field to update.';
		if (!data.script) {
			return callback({ message: 'Empty script' });
		}

		logger.clear();
		logger.log('info', data, data.hiddenKeys);

		try {
			const { registry, schema } = getApiStatements(data.script);
			const schemasInstance = getSchemasInstance(data);
			
			if (registry) {
				try {
					if (registry.Description) {
						await schemasInstance.updateRegistry((getItemUpdateParamiters(registry))).promise();
					} else {
						await schemasInstance.describeRegistry({ RegistryName: registry.RegistryName }).promise();
					}
				} catch (err) {
					if (err.code === NOT_FOUND_RESPONSE_CODE) {
						await schemasInstance.createRegistry(registry).promise();
					} else {
						return callback(err);
					}
				}
			}
			if (schema) {
				try {
					await schemasInstance.updateSchema(getItemUpdateParamiters(schema)).promise();
				} catch (err) {
					if (err.code === NOT_FOUND_RESPONSE_CODE) {
						await schemasInstance.createSchema(schema).promise();
					}
					else {
						return callback(err);
					}
				}
			}
			callback();
		} catch(err) {
			callback(err);
		}
	},

	async testConnection(connectionInfo, logger, callback, app) {
		logger.log('info', connectionInfo, 'Test connection', connectionInfo.hiddenKeys);
		const schemasInstance = getSchemasInstance(connectionInfo);
		try {
			await schemasInstance.listRegistries().promise();
			callback();
		} catch (err) {
			logger.log('error', { message: err.message, stack: err.stack, error: err }, 'Connection failed');
			callback(err);
		}
	}
};

const addCommentsSigns = (string, format) => {
	const commentsStart = /hackoladeCommentStart\d+/i;
	const commentsEnd = /hackoladeCommentEnd\d+/i;
	const innerCommentStart = /hackoladeInnerCommentStart/i;
	const innerCommentEnd = /hackoladeInnerCommentEnd/i;
	
	const { result } = string.split('\n').reduce(({ isCommented, result }, line, index, array) => {
		if (commentsStart.test(line) || innerCommentStart.test(line)) {
			return { isCommented: true, result: result };
		}
		if (commentsEnd.test(line)) {
			return { isCommented: false, result };
		}
		if (innerCommentEnd.test(line)) {
			if (format === 'json') {
				array[index + 1] = '# ' + array[index + 1];
			}
			return { isCommented: false, result };
		}

		const isNextLineInnerCommentStart = index + 1 < array.length && innerCommentStart.test(array[index + 1]);
		if (isCommented || isNextLineInnerCommentStart) {
			result = result + '# ' + line + '\n';
		} else {
			result = result + line + '\n';
		}

		return { isCommented, result };
	}, { isCommented: false, result: '' });

	return result;
}

const removeCommentLines = (scriptString) => {
	const isCommentedLine = /^\s*#\s+/i;

	return scriptString
		.split('\n')
		.filter(line => !isCommentedLine.test(line))
		.join('\n')
		.replace(/(.*?),\s*(\}|])/g, '$1$2');
}

const buildAWSCLIScript = (modelMetadata, openAPISchema) => {
	const registryStatement = getRegistryCreateCLIStatement({ modelMetadata });
	const schemaStatement = getSchemaCreateCLIStatement({ openAPISchema, modelMetadata });
	return [registryStatement, schemaStatement].join('\n\n');
}

const getSchemasInstance = (connectionInfo) => {
	const { accessKeyId, secretAccessKey, region } = connectionInfo;
	aws.config.update({ accessKeyId, secretAccessKey, region });
	return new aws.Schemas({apiVersion: '2019-12-02'});
}
