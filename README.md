The source code includes the reference project implementation code and it is a Maven project with the following modules.

* *hms-service-api*: the APIs between Lambda function and Athena service clients, which are defined in the HiveMetaStoreService interface. Since this is a service contract, please don’t change anything in this module.
* *hms-lambda-handler*: a set of default lambda handlers to process each hive metastore API calls. The class MetadataHandler is the dispatcher for all different API calls. Customer don’t need to change this package either.
* *hms-lambda-layer*: a Maven assembly project to put hms-sevice-api, hms-lambda-handler, and their dependencies into a zip file so that this zip file could be registered as a Lambda layer and then could be used by multiple Lambda functions.
* *hms-lambda-func: *an example Lambda function, where
    * *HiveMetaStoreLambdaFunc*: the example lambda function and it simply extends MetadataHandler.
    * *ThriftHiveMetaStoreClient*: a thrift client to communicate with hive metastore. This client is written for Hive 2.3.0. For other hive versions, customer might need to update this class to make sure the response objects are compatible.
    * *ThriftHiveMetaStoreClientFactory*: controls the behavior of the lambda function, for example, customer could provide their own set of HandlerProviders by overriding the getHandlerProvider() method.
    * hms.properties: Lambda function configuration. Most likely customer only need to update the following two properties
        * hive.metastore.uris: the URIs of the hive metastore, for example, *thrift://ip-172-31-11-81.ec2.internal:9083*
        * hive.metastore.response.spill.location: the s3 location to store response objects when their sizes exceed a given threshod, for example, 4MB. The threshold is defined in the property “hive.metastore.response.spill.threshold”, but we don’t recommend customer change the default value.
        * The two properties could be overridden by Lambda environment variables (https://docs.aws.amazon.com/lambda/latest/dg/env_variables.html) so that customer don’t need to recompile the source code for different Lambda functions with different properties.

Customer could choose to update the source code and build the artifacts from scratch. To do that, they need to have Apache Maven (https://maven.apache.org/) installed and then run the command “mvn install” to generate the layer zip file in the output folder called “target” in the module hms-lambda-layer and the lambda function jar in the module hms-lambd-func. Customer need to update the two properties, i.e., hive.metastore.uris and hive.metastore.response.spill.location in the file hms.properties in the hms-lambda-func module before they build the artifacts.

The artifacts consists of the following files

* hms-lambda-func-1.0-withdep.jar: an example Lambda function with all runtime dependencies, this jar can be used alone to define a lambda function
* hms-lambda-layer-1.0-athena.zip: the runtime library for Lambda functions as a Lambda layer (https://docs.aws.amazon.com/lambda/latest/dg/configuration-layers.html).
* hms-lambda-func-1.0.jar: an example lightweight Lambda function and it relies on the layer to provide Lambda runtime dependencies
