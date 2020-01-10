# Amazon Athena Hive Metastore Lambda Function

The Lambda function enables Amazon Athena to communicate with your hive metastore to run Athena queries with database metadata defined in your Hive Metastore.

Two Hive Metastore SAR (Serverless Application Repo) applications are provided for you to create lambda functions, i.e.,
1. **AthenaHiveMetastoreFunction** - create a lambda function by using the uber jar from the module hms-lambda-func.
2. **AthenaHiveMetastoreFunctionWithLayer** - first create a lambda layer by using the jar from the module hms-lambda-layer, then create a lambda function by using the thin jar from the module hms-lambda-func.

## Usage

### Parameters

The two applications expose the same configuration options as follows.

1. **LambdaFuncName** - The lambda function name.
2. **SpillLocation** - Defaults to sub-folder in your bucket called 'athena-hms-spill'. The S3 location where this function can spill metadata responses. You should configure an S3 lifecycle on this location to delete old spills after X days/Hours.
3. **HMSUris** - Hive metastore URIs.
4. **LambdaMemory** - (Optional) Defaults to 1024 MB, Lambda memory in MB (min 128 - 3008 max). 
5. **LambdaTimeout** - (Optional) Defaults to 300 seconds, Maximum Lambda invocation runtime in seconds. (min 1 - 900 max)
6. **VPCSecurityGroupIds** - Comma separated VPC security groups IDs where hive metastore is in.
7. **VPCSubnetIds** - Comma separated VPC subnet IDs where hive metastore is in.

### Required Permissions

Please review the "Policies" subsection in the yaml files. A brief summary is below.
1. **Lambda invocation**, i.e., lambda:GetFunction and lambda:InvokeFunction (lambda:GetLayerVersion for lambda function with layers)
2. **Lambda cloudwatch logs**, i.e., logs:CreateLogGroup, logs:CreateLogStream, and logs:PutLogEvents 
3. **S3 read and write permissions on the s3 spill location**, i.e., s3:GetObject, s3:GetBucketLocation, s3:ListBucket, and s3:PutObject
4. **VPCAccessPolicy**, access to create, delete, describe, and detach Elastic Network Interfaces to connect to customer's VPC.

## License

This project is licensed under the Apache-2.0 License.