# How to get 
- Download the NAR package.
   
  The Delta Lake connector nar tarball currently is not available for download. You can build it from the source code.
  
- Build it from the source code. 
  1. install delta standalone jar in local environment
Because the ***io.delta:delta-standalone_2.12:0.2.1-SNAPSHOT*** is not released, so you can install the jar into your local
maven through [delta connector build script](https://github.com/delta-io/connectors/blob/6b468dabcbea5e24a8f81887d2f6e855b2b63ed5/build.sbt#L376).
  
  2. build delta lake connector
    ```bash
    mvn package -DskipTests
    ```

    After the connector is successfully built, a `NAR` package is generated under the `target` directory. 
    The `NAR` tarball file name is `pulsar-io-delta-lake-2.9.0.nar` 

# How to configure
Before using the Delta Lake source connector, you need to configure it.

You can create a configuration file (JSON) to set the following properties.

| Name | Type|Required | Default | Description
|------|----------|----------|---------|-------------|
| `tenant` |String| true | "" (empty string) | Pulsar tenant name. |
| `namespace` | String| true | " " (empty string) | Pulsar namespace name. |
| `topicName` | String|true | " " (empty string) | Pulsar topic name, the delta record will produce into this topic. |
| `name` | String|false | " " (empty string) | Source connector name. |
| `parallelism` | Int|true | " " (empty string) | Number of source connector instances, each instance will run on a function worker. |
| `processingGuarantees` | Int|true | " " (empty string) | Process gurantees. 2 means EFFECTIVE_ONCE |
| `tablePath` | String|true | " " (empty string) | Delta table path for example: /tmp/delta_test or s3a://bucketname/ |
| `fileSystemType` | String|true | " " (empty string) | Storage type, `filesystem` or `s3` |
| `s3aAccesskey` | String|true | " " (empty string) | If storage type is `s3` s3a [Accesskey](https://aws.amazon.com/cn/console/) |
| `s3aSecretKey` | String|true | " " (empty string) | If storage type is `s3` s3a [SecretKey](https://aws.amazon.com/cn/console/) |
| `startingVersion` | Int|true | 0 | Delta snapshot version to start to capture data change, `startingVersion` and `startingTimeStamp`, you can only configure one |
| `startingTimeStamp` | String|true | "" | Delta snapshot timestamp to start to capture data change, for example `2021-09-29T20:17:46.384Z`, `startingVersion` and `startingTimeStamp`, you can only configure one  |
| `includeHistoryData` | Boolean |true | false | If we should include history data in the table, if `false` only capture data changes, if `true` will read all delta table history data|


```bash
{
  "tenant": "public",
  "namespace": "default",
  "name": "{connectorName}",
  "topicName": "{topicName}",
  "parallelism": 1,
  "processingGuarantees": 2,
  "configs":
  {
    "tablePath": "{delta_path}",
    "fileSystemType": "filesystem or s3",
    "s3aAccesskey":"{s3a access key}",
    "s3aSecretKey":"{s3a secrect key}",
    "startingVersion": {delta snapshot version number},
    "startingTimeStamp"：{delta snapshot timetstamp },
  }
}
```

# How to use
You can use the Delta lake source connector as a non built-in connector or a built-in connector as below. 

## Use as non built-in connector 
Supposing you have a pulsar cluster, we can create source using following steps 

* Enable function-worker in broker
```bash
functionsWorkerEnabled=true
```
* Enable statestore in function_worker.yml
```bash
stateStorageServiceUrl: bk://127.0.0.1:6282
```
* Create Source Connector using pulsar-admin sources create api,in config.json, 
the format is define above, you can define the source connector config.***

```bash
bin/pulsar-admin sources create \
--archive  {nar tarball path} \
--source-config-file {config json path} \
--classname org.apache.pulsar.ecosystem.io.deltalake.DeltaLakeConnectorSource \
--name {connectorName}
```

## Use as built-in connector
You can make the Delta source connector as a built-in connector and use it on a standalone cluster, on-premises cluster, or K8S cluster.

### Standalone cluster

### on-premises cluster

### K8S cluster
