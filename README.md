The Delta Lake connector is a Pulsar IO connector for copying data between Delta Lake and Pulsar. It contains two types of connectors:

Delta Lake source connector

This connector capture data changes from delta lake and writes data to Pulsar topics.


Currently, Delta Lake connector versions (x.y.z) are based on Pulsar versions (x.y.z).

| SQS connector version | Pulsar version | Doc |
| :---------- | :------------------- | :------------- |
[2.9.0]()| [2.9.0]() | - [Delta Lake source connector]()<br><br>- [Delta sink connector]()


## Project layout

Below are the sub folders and files of this project and their corresponding descriptions.

    ```bash
    ├── conf // stores configuration examples.
    ├── docs // stores user guides.
    ├── src // stores source codes.
    │   ├── checkstyle // stores checkstyle configuration files.
    │   ├── license // stores license headers. You can use `mvn license:format` to format the project with the stored license header.
    │   │   └── ALv2
    │   ├── main // stores all main source files.
    │   │   └── java
    │   ├── spotbugs // stores spotbugs configuration files.
    │   └── test // stores all related tests.
    │ 
    ```
## Build package

```bash
mvn package
```

1. Create Source Connector using create api

```bash
bin/pulsar-admin sources update \
--archive  {nar tarball path} \
--source-config-file config.json \
--classname org.apache.pulsar.ecosystem.io.deltalake.source.DeltaLakeConnectorSource \
--name {connectorName}
```

in config.json, you can define the source connector config.

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
    "startingVersion":"0",
    "tablePath": "{delta_path}",
    "fileSystemType": "filesystem or s3"
  }
}
```




