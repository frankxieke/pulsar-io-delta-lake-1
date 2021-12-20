The Delta Lake connector is a Pulsar IO connector for copying data between Delta Lake and Pulsar. It contains two types of connectors:

***Delta Lake source connector***

This source connector can capture data changes from delta lake through [DSR](https://github.com/delta-io/connectors/wiki/Delta-Standalone-Reader) and writes data to Pulsar topics.

***Delta Lake sink connector***

This sink connector can consume pulsar topic data and write into delta lake through [DSW](https://github.com/delta-io/connectors/blob/6b468dabcbea5e24a8f81887d2f6e855b2b63ed5/standalone/src/main/java/io/delta/standalone/DeltaLog.java#L85) 
and users can use spark to process the delta lake table data further.


Currently, Delta Lake connector versions (x.y.z) are based on Pulsar versions (x.y.z).

| Delta connector version | Pulsar version | Doc |
| :--------------- | :------------------- | :------------------------------|
[2.9.0-rc-202110221101]()| [2.9.0-rc-202110221101]() | - [Delta Lake source connector]()<br><br>- [Delta sink connector]()


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


