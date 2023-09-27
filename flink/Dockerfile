FROM flink:1.16.2-scala_2.12-java11

RUN wget -P /opt/sql-client/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.12/1.16.2/flink-sql-connector-kafka_2.12-1.16.2.jar; \
RUN wget -P /opt/sql-client/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.12/1.16.2/flink-sql-connector-kafka_2.12-1.16.2.jar; \
    wget -P /opt/sql-client/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/1.16.2/flink-json-1.16.2.jar; \
    wget -P /opt/sql-client/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-avro/1.16.2/flink-avro-1.16.2.jar; \
    wget -P /opt/sql-client/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-avro-glue-schema-registry_2.12/1.16.2/flink-avro-glue-schema-registry_2.12-1.16.2.jar; \
    wget -P /opt/sql-client/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-json-glue-schema-registry_2.12/1.16.2/flink-json-glue-schema-registry_2.12-1.16.2.jar; \
    wget -P /opt/sql-client/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-avro-confluent-schema-registry/1.16.2/flink-avro-confluent-schema-registry-1.16.2.jar; \
    wget -P /opt/sql-client/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-table-api-scala-bridge_2.12/1.16.2/flink-table-api-scala-bridge_2.12-1.16.2.jar;
# ADD jars/*.jar /opt/flink/lib/
