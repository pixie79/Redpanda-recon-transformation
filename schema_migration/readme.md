# Schema Migration Tool

This tool helps migrate schemas from one Schema Registry to another, in a way that allows direct control of version
numbers and schema IDs.

# Dependencies

This tool has the following dependencies:

### Linux
- Python 3.9: `apt install python3`

### Python Libraries
- fastavro: `pip3 install fastavro`
- requests: `pip3 install requests`
- kafka-python: `pip3 install kafka-python`

# Schema Export

`exporter.py` connects to the source Schema Registry instance and uses the REST API to extract all schemas and subjects.
These are written out to an intermediate file, allowing the schemas to be stored in version control, transferred
remotely, etc.

###  Usage:
```shell
usage: python3 exporter.py --config config.yaml
```

### Configuration

`exporter.py` uses YAML for configuration - examples are available in the `conf/` directory.

An example config is below:

```yaml
exporter:
  source:
    url: https://localhost:8081/ # Mandatory
    ssl.key.location: client.key
    ssl.certificate.location: client.cert
    ssl.ca.location: ca.cert
  options:
    exclude.deleted.versions: false # Mandatory
    exclude.deleted.subjects: false # Mandatory
    logfile: export.log # Mandatory

schemas: exported-schemas.txt # Mandatory
```

Under `source:`, the URL is mandatory. The TLS parameters can be removed when connecting to an insecure Schema Registry
instance.

By default, `exporter.py` will extract all subjects and versions, *including* those which have been *soft-deleted*.
Soft-deleted items can be excluded by setting the appropriate option.

**Hard-deleted schema versions and subjects are permanently removed by Schema Registry. The migration tool can not
retrieve these.**

The `schemas:` configuration defines where the exported schemas will be stored. This configuration is also used by 
`importer.py`.

# Schema Import

`importer.py` reads messages from the intermediate file (written by `exporter.py`) and writes them to the specified
schemas topic on the target Redpanda cluster. These messages are then read by Schema Registry and made available via
REST.

### Usage

```shell
usage: python3 importer.py --config config.yaml
```

### Configuration

`importer.py` uses YAML for configuration - examples are available in the `conf/` directory.

An example config is below:

```yaml
schemas: exported-schemas.txt # Mandatory

importer:
  target:
    bootstrap_servers: localhost:9092 # Mandatory
    security_protocol: SASL_SSL
    sasl_mechanism: SCRAM-SHA-256
    sasl_username: username
    sasl_password: password
  options:
    topic: _schemas # Mandatory
    logfile: import.log # Mandatory
```

As with `exporter.py`, the input file (containing schemas to import) is specified using the `schemas:` property and is
mandatory.

The Kafka producer configuration uses a minimum of `bootstrap_servers` in order to connect. For secure Redpanda
clusters, the remaining security properties are also necessary.

# Considerations

## Idempotency

From limited testing, it appears that Redpanda Schema Registry will accept duplicate messages without issue, so running the migration
import tool multiple times should work.

## Migration Process

Once a Schema Registry instance begins becomes active and begins to assign new schema IDs for itself, further migrations would be inadvisable.

If further export / import processes are performed, there is a risk that the import would write a schema with an ID that
has already been used on the target registry. If there is an ID clash, existing message data may become unreadable.

You have been warned!