#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Redpanda
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import argparse
import json
import logging
import os
import yaml

from custom_schema_registry_client import CustomSchemaRegistryClient
from custom_schema_registry_client import SchemaRegistryError

parser = argparse.ArgumentParser()
parser.add_argument('--config', help='configuration file', default='/app/conf/config.yaml')
args = parser.parse_args()
with open(args.config) as f:
    config = yaml.safe_load(f)

logfile = config["exporter"]["options"]["logfile"]
logging.basicConfig(filename=logfile, encoding='utf-8', level=logging.INFO)

logging.info('Exporter Started')
logging.info('Arguments ' + str(args))

logging.info('Config ' + str(config))

source = config["exporter"]["source"]
registry = CustomSchemaRegistryClient(source)

logging.info('Connected to Schema Registry at ' + source["url"])


def emit_schema(subject, version, schema, deleted, file):
    k = {"keytype": "SCHEMA", "subject": subject, "version": version, "magic": 1}
    v = {"subject": subject,
         "version": version,
         "id": schema.schema_id,
         "schemaType": schema.schema.schema_type,
         "references": schema.schema.references,
         "schema": schema.schema.schema_str, "deleted": deleted}
    if v.get("schemaType") == "AVRO":
        v.pop("schemaType")
    if len(v.get("references")) == 0:
        v.pop("references")
    record = {'key': k, 'value': v}
    file.write(json.dumps(record) + '\n')


def emit_compatibility(file, compatibility, subject=None):
    k = {"keytype": "CONFIG", "magic": 0}
    if subject is not None:
        k["subject"] = subject
    if compatibility is None:
        v = {"compatibilityLevel": "NONE"}
    else:
        v = {"compatibilityLevel": compatibility}
    record = {'key': k, 'value': v}
    file.write(json.dumps(record) + '\n')


def main():
    output_file = config["schemas"]
    if os.path.exists(output_file):
        logging.info("output file exists - exiting without exporting")
        print("output file exists - exiting without exporting")
    output = open(output_file, 'w')

    logging.info('Finding current compatibility level')
    current_compatibility = registry.get_compatibility()
    emit_compatibility(output, "NONE")

    exclude_deleted_subjects = config["exporter"]["options"]["exclude.deleted.subjects"]
    exclude_deleted_versions = config["exporter"]["options"]["exclude.deleted.versions"]

    (subjects, deleted_subjects) = registry.fetch_subjects(not exclude_deleted_subjects)

    logging.info('Found ' + str(len(subjects)) + ' active subjects for migration')
    logging.info('Active subjects:' + str(subjects))

    if not exclude_deleted_subjects:
        logging.info('Found ' + str(len(deleted_subjects)) + ' deleted subjects')
        logging.info('Deleted subjects: ' + str(deleted_subjects))

    all_subjects = [(s, False) for s in subjects] + [(s, True) for s in deleted_subjects]

    for subject, subject_deleted in all_subjects:
        logging.info('Finding versions for subject: ' + subject)
        (versions, deleted_versions) = registry.fetch_versions(subject,
                                                               not exclude_deleted_versions or subject_deleted)
        logging.info('  Found the following versions for "' + subject + '": ' + str(versions))
        all_versions = [(v, False) for v in versions] + [(v, True) for v in deleted_versions]
        for version, version_deleted in all_versions:
            schema = registry.fetch_version(subject, version, True)
            emit_schema(subject, version, schema, version_deleted, output)

        logging.info('Finding compatibility for subject: ' + subject)
        try:
            compatibility = registry.get_compatibility(subject)
            emit_compatibility(output, compatibility, subject)
        except SchemaRegistryError as e:
            if e.http_status_code == 404:
                pass
            else:
                raise e

    emit_compatibility(output, current_compatibility)
    output.close()


if __name__ == "__main__":
    main()
