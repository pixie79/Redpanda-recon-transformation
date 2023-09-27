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

from schema_registry_client import SchemaRegistryClient, SchemaRegistryError
from schema_registry_client import _urlencode, RegisteredSchema, Schema


class CustomSchemaRegistryClient(SchemaRegistryClient):

    def __get_all_subjects(self):
        """
        Internal method that allows for all subjects to be retrieved from the Schema Registry, including deleted entries.

        Returns:
            list(str): Registered subject names

        Raises:
            SchemaRegistryError: if subjects can't be found

        See Also:
            `GET subjects API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions>`_
        """
        return self._rest_client.get('subjects?deleted=true')

    def __get_all_versions(self, subject_name):
        """
        Internal method that allows for all version IDs to be retrieved from the Schema Registry, including deleted entries.

        Args:
            subject_name (str): Subject name.

        Returns:
            list(int): Registered versions

        Raises:
            SchemaRegistryError: If subject can't be found

        See Also:
            `GET Subject Versions API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)-versions>`_
        """
        return self._rest_client.get('subjects/{}/versions?deleted=true'.format(_urlencode(subject_name)))

    def __get_deleted_version(self, subject_name, version):
        """
        Internal method that retrieves the full version definition, including versions that are deleted.

        Args:
            subject_name (str): Subject name.

            version (int): version number.

        Returns:
            RegisteredSchema: Registration information for this version.

        Raises:
            SchemaRegistryError: if the version can't be found or is invalid.

        See Also:
            `GET Subject Version API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions-(versionId-%20version)>`_
        """
        response = self._rest_client.get('subjects/{}/versions/{}?deleted=true'
                                         .format(_urlencode(subject_name),
                                                 version))

        schema_type = response.get('schemaType', 'AVRO')
        return RegisteredSchema(schema_id=response['id'],
                                schema=Schema(response['schema'],
                                              schema_type,
                                              response.get('references', [])),
                                subject=response['subject'],
                                version=response['version'])

    def delete_permanent_version(self, subject_name, version):
        response = self._rest_client.delete('subjects/{}/versions/{}?permanent=true'.
                                            format(_urlencode(subject_name),
                                                   version))
        return response

    def fetch_subjects(self, include_deleted):
        """
        Similar to registry.get_subjects(), except this adds the ability to get deleted subjects if required.

        Args:
            include_deleted (bool): Whether to retrieve deleted subjects.

        Returns:
            (list(str), list(str)): Registered subject names, split into two lists - active and deleted.

        Raises:
            SchemaRegistryError: if subjects can't be found

        See Also:
            `GET subjects API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions>`_
        """
        subjects = self.get_subjects()
        if include_deleted:
            all_subjects = self.__get_all_subjects()
            deleted_subjects = [x for x in all_subjects if x not in subjects]
            return subjects, deleted_subjects
        else:
            return subjects, []

    def fetch_versions(self, subject, include_deleted):
        """
        Similar to registry.versions(), except this adds the ability to get deleted versions if required.
        As with fetch_subjects, this method splits the results into two lists, based on whether they are deleted.

        Args:
            subject (str): Subject name.
            include_deleted (bool): Whether to retrieve deleted versions.

        Returns:
            (list(int), list(int)): Registered versions, split into two lists - active and deleted.

        Raises:
            SchemaRegistryError: If subject can't be found

        See Also:
            `GET Subject Versions API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)-versions>`_
        """
        try:
            versions = self.get_versions(subject)
        except SchemaRegistryError as e:
            if 'not found.' in e.error_message:
                versions = []
            else:
                raise e
        if include_deleted:
            all_versions = self.__get_all_versions(subject)
            deleted_versions = [x for x in all_versions if x not in versions]
            return versions, deleted_versions
        else:
            return versions, []

    def fetch_version(self, subject, version, include_deleted):
        """
        Similar to get_version, except that this method adds the ability to get deleted versions if required.

        Args:
            subject (str): Subject name.
            version (int): version number.
            include_deleted (bool): Whether to retrieve deleted versions.

        Returns:
            RegisteredSchema: Registration information for this version.

        Raises:
            SchemaRegistryError: if the version can't be found or is invalid.

        See Also:
            `GET Subject Version API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions-(versionId-%20version)>`_
        """
        if include_deleted:
            return self.__get_deleted_version(subject, version)
        else:
            return self.get_version(subject, version)