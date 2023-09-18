#!/usr/bin/bash

jq '. | {schema: tojson}' go/transform/schema.avsc | \
curl -X POST "http://localhost:18081/subjects/recon-value/versions" \
-H "Content-Type: application/vnd.schemaregistry.v1+json" \
-d @-