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
import logging as logging
import yaml

from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument('--config', help='configuration file', default='/app/conf/config.yaml')
args = parser.parse_args()

with open(args.config) as f:
    config = yaml.safe_load(f)

logfile = config["importer"]["options"]["logfile"]
logging.basicConfig(filename=logfile, encoding='utf-8', level=logging.INFO)

logging.info('Schema Import Started')
logging.info('Arguments ' + str(args))
logging.info('Config ' + str(config))

conf = config["importer"]["target"]
producer = KafkaProducer(**conf)
logging.info('Connected to brokers: ' + config["importer"]["target"]["bootstrap_servers"])

input_file = config["schemas"]
topic = config["importer"]["options"]["topic"]

count = 0
with open(input_file) as f:
    for line in f:
        record = json.loads(line)
        count = count + 1
        k = json.dumps(record.get('key')).encode('utf-8')
        v = json.dumps(record.get('value')).encode('utf-8')
        producer.send(topic=topic, key=k, value=v)

logging.info('Wrote ' + str(count) + ' messages into ' + topic)
producer.flush()
logging.info('Flushed producer')
