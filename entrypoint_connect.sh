#!/bin/sh

kafka/bin/connect-standalone.sh kafka/config/connect-standalone.properties kafka/config/source_connector_config.properties kafka/config/sink_connector_config.properties
