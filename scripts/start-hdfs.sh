#!/bin/bash
echo "Starting HDFS NameNode..."
set -e

mkdir -p /opt/hadoop/data/nameNode
mkdir -p /opt/hadoop/data/tmp
chown -R root:root /opt/hadoop/data || true

if [ ! -d "/opt/hadoop/data/nameNode/current" ]; then
  echo "Formatting NameNode..."
  hdfs namenode -format -force -nonInteractive
else
  echo "NameNode already formatted, skipping."
fi

echo "Starting NameNode..."
hdfs namenode