#!/bin/bash
set -e

mkdir -p /opt/hadoop/data/dataNode
chown -R root:root /opt/hadoop/data || true

echo "Waiting for NameNode to be ready..."
until curl -sSf http://namenode:9870/ >/dev/null 2>&1; do
  echo "NameNode not ready yet, retrying in 2s..."
  sleep 2
done
echo "NameNode is up."

echo "Starting DataNode..."
hdfs datanode