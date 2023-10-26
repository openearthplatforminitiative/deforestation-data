#!/bin/bash
#
# File: sedona-init.sh
# Author: Erni Durdevic
# Created: 2021-11-01
# 
# On cluster startup, this script will copy the Sedona jars to the cluster's default jar directory.
# In order to activate Sedona functions, remember to add to your spark configuration the Sedona extensions: "spark.sql.extensions org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions"

cp /dbfs/FileStore/jars/sedona/1.5.0/*.jar /databricks/jars

