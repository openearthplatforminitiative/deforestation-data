-- Databricks notebook source
-- MAGIC %md
-- MAGIC Install Apache Sedona following this guide: https://sedona.apache.org/latest-snapshot/setup/databricks/

-- COMMAND ----------

-- MAGIC %sh 
-- MAGIC # Create JAR directory for Sedona
-- MAGIC mkdir -p /dbfs/FileStore/jars/sedona/1.5.0
-- MAGIC
-- MAGIC # Download the dependencies from Maven into DBFS
-- MAGIC curl -o /dbfs/FileStore/jars/sedona/1.5.0/geotools-wrapper-1.5.0-28.2.jar "https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/1.5.0-28.2/geotools-wrapper-1.5.0-28.2.jar"
-- MAGIC
-- MAGIC curl -o /dbfs/FileStore/jars/sedona/1.5.0/sedona-spark-shaded-3.0_2.12-1.5.0.jar "https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.0_2.12/1.5.0/sedona-spark-shaded-3.0_2.12-1.5.0.jar"
-- MAGIC
-- MAGIC curl -o /dbfs/FileStore/jars/sedona/1.5.0/sedona-viz-3.0_2.12-1.5.0.jar "https://repo1.maven.org/maven2/org/apache/sedona/sedona-viz-3.0_2.12/1.5.0/sedona-viz-3.0_2.12-1.5.0.jar"

-- COMMAND ----------

-- MAGIC %sh 
-- MAGIC
-- MAGIC # Create init script directory for Sedona
-- MAGIC mkdir -p /dbfs/FileStore/sedona/
-- MAGIC
-- MAGIC # Create init script
-- MAGIC cat > ./sedona-init.sh <<'EOF'
-- MAGIC #!/bin/bash
-- MAGIC #
-- MAGIC # File: sedona-init.sh
-- MAGIC # Author: Erni Durdevic
-- MAGIC # Created: 2021-11-01
-- MAGIC # 
-- MAGIC # On cluster startup, this script will copy the Sedona jars to the cluster's default jar directory.
-- MAGIC # In order to activate Sedona functions, remember to add to your spark configuration the Sedona extensions: "spark.sql.extensions org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions"
-- MAGIC
-- MAGIC cp /dbfs/FileStore/jars/sedona/1.5.0/*.jar /databricks/jars
-- MAGIC
-- MAGIC EOF

-- COMMAND ----------


