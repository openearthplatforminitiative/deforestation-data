# Databricks notebook source
import pandas as pd
import geopandas as gpd
import json

# COMMAND ----------

with open("config.json") as f:
    config = json.load(f)

# COMMAND ----------


