from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, IntegerType, StringType, BooleanType, DoubleType
from pyspark.sql.functions import (
    col, lit, when,to_date,
    first, collect_set, max as smax,
    array_contains, lpad,sum as ssum
)
spark = SparkSession.builder.appName('akshays_session') \
    .config('spark.sql.catalogImplementation', 'hive') \
    .config('hive.metastore.connect.retries', 15) \
    .config('hive.metastore.client.factory.class','com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory') \
    .enableHiveSupport() \
    .getOrCreate();

df=spark.table('snapchat_onprem.ww_snapchat_onprem')

adv = spark.table('firewall.partner_measured_advertiser_string')
camp = spark.table('firewall.partner_measured_campaign_string')
cre = spark.table('firewall.partner_measured_creative_string')
pla = spark.table('firewall.partner_measured_placement_string')

final  = df.join(adv, df.advertiserid == adv.ID,how='left') \
    .withColumn("advertiserid",col("PARTNER_MEASURED_ADVERTISER_ID")) \
    .drop('PARTNER_MEASURED_ADVERTISER_ID','MEASUREMENT_SOURCE_ID','ID') \
    .join(camp, df.campaignid == camp.ID,how='left') \
    .withColumn("campaignid",col("PARTNER_MEASURED_CAMPAIGN_ID")) \
    .drop('PARTNER_MEASURED_CAMPAIGN_ID','MEASUREMENT_SOURCE_ID','ID') \
    .join(pla, df.placementid == pla.ID,how='left') \
    .withColumn("placementid",col("PARTNER_MEASURED_PLACEMENT_ID")) \
    .drop('PARTNER_MEASURED_PLACEMENT_ID','MEASUREMENT_SOURCE_ID','ID') \
    .join(cre, df.creativeid == cre.ID,how='left') \
    .withColumn("creativeid",col("PARTNER_MEASURED_CREATIVE_ID")) \
    .drop('PARTNER_MEASURED_CREATIVE_ID','MEASUREMENT_SOURCE_ID','ID') \

final.write \
    .mode('overwrite') \
    .partitionBy('utcdate', 'hour') \
    .format('json') \
    .save("s3://ww-akshay-test/mapping_data/")