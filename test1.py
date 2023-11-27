import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkUnion(glueContext, unionType, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(
        "(select * from source1) UNION " + unionType + " (select * from source2)"
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1697734605019 = glueContext.create_dynamic_frame.from_catalog(
    database="swm-raw",
    table_name="race_tw_2023_05_24_2023_05_31_csv",
    transformation_ctx="AmazonS3_node1697734605019",
)

# Script generated for node Amazon S3
AmazonS3_node1697699297595 = glueContext.create_dynamic_frame.from_catalog(
    database="swm-raw",
    table_name="race_tw_2023_05_15_2023_05_23_csv",
    transformation_ctx="AmazonS3_node1697699297595",
)

# Script generated for node Amazon S3
AmazonS3_node1697734574441 = glueContext.create_dynamic_frame.from_catalog(
    database="swm-raw",
    table_name="race_tw_2023_05_01_2023_05_14_csv",
    transformation_ctx="AmazonS3_node1697734574441",
)

# Script generated for node Change Schema
ChangeSchema_node1697734712996 = ApplyMapping.apply(
    frame=AmazonS3_node1697734605019,
    mappings=[
        ("parent_tweet_id", "long", "parent_tweet_id", "long"),
        ("tweet_text", "string", "text", "string"),
        ("language", "string", "language", "string"),
    ],
    transformation_ctx="ChangeSchema_node1697734712996",
)

# Script generated for node Change Schema
ChangeSchema_node1697699302325 = ApplyMapping.apply(
    frame=AmazonS3_node1697699297595,
    mappings=[
        ("parent_tweet_id", "long", "parent_tweet_id", "long"),
        ("tweet_text", "string", "text", "string"),
        ("language", "string", "language", "string"),
    ],
    transformation_ctx="ChangeSchema_node1697699302325",
)

# Script generated for node Change Schema
ChangeSchema_node1697734632641 = ApplyMapping.apply(
    frame=AmazonS3_node1697734574441,
    mappings=[
        ("parent_tweet_id", "long", "parent_tweet_id", "long"),
        ("tweet_text", "string", "text", "string"),
        ("language", "string", "language", "string"),
    ],
    transformation_ctx="ChangeSchema_node1697734632641",
)

# Script generated for node Union
Union_node1698180615722 = sparkUnion(
    glueContext,
    unionType="DISTINCT",
    mapping={
        "source1": ChangeSchema_node1697699302325,
        "source2": ChangeSchema_node1697734632641,
    },
    transformation_ctx="Union_node1698180615722",
)

# Script generated for node Union
Union_node1698180740742 = sparkUnion(
    glueContext,
    unionType="DISTINCT",
    mapping={
        "source1": Union_node1698180615722,
        "source2": ChangeSchema_node1697734712996,
    },
    transformation_ctx="Union_node1698180740742",
)

# Script generated for node SQL Query
SqlQuery14 = """
select * from myDataSource
where language=='en'  ;
"""
SQLQuery_node1698183083831 = sparkSqlQuery(
    glueContext,
    query=SqlQuery14,
    mapping={"myDataSource": Union_node1698180740742},
    transformation_ctx="SQLQuery_node1698183083831",
)

# Script generated for node Change Schema
ChangeSchema_node1698181732403 = ApplyMapping.apply(
    frame=SQLQuery_node1698183083831,
    mappings=[
        ("parent_tweet_id", "long", "parent_tweet_id", "long"),
        ("text", "string", "text", "string"),
    ],
    transformation_ctx="ChangeSchema_node1698181732403",
)

# Script generated for node Amazon S3
AmazonS3_node1698181238660 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1698181732403,
    connection_type="s3",
    format="csv",
    connection_options={"path": "s3://swm-data-raw/clean/", "partitionKeys": []},
    transformation_ctx="AmazonS3_node1698181238660",
)

job.commit()
