"""
recommendation_engine.py
Graph-Based Recommendation System — Project 36
BDA Lab, Semester VI, RCOEM Nagpur
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import json, os, sys

os.environ["HADOOP_USER_NAME"] = "root"

os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.driver.extraJavaOptions=--add-opens=java.base/javax.security.auth=ALL-UNNAMED pyspark-shell"
def create_spark():
    spark = (SparkSession.builder
        .appName("GraphRec_Project36")
        .config("spark.sql.shuffle.partitions","8")
        .config("spark.driver.memory","2g")
        .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark

def load_data(spark, data_dir):
    rv_schema = StructType([
        StructField("review_id",StringType(),True), StructField("user_id",StringType(),True),
        StructField("product_id",StringType(),True), StructField("rating",IntegerType(),True),
        StructField("helpful_votes",IntegerType(),True), StructField("verified",StringType(),True)])
    pd_schema = StructType([
        StructField("product_id",StringType(),True), StructField("product_name",StringType(),True),
        StructField("category",StringType(),True), StructField("price",FloatType(),True)])
    us_schema = StructType([StructField("user_id",StringType(),True),StructField("username",StringType(),True)])
    reviews  = spark.read.csv(f"{data_dir}/reviews.csv",  header=True, schema=rv_schema)
    products = spark.read.csv(f"{data_dir}/products.csv", header=True, schema=pd_schema)
    users    = spark.read.csv(f"{data_dir}/users.csv",    header=True, schema=us_schema)
    return reviews, products, users

def build_user_product_graph(reviews):
    return (reviews
        .withColumn("verified_bonus", F.when(F.col("verified")=="True",0.5).otherwise(0.0))
        .withColumn("helpful_norm",   F.least(F.col("helpful_votes")/50.0, F.lit(0.5)))
        .withColumn("weight", F.col("rating")+F.col("verified_bonus")+F.col("helpful_norm"))
        .select("user_id","product_id","weight"))

def build_product_graph(edges):
    e1=edges.alias("e1"); e2=edges.alias("e2")
    co = (e1.join(e2,on="user_id")
        .filter(F.col("e1.product_id")!=F.col("e2.product_id"))
        .select(F.col("e1.product_id").alias("src"),F.col("e2.product_id").alias("dst"),
                (F.col("e1.weight")*F.col("e2.weight")).alias("co_weight")))
    return (co.groupBy("src","dst").agg(F.sum("co_weight").alias("edge_weight")))

def run_pagerank(product_graph, num_iterations=10, damping=0.85):
    out_weights = (product_graph.groupBy("src").agg(F.sum("edge_weight").alias("total_out_weight")))
    norm_edges = (product_graph
        .join(out_weights,on="src")
        .withColumn("norm_weight", F.col("edge_weight")/F.col("total_out_weight"))
        .select("src","dst","norm_weight").cache())
    nodes = (product_graph.select(F.col("src").alias("node"))
        .union(product_graph.select(F.col("dst").alias("node"))).distinct())
    n = nodes.count()
    ranks = nodes.withColumn("rank", F.lit(1.0/n))
    for _ in range(num_iterations):
        contribs = (norm_edges.join(ranks.withColumnRenamed("node","src"),on="src")
            .withColumn("contribution", F.col("rank")*F.col("norm_weight"))
            .groupBy("dst").agg(F.sum("contribution").alias("contrib_sum")))
        ranks = (nodes.join(contribs, nodes.node==contribs.dst, how="left")
            .withColumn("rank", F.lit(1-damping)/n + F.lit(damping)*F.coalesce(F.col("contrib_sum"),F.lit(0.0)))
            .select(F.col("node"),F.col("rank")))
    return ranks

def get_recommendations(user_id, edges, product_graph, pagerank_scores, products, top_n=10):
    already = set(r.product_id for r in edges.filter(F.col("user_id")==user_id).select("product_id").collect())
    if not already:
        recs = (pagerank_scores.join(products, pagerank_scores.node==products.product_id)
            .orderBy(F.col("rank").desc())
            .select("product_id","product_name","category",
                    F.col("price").cast("float"),
                    F.round("rank",6).alias("pagerank"),
                    F.lit(0.0).alias("co_score"),
                    F.round("rank",6).alias("final_score")).limit(top_n))
    else:
        co = (product_graph.filter(F.col("src").isin(already))
            .filter(~F.col("dst").isin(already))
            .groupBy("dst").agg(F.sum("edge_weight").alias("co_score")))
        recs = (co.join(pagerank_scores, co.dst==pagerank_scores.node, how="left")
            .withColumn("final_score", F.col("co_score")*F.coalesce(F.col("rank"),F.lit(0.0)))
            .join(products, co.dst==products.product_id)
            .orderBy(F.col("final_score").desc())
            .select("product_id","product_name","category",
                    F.col("price").cast("float"),
                    F.round("co_score",4).alias("co_score"),
                    F.round("rank",6).alias("pagerank"),
                    F.round("final_score",4).alias("final_score")).limit(top_n))
    return [r.asDict() for r in recs.collect()]

def get_top_products(pagerank_scores, products, n=20):
    rows = (pagerank_scores.join(products, pagerank_scores.node==products.product_id)
        .orderBy(F.col("rank").desc())
        .select("product_id","product_name","category",
                F.col("price").cast("float"),
                F.round("rank",6).alias("pagerank")).limit(n))
    return [r.asDict() for r in rows.collect()]

def get_category_stats(pagerank_scores, products):
    rows = (pagerank_scores.join(products, pagerank_scores.node==products.product_id)
        .groupBy("category")
        .agg(F.round(F.avg("rank"),6).alias("avg_pagerank"),
             F.round(F.max("rank"),6).alias("max_pagerank"),
             F.count("*").alias("product_count"))
        .orderBy(F.col("avg_pagerank").desc()))
    return [r.asDict() for r in rows.collect()]

def get_user_history(user_id, edges, products):
    rows = (edges.filter(F.col("user_id")==user_id)
        .join(products, "product_id")
        .select("product_id","product_name","category",
                F.col("price").cast("float"),
                F.round("weight",2).alias("weight"))
        .orderBy(F.col("weight").desc()))
    return [r.asDict() for r in rows.collect()]

def get_graph_stats(edges, product_graph, pagerank_scores):
    return {
        "total_users":    edges.select("user_id").distinct().count(),
        "total_products": edges.select("product_id").distinct().count(),
        "total_reviews":  edges.count(),
        "graph_edges":    product_graph.count(),
        "pagerank_nodes": pagerank_scores.count(),
    }

if __name__ == "__main__":
    user_id  = sys.argv[1] if len(sys.argv)>1 else "U0001"
    data_dir = sys.argv[2] if len(sys.argv)>2 else "data"
    spark = create_spark()
    reviews, products, users = load_data(spark, data_dir)
    edges    = build_user_product_graph(reviews)
    pg       = build_product_graph(edges)
    pr       = run_pagerank(pg)
    recs     = get_recommendations(user_id, edges, pg, pr, products)
    print(f"\nTop recommendations for {user_id}:")
    for r in recs: print(r)
    spark.stop()
