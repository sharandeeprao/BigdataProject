from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col , when
from pyspark.sql.types import StructType, StructField, StringType, MapType

# # Initialize Spark Session
# spark = SparkSession.builder \
#     .appName("KafkaToSparkConsole") \
#     .getOrCreate()

mysql_jdbc_jar_path = '/home/sharan-rao/mysql-connector-j-9.0.0/mysql-connector-j-9.0.0.jar '

# Initialize Spark Session with JDBC driver
spark = SparkSession.builder \
    .appName("KafkaToMySQL") \
    .config("spark.jars", mysql_jdbc_jar_path) \
    .getOrCreate()

# Kafka parameters
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'test-topic'

# Read data from Kafka
kafka_stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Kafka data has keys and values in binary format; convert them to string
kafka_stream_df = kafka_stream_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Define schema for JSON data
json_schema = StructType([
    StructField('_index', StringType(), True),
    StructField('_type', StringType(), True),
    StructField('_score', StringType(), True),
    StructField('_source', StructType([
        StructField('layers', StructType([
            StructField('frame', StructType([
                StructField('frame.section_number', StringType(), True),
                StructField('frame.interface_id', StringType(), True),
                StructField('frame.interface_id_tree', StructType([
                    StructField('frame.interface_name', StringType(), True)
                ]), True),
                StructField('frame.encap_type', StringType(), True),
                StructField('frame.time', StringType(), True),
                StructField('frame.offset_shift', StringType(), True),
                StructField('frame.time_epoch', StringType(), True),
                StructField('frame.time_delta', StringType(), True),
                StructField('frame.time_delta_displayed', StringType(), True),
                StructField('frame.time_relative', StringType(), True),
                StructField('frame.number', StringType(), True),
                StructField('frame.len', StringType(), True),
                StructField('frame.cap_len', StringType(), True),
                StructField('frame.marked', StringType(), True),
                StructField('frame.ignored', StringType(), True),
                StructField('frame.protocols', StringType(), True),
            ]), True),
            StructField('eth', StructType([
                StructField('eth.dst', StringType(), True),
                StructField('eth.dst_tree', StructType([
                    StructField('eth.dst_resolved', StringType(), True),
                    StructField('eth.dst.oui', StringType(), True),
                    StructField('eth.dst.oui_resolved', StringType(), True),
                    StructField('eth.addr', StringType(), True),
                    StructField('eth.addr_resolved', StringType(), True),
                    StructField('eth.addr.oui', StringType(), True),
                    StructField('eth.addr.oui_resolved', StringType(), True),
                    StructField('eth.dst.lg', StringType(), True),
                    StructField('eth.lg', StringType(), True),
                    StructField('eth.dst.ig', StringType(), True),
                    StructField('eth.ig', StringType(), True),
                ]), True),
                StructField('eth.src', StringType(), True),
                StructField('eth.src_tree', StructType([
                    StructField('eth.src_resolved', StringType(), True),
                    StructField('eth.src.oui', StringType(), True),
                    StructField('eth.src.oui_resolved', StringType(), True),
                    StructField('eth.addr', StringType(), True),
                    StructField('eth.addr_resolved', StringType(), True),
                    StructField('eth.addr.oui', StringType(), True),
                    StructField('eth.addr.oui_resolved', StringType(), True),
                    StructField('eth.src.lg', StringType(), True),
                    StructField('eth.lg', StringType(), True),
                    StructField('eth.src.ig', StringType(), True),
                    StructField('eth.ig', StringType(), True),
                ]), True),
                StructField('eth.type', StringType(), True),
            ]), True),
            StructField('ip', StructType([
                StructField('ip.version', StringType(), True),
                StructField('ip.hdr_len', StringType(), True),
                StructField('ip.dsfield', StringType(), True),
                StructField('ip.dsfield_tree', StructType([
                    StructField('ip.dsfield.dscp', StringType(), True),
                    StructField('ip.dsfield.ecn', StringType(), True),
                ]), True),
                StructField('ip.len', StringType(), True),
                StructField('ip.id', StringType(), True),
                StructField('ip.flags', StringType(), True),
                StructField('ip.flags_tree', StructType([
                    StructField('ip.flags.rb', StringType(), True),
                    StructField('ip.flags.df', StringType(), True),
                    StructField('ip.flags.mf', StringType(), True),
                ]), True),
                StructField('ip.frag_offset', StringType(), True),
                StructField('ip.ttl', StringType(), True),
                StructField('ip.proto', StringType(), True),
                StructField('ip.checksum', StringType(), True),
                StructField('ip.checksum.status', StringType(), True),
                StructField('ip.src', StringType(), True),
                StructField('ip.addr', StringType(), True),
                StructField('ip.src_host', StringType(), True),
                StructField('ip.host', StringType(), True),
                StructField('ip.dst', StringType(), True),
                StructField('ip.dst_host', StringType(), True),
            ]), True),
            StructField('udp', StructType([
                StructField('udp.srcport', StringType(), True),
                StructField('udp.dstport', StringType(), True),
                StructField('udp.port', StringType(), True),
                StructField('udp.length', StringType(), True),
                StructField('udp.checksum', StringType(), True),
                StructField('udp.checksum.status', StringType(), True),
                StructField('udp.stream', StringType(), True),
                StructField('Timestamps', StructType([
                    StructField('udp.time_relative', StringType(), True),
                    StructField('udp.time_delta', StringType(), True),
                ]), True),
                StructField('udp.payload', StringType(), True),
            ]), True),
            StructField('tcp', StructType([
                StructField('tcp.srcport', StringType(), True),
                StructField('tcp.dstport', StringType(), True),
                StructField('tcp.port', StringType(), True),
                StructField('tcp.length', StringType(), True),
                StructField('tcp.checksum', StringType(), True),
                StructField('tcp.checksum.status', StringType(), True),
                StructField('tcp.stream', StringType(), True),
                StructField('Timestamps', StructType([
                    StructField('tcp.time_relative', StringType(), True),
                    StructField('tcp.time_delta', StringType(), True),
                ]), True),
                StructField('tcp.payload', StringType(), True),
            ]), True),
            StructField('nbns', StructType([
                StructField('nbns.id', StringType(), True),
                StructField('nbns.flags', StringType(), True),
                StructField('nbns.flags_tree', StructType([
                    StructField('nbns.flags.response', StringType(), True),
                    StructField('nbns.flags.opcode', StringType(), True),
                    StructField('nbns.flags.truncated', StringType(), True),
                    StructField('nbns.flags.recdesired', StringType(), True),
                    StructField('nbns.flags.broadcast', StringType(), True),
                ]), True),
                StructField('nbns.count.queries', StringType(), True),
                StructField('nbns.count.answers', StringType(), True),
                StructField('nbns.count.auth_rr', StringType(), True),
                StructField('nbns.count.add_rr', StringType(), True),
                StructField('Queries', MapType(StringType(), StructType([
                    StructField('nbns.name', StringType(), True),
                    StructField('nbns.type', StringType(), True),
                    StructField('nbns.class', StringType(), True)
                ])), True),
                StructField('Additional records', MapType(StringType(), StructType([
                    StructField('nbns.name', StringType(), True),
                    StructField('nbns.type', StringType(), True),
                    StructField('nbns.class', StringType(), True),
                    StructField('nbns.ttl', StringType(), True),
                    StructField('nbns.data_length', StringType(), True),
                    StructField('nbns.nb_flags', StringType(), True),
                    StructField('nbns.nb_flags_tree', StructType([
                        StructField('nbns.nb_flags.group', StringType(), True),
                        StructField('nbns.nb_flags.ont', StringType(), True)
                    ]), True),
                    StructField('nbns.addr', StringType(), True)
                ])), True),
            ]), True)
        ]), True)
    ]), True)
])

# Parse JSON data using schema
parsed_df = kafka_stream_df.select(from_json(col("value"), json_schema).alias("data"))

# Select everything under the 'ip' field
ip_df = parsed_df.select(
    col("data._source.layers.ip.`ip.src`").alias("ip_src"),
    col("data._source.layers.ip.`ip.dst`").alias("ip_dst"),
    col("data._source.layers.eth.`eth.src`").alias("mac_src"),
    col("data._source.layers.eth.`eth.dst`").alias("mac_dst"),
    # Use when clause to handle both UDP and TCP dynamically
    when(col("data._source.layers.udp").isNotNull(), col("data._source.layers.udp.`udp.srcport`")).otherwise(col("data._source.layers.tcp.`tcp.srcport`")).alias("srcport"),
    when(col("data._source.layers.udp").isNotNull(), col("data._source.layers.udp.`udp.dstport`")).otherwise(col("data._source.layers.tcp.`tcp.dstport`")).alias("dstport")
)

# # Write to console
# query = ip_df \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

# # Await termination
# query.awaitTermination()


# Define MySQL connection properties
mysql_url = 'jdbc:mysql://localhost:3306/networkdb'
mysql_properties = {
    'user': 'root',
    'password': 'Sharan@2000',
    'driver': 'com.mysql.cj.jdbc.Driver'
}

# Function to write each batch to MySQL
def write_to_mysql(batch_df, batch_id):
    batch_df.write \
        .jdbc(url=mysql_url, table='network_data', mode='append', properties=mysql_properties)

# Write to MySQL
query = ip_df.writeStream \
    .foreachBatch(write_to_mysql) \
    .outputMode("append") \
    .start()

# Await termination
query.awaitTermination()
