"""Build-time: download the Kafka connector jars matching pyspark 3.5.3 (Scala 2.12) into pyspark/jars."""
import os
import urllib.request

import pyspark

JARS = [
    "org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar",
    "org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar",
    "org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar",
    "org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar",
]
dest = os.path.join(os.path.dirname(pyspark.__file__), "jars")
for j in JARS:
    out = os.path.join(dest, os.path.basename(j))
    urllib.request.urlretrieve("https://repo1.maven.org/maven2/" + j, out)
    print("downloaded", out, os.path.getsize(out))
