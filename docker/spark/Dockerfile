FROM spark:3.5.6-python3

USER root

RUN mkdir -p \
    /opt/spark/app \
    /opt/spark/exports \
    /opt/spark/checkpoints \
    /tmp/spark-ivy \
    && chmod -R 777 \
    /opt/spark/exports \
    /opt/spark/checkpoints \
    /tmp/spark-ivy

COPY scripts/spark_streaming.py /opt/spark/app/spark_streaming.py

USER spark

CMD ["/opt/spark/bin/spark-submit", "--conf", "spark.jars.ivy=/tmp/spark-ivy", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6", "/opt/spark/app/spark_streaming.py"]