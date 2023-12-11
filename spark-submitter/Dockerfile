FROM bitnami/spark:3.5.0
USER root

ENV SPARK_MASTER_URL="spark://spark-master:7077"
ENV SPARK_APPLICATION_MAIN_CLASS1="myPackage.Job1"
ENV SPARK_APPLICATION_JAR_LOCATION1="/opt/spark/app/Job1-1.0-SNAPSHOT-jar-with-dependencies.jar"

ENV SPARK_APPLICATION_MAIN_CLASS2="myPackage.Job2"
ENV SPARK_APPLICATION_JAR_LOCATION2="/opt/spark/app/Job2-1.0-SNAPSHOT-jar-with-dependencies.jar"

COPY spark-submit.sh /opt/spark/app/spark-submit.sh

RUN chmod +x /opt/spark/app/spark-submit.sh

CMD [ "/opt/spark/app/spark-submit.sh" ]