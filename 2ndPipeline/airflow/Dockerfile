FROM apache/airflow:2.9.3-python3.11


USER root
RUN apt-get update && \
    apt-get install -y \
        openjdk-17-jdk-headless \
        ant \
        procps \
        net-tools \
        wget \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


ENV SPARK_VERSION=3.5.3
ENV HADOOP_VERSION=3
RUN wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz -O /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION /opt/spark && \
    rm /tmp/spark.tgz

ENV SPARK_HOME=/opt/spark



ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}:${SPARK_HOME}/bin" 


RUN mkdir -p /etc/hadoop/conf
ENV HADOOP_CONF_DIR=/etc/hadoop/conf
ENV YARN_CONF_DIR=/etc/hadoop/conf


USER airflow


COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt


RUN java -version