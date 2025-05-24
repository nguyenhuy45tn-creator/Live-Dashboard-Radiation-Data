FROM flink:1.17.2

# Set environment variables
ENV KAFKA_VERSION=3.1.0
ENV KAFKA_PYTHON=2.0.2
ENV CONFLUENT_KAFKA_VERSION=2.3.0
ENV PYFLINK_VERSION=1.17.2
ENV FLINK_VERSION=1.17.2

# install python3 and pip3
RUN apt-get update -y && \
apt-get install -y python3 python3-pip python3-dev && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

# install required packages
RUN pip3 install apache-flink==${PYFLINK_VERSION} && \
    pip3 install confluent-kafka==${CONFLUENT_KAFKA_VERSION} && \
    pip3 install kafka-python==${KAFKA_PYTHON}

# Download Flink Kafka connectors
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/${FLINK_VERSION}/flink-connector-kafka-${FLINK_VERSION}.jar /opt/flink/lib/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_VERSION}/kafka-clients-${KAFKA_VERSION}.jar /opt/flink/lib/
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_VERSION}/flink-sql-connector-kafka-${FLINK_VERSION}.jar /opt/flink/lib/

ADD src/ .
ADD test_data/ test_data/
