FROM flink:1.20.1


# install python3 and pip3
RUN apt-get update -y && \
apt-get install -y python3 python3-pip python3-dev && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python
# install PyFlink
RUN pip install apache-flink==2.0.0

ADD src/ .
ADD test_data/ test_data/
ADD jars/* /opt/flink/lib/