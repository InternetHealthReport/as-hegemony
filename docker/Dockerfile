FROM caida/bgpstream:latest

RUN apt-get update -y

RUN curl -X GET https://codeload.github.com/edenhill/librdkafka/tar.gz/v1.5.0 -o /tmp/librdkafka-1.5.0.tar.gz

RUN apt-get install python3-pip libpq-dev -y

RUN cd /tmp && \
    tar -xvf librdkafka-1.5.0.tar.gz && \
    cd librdkafka-1.5.0/ && \
    ./configure && \
    make && \
    make install && \
    rm -rf /tmp/librdkafka-1.5.0 /tmp/librdkafka-1.5.0.tar.gz

RUN pip3 install --no-cache-dir \
    confluent-kafka \
    fastapi \
    minio \
    uvicorn \
    psycopg2 \
    py-radix \
    msgpack \
    scipy
