#!/bin/bash

java -Xmx2048m \
    -jar target/hermes-traffic-test-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
    --endpoint="failover:(<brokerURL>:61617)" \
    -pu="" \
    -pp="" \
    -cu="" \
    -cp="" \
    --topics=1 \
    --queues=2 \
    --consumers=2 \
    --consumer-delay=1 \
    --producer-delay=1