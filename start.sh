#!/bin/bash

docker-compose down
rm -r ./out_data/job1
rm -r ./out_data/job2
rm -r ./out_data/output_join
docker create network spark-network
docker-compose build
docker-compose up &

sleep 80
docker cp spark-worker-1:/opt/bitnami/spark/job1_out/ ./out_data/job1/
docker cp spark-worker-1:/opt/bitnami/spark/job2_out/ ./out_data/job2/
docker cp spark-worker-1:/opt/bitnami/spark/output_join/ ./out_data/output_join/

docker cp spark-worker-2:/opt/bitnami/spark/job1_out/ ./out_data/job1/
docker cp spark-worker-2:/opt/bitnami/spark/job2_out/ ./out_data/job2/
docker cp spark-worker-2:/opt/bitnami/spark/output_join/ ./out_data/output_join/
sleep 999999