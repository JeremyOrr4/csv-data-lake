cd init

./start_minio.sh
./start_postgres.sh
./start_trino.sh

kubectl port-forward svc/minio 9000:9000 9001:9001

cd ..