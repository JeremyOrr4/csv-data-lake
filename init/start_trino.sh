# Start Trino

cd trino

helm repo add trino https://trinodb.github.io/charts
helm upgrade example-trino-cluster trino/trino -f values.yaml

cd ..