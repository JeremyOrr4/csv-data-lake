
cd csv-data-lake
docker build -f Dockerfile -t csv-data-lake .
cd ..

kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/csv-data-lake.yaml