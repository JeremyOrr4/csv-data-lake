
cd csv-data-lake

kubectl delete jobs -l job-name=csv-data-lake --ignore-not-found

docker build -f Dockerfile -t csv-data-lake .

kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/csv-data-lake.yaml

kubectl logs -l app=csv-data-lake -f

cd ..
