
cd minio

kubectl apply -f init_minio.yaml
kubectl apply -f service_minio.yaml

cd ..