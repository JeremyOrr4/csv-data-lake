# Start Postgres

kubectl apply -f postgres/postgres-pv.yaml
kubectl apply -f postgres/postgres-deployment.yaml
kubectl apply -f postgres/postgres-service.yaml
