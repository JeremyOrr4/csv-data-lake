# CSV Data Lake

A simple data lake setup using MinIO, PostgreSQL, and Trino, orchestrated with Kubernetes and Podman.

## Getting Started

### Prerequisites

- [Podman](https://podman.io/)
- [Docker](https://docker.com)
- [Kubernetes](https://kubernetes.io/)
- [Helm](https://helm.sh/)

### Starting Services

Run each script to start the required services:

```sh
./start_minio.sh
./start_postgres.sh
./start_trino.sh