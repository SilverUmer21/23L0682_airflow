FROM astrocrpublic.azurecr.io/runtime:3.3-12

# Meta details on the image (assignment: Docker networking and persistent storage)
LABEL com.example.project="23L0682_airflow" \
      com.example.network="Joins the Docker network 'airflow'; reaches MongoDB at host 'mongo' port 27017 (see docker-compose.override.yml)" \
      com.example.storage="Holds no data itself; MongoDB data is kept in the named volume 'mongo_data' (/data/db)"
