SCHEMA_REGISTRY_HOST="localhost"
SCHEMA_REGISTRY_PORT="8081"
TOPIC_NAME="transactions"

curl -d "@transactions.avsc"  -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
"http://${SCHEMA_REGISTRY_HOST}:${SCHEMA_REGISTRY_PORT}/subjects/${TOPIC_NAME}-value/versions"
