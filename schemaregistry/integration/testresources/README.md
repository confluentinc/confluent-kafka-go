# Schema Registry integration test resources

`docker-compose.yaml` starts a KRaft Kafka broker and a Schema Registry:

| Endpoint         | Service                  |
| ---------------- | ------------------------ |
| `localhost:9092` | Kafka, PLAINTEXT         |
| `localhost:8081` | Schema Registry, no auth |

The tests can bring it up and tear it down themselves:

```bash
(cd schemaregistry/integration && go test -v -timeout 1800s -run ^TestIntegration$ -docker.needed=true)
```

Or reuse a stack you started, which is much faster while iterating:

```bash
(cd schemaregistry/integration/testresources && docker compose up -d)
(cd schemaregistry/integration && go test -v -timeout 1800s -run ^TestIntegration$ -docker.exists=true)
```

These host ports are the same ones `kafka/integration/testresources` binds, so
only one of the two stacks can run at a time.
