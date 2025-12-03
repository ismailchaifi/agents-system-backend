# PowerShell script to create Kafka topics `prediction_requests` and `prediction_responses`
# Usage: run this after `docker-compose up -d`.

param()

Write-Host "Waiting 8 seconds for Kafka to start..."
Start-Sleep -Seconds 8

# Try to create topics by running kafka-topics.sh inside a temporary container connected to the compose network
$cmd = @"
# create prediction_requests (ignore errors if already exists)
/opt/confluent/bin/kafka-topics --bootstrap-server kafka:9092 --create --topic prediction_requests --partitions 1 --replication-factor 1 || true
# create prediction_responses
/opt/confluent/bin/kafka-topics --bootstrap-server kafka:9092 --create --topic prediction_responses --partitions 1 --replication-factor 1 || true
"@

# Execute using docker-compose run --rm topic-init sh -c "..."
$composeCmd = "docker-compose run --rm topic-init /bin/sh -c `"$cmd`""

Write-Host "Running: $composeCmd"
Invoke-Expression $composeCmd

Write-Host "Topic creation attempted. If there were no errors, topics should exist."
Write-Host "You can inspect topics with: docker-compose run --rm topic-init /bin/sh -c '/opt/confluent/bin/kafka-topics --bootstrap-server kafka:9092 --list'"
