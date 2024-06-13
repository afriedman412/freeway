#!/bin/bash

while IFS= read -r line || [[ -n "$line" ]]; do
    key=$(echo "$line" | cut -d '=' -f1)
    value=$(echo "$line" | cut -d '=' -f2-)

    # Trim leading/trailing whitespace (optional)
    value=$(echo "$value" | xargs)

    # Pass each variable as a build argument
    docker_build_args+=" --build-arg $key=\"$value\""
done < .env

# Build Docker image with all build arguments
docker build $docker_build_args . -t freeway
