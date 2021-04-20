#!/bin/bash

set -euo pipefail

docker run --rm --name graph-db --publish=7474:7474 --publish=7687:7687 -e 'NEO4J_AUTH=neo4j/secret' neo4j:4.0.4
