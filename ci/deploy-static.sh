#!/usr/bin/env bash
set -euo pipefail
: "${DEPLOY_HOST:?DEPLOY_HOST is required}"
: "${CI_COMMIT_SHA:?CI_COMMIT_SHA is required}"
: "${STATIC_RELEASE_BUNDLE:?STATIC_RELEASE_BUNDLE is required}"
[[ "$CI_COMMIT_SHA" =~ ^[a-f0-9]{40}$ ]]
[[ -f "$STATIC_RELEASE_BUNDLE" ]]
# No commands, paths, OCI image, port or backend settings are accepted by the forced static action.
{
  printf '{"action":"deploy-static","revision":"%s"}\n' "$CI_COMMIT_SHA"
  cat "$STATIC_RELEASE_BUNDLE"
} | ssh -o BatchMode=yes -o ConnectTimeout=15 -o ServerAliveInterval=15 \
    -o StrictHostKeyChecking=yes -i ~/.ssh/harbor_deploy "harbor_deploy@$DEPLOY_HOST"
