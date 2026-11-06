#!/usr/bin/env bash
set -euo pipefail
[[ "${HARBOR_ENTRY_INSTALLED_RELEASE:-}" == '1' ]]
: "${DEPLOY_HOST:?DEPLOY_HOST is required}"
: "${ENTRY_INSTALLED_IMAGE:?ENTRY_INSTALLED_IMAGE must be the approved installed business tag}"
: "${HARBOR_REGISTRY_HOST:?HARBOR_REGISTRY_HOST is required}"
: "${HARBOR_PROJECT:?HARBOR_PROJECT is required}"
: "${CI_PROJECT_NAME:?CI_PROJECT_NAME is required}"
[[ "$DEPLOY_HOST" =~ ^[a-zA-Z0-9][a-zA-Z0-9.-]*$ ]]
[[ "$ENTRY_INSTALLED_IMAGE" =~ ^[a-zA-Z0-9./:_-]+:[a-f0-9]{40}$ ]]
[[ "${ENTRY_INSTALLED_IMAGE%:*}" == "$HARBOR_REGISTRY_HOST/$HARBOR_PROJECT/$CI_PROJECT_NAME" ]]
# The fixed root endpoint independently requires installed mode, image proof,
# activation, source fencing, and entry control. Only this JSON line is sent.
printf '{"action":"deploy-entry-postgres","image":"%s"}\n' "$ENTRY_INSTALLED_IMAGE" |
  ssh -o BatchMode=yes -o ConnectTimeout=15 -o ServerAliveInterval=15 \
    -o StrictHostKeyChecking=yes -i ~/.ssh/harbor_deploy "harbor_deploy@$DEPLOY_HOST"
