#!/bin/sh
# 显式手动准备同项目依赖缓存；不构建候选、不改变镜像或部署门禁。
set +x
set -eu
umask 077
fail() { printf '%s\n' "$1" >&2; exit 1; }
number() { case "$1" in ''|0|*[!0-9]*) fail harbor_postgres_cache_context_invalid;; esac; }
source_failure_category() {
  # Only these fixed labels may leave the private error file. Never echo URLs,
  # registry responses, anonymous bearer tokens or credential-bearing stderr.
  if LC_ALL=C grep -Eiq 'toomanyrequests|too many requests|rate.?limit|(^|[^0-9])429([^0-9]|$)' "$1"; then category=rate_limit
  elif LC_ALL=C grep -Eiq 'no such host|temporary failure in name resolution|name or service not known|server misbehaving|dns|dial tcp: lookup' "$1"; then category=dns
  elif LC_ALL=C grep -Eiq 'timeout|timed out|context deadline exceeded|deadline exceeded' "$1"; then category=timeout
  elif LC_ALL=C grep -Eiq 'x509:|certificate|tls handshake|tls:|ssl handshake|wrong version number|unknown authority' "$1"; then category=tls
  else category=other
  fi
  printf 'harbor_postgres_cache_source_failure_category=%s\n' "$category"
}
test "$#" = 0 || fail harbor_postgres_cache_disabled
case "${HARBOR_CACHE_BASE_IMAGE:-postgres}" in
  postgres)
    image=postgres; tag=18.6-bookworm
    expected=sha256:5c4783a8cc3f0074912164f94dfa1be4e043438f0f707cf968d7af9a0d31a2c8
    selector=${HARBOR_POSTGRES_CACHE_SOURCE:-dockerhub}
    test "${HARBOR_CACHE_POSTGRES_BASE:-}" = 1 || fail harbor_postgres_cache_disabled;;
  node)
    image=node; tag=24.21.0-bookworm-slim
    expected=sha256:d6aa754f16b3197301076f047b5def2f02ea1dbbc2ca920407d46d7ec7f87b20
    selector=${HARBOR_NODE_CACHE_SOURCE:-dockerhub}
    test "${HARBOR_CACHE_NODE_BASE:-}" = 1 || fail harbor_postgres_cache_disabled;;
  *) fail harbor_postgres_cache_image_invalid;;
esac
test "${HARBOR_PROJECT:-harbor-local-gateway}" = harbor-local-gateway || fail harbor_postgres_cache_project_invalid
number "${CI_PROJECT_ID:-}"; number "${CI_PIPELINE_ID:-}"; number "${CI_JOB_ID:-}"
case "${CI_COMMIT_SHA:-}" in ''|*[!a-f0-9]*) fail harbor_postgres_cache_context_invalid;; esac
test "${#CI_COMMIT_SHA}" = 40 || fail harbor_postgres_cache_context_invalid
case "${HARBOR_REGISTRY_HOST:-}" in ''|-*|*[!a-z0-9.:-]*) fail harbor_postgres_cache_registry_invalid;; esac
test -n "${GATEWAY_REGISTRY_USERNAME:-}" && test -n "${GATEWAY_REGISTRY_PASSWORD:-}" || fail harbor_postgres_cache_credentials_required

case "$selector" in
  dockerhub) source="docker://docker.io/library/$image@$expected";;
  ecr) source="docker://public.ecr.aws/docker/library/$image@$expected";;
  *) fail harbor_postgres_cache_source_invalid;;
esac
destination="docker://$HARBOR_REGISTRY_HOST/harbor-local-gateway/docker/library/$image:$tag-sha256-${expected#sha256:}"
root=.ci-$image-base-cache
test ! -L "$root" || fail harbor_postgres_cache_evidence_invalid
mkdir -p "$root"
evidence="$root/$CI_PIPELINE_ID-$CI_JOB_ID.json"
test ! -e "$evidence" && test ! -L "$evidence" || fail harbor_postgres_cache_evidence_exists
work=$(mktemp -d "${TMPDIR:-/tmp}/harbor-postgres-base-cache.XXXXXX")
trap 'rm -rf -- "$work"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if ! skopeo inspect --no-creds --tls-verify=true --raw "$source" > "$work/source.json" 2> "$work/error"; then source_failure_category "$work/error" >&2; fail harbor_postgres_cache_source_inspect_failed; fi
actual="sha256:$(sha256sum "$work/source.json" | cut -d ' ' -f 1)"
test "$actual" = "$expected" || fail harbor_postgres_cache_source_digest_mismatch
if ! printf '%s' "$GATEWAY_REGISTRY_PASSWORD" | skopeo login --authfile "$work/auth.json" --tls-verify=false --username "$GATEWAY_REGISTRY_USERNAME" --password-stdin "$HARBOR_REGISTRY_HOST" > "$work/login" 2> "$work/error"; then fail harbor_postgres_cache_login_failed; fi
if ! skopeo copy --quiet --all --preserve-digests --retry-times 0 --src-no-creds --src-tls-verify=true --dest-authfile "$work/auth.json" --dest-tls-verify=false "$source" "$destination" > "$work/copy" 2> "$work/error"; then fail harbor_postgres_cache_copy_failed; fi
if ! skopeo inspect --authfile "$work/auth.json" --tls-verify=false --raw "$destination" > "$work/destination.json" 2> "$work/error"; then fail harbor_postgres_cache_destination_inspect_failed; fi
actual="sha256:$(sha256sum "$work/destination.json" | cut -d ' ' -f 1)"
test "$actual" = "$expected" && cmp -s "$work/source.json" "$work/destination.json" || fail harbor_postgres_cache_destination_digest_mismatch
printf '{"version":1,"project":"%s","pipeline":"%s","job":"%s","commit":"%s","source":"%s","destination":"%s","indexDigest":"%s","allPlatforms":true,"preserveDigests":true}\n' "$CI_PROJECT_ID" "$CI_PIPELINE_ID" "$CI_JOB_ID" "$CI_COMMIT_SHA" "$source" "$destination" "$expected" > "$work/evidence.json"
mv "$work/evidence.json" "$evidence"
printf 'harbor_%s_cache_verified\n' "$image"
