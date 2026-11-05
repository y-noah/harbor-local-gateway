#!/bin/sh
# 只消费当前流水线受控构建的摘要，绝不回退到可覆盖标签。
set -eu
fail() { printf '%s\n' 'harbor_image_digest_invalid' >&2; exit 1; }
number() { case "$1" in ''|0|*[!0-9]*) fail;; esac; }
word() { case "$1" in ''|*[!a-z0-9._-]*) fail;; esac; }
digest() { case "$1" in sha256:*) value=${1#sha256:};; *) fail;; esac; test "${#value}" = 64 || fail; case "$value" in *[!a-f0-9]*) fail;; esac; }
number "$CI_PROJECT_ID"; number "$CI_PIPELINE_ID"; number "$CI_JOB_ID"
case "$CI_COMMIT_SHA" in *[!a-f0-9]*|'') fail;; esac
test "${#CI_COMMIT_SHA}" = 40 || fail
word "$HARBOR_PROJECT"; word "$CI_PROJECT_NAME"
case "$HARBOR_REGISTRY_HOST" in ''|*[!a-z0-9.:-]*) fail;; esac
repository="$HARBOR_REGISTRY_HOST/$HARBOR_PROJECT/$CI_PROJECT_NAME"
root=".ci-image-digests/$CI_PIPELINE_ID"
role=${1-}; parent=none
case "$role" in
 gateway) dockerfile=Dockerfile; argument="HARBOR_IMAGE_REVISION=$CI_COMMIT_SHA"; destination=$repository;;
 postgres-validation|postgres-infrastructure)
  number "$HARBOR_GATEWAY_PRODUCER_JOB_ID"
  input="$root/gateway-$HARBOR_GATEWAY_PRODUCER_JOB_ID.manifest"
  test -f "$input" && test ! -L "$input" || fail
  # Require one exact, canonical manifest; never execute artifact text as shell.
  test "$(wc -l < "$input" | tr -d ' ')" = 9 || fail
  expected="schema=1
project=$CI_PROJECT_ID
pipeline=$CI_PIPELINE_ID
commit=$CI_COMMIT_SHA
role=gateway
repository=$repository
producer=$HARBOR_GATEWAY_PRODUCER_JOB_ID"
  test "$(head -n 7 "$input")" = "$expected" || fail
  parent=$(sed -n '8s/^digest=//p' "$input"); digest "$parent"
  test "$(tail -n 1 "$input")" = parent=none || fail
  case "$role" in
   postgres-validation) dockerfile=ci/postgres-validation.Dockerfile; argument="HARBOR_CANDIDATE_IMAGE=$repository@$parent"; destination="$repository-pg-validation";;
   postgres-infrastructure) dockerfile=ci/storage-recovery/postgres-runtime.Dockerfile; argument="HARBOR_BUSINESS_IMAGE=$repository@$parent"; destination="$repository-pg-infra";;
  esac;;
 *) fail;;
esac
mkdir -p "$root"
raw="$root/$role-$CI_JOB_ID.raw"
manifest="$root/$role-$CI_JOB_ID.manifest"
test ! -e "$raw" && test ! -e "$manifest" || fail
set -- --build-arg "$argument"
if test "$role" = gateway; then set -- "$@" --build-arg "HARBOR_BUILD_DEBIAN_MIRROR_ORIGIN=${HARBOR_TEST_DEBIAN_MIRROR_ORIGIN-}"; fi
GODEBUG="${GODEBUG:+$GODEBUG,}http2client=0" /kaniko/executor --context "$CI_PROJECT_DIR" --dockerfile "$CI_PROJECT_DIR/$dockerfile" "$@" --destination "$destination:$CI_COMMIT_SHA" --digest-file "$CI_PROJECT_DIR/$raw" --insecure --insecure-pull --skip-tls-verify
result=$(cat "$raw"); digest "$result"
printf 'schema=1\nproject=%s\npipeline=%s\ncommit=%s\nrole=%s\nrepository=%s\nproducer=%s\ndigest=%s\nparent=%s\n' "$CI_PROJECT_ID" "$CI_PIPELINE_ID" "$CI_COMMIT_SHA" "$role" "$destination" "$CI_JOB_ID" "$result" "$parent" > "$manifest"
case "$role" in gateway) prefix=GATEWAY;; postgres-validation) prefix=POSTGRES_VALIDATION;; postgres-infrastructure) prefix=POSTGRES_INFRASTRUCTURE;; esac
printf 'HARBOR_%s_PRODUCER_JOB_ID=%s\n' "$prefix" "$CI_JOB_ID" > "$root/$role.env"
rm "$raw"
