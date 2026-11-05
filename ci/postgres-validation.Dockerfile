ARG HARBOR_CANDIDATE_IMAGE
FROM ${HARBOR_CANDIDATE_IMAGE} AS candidate
FROM public.ecr.aws/docker/library/postgres:18.6-bookworm@sha256:5c4783a8cc3f0074912164f94dfa1be4e043438f0f707cf968d7af9a0d31a2c8
LABEL com.harbor.ci-validation-only="true"
RUN apt-get update && apt-get install -y --no-install-recommends libstdc++6 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=candidate /usr/local/bin/node /usr/local/bin/node
COPY --from=candidate /app /app
COPY ci/postgres-ci-entrypoint.sh /usr/local/bin/harbor-postgres-ci
RUN node --version && chmod 0755 /usr/local/bin/harbor-postgres-ci
ENTRYPOINT ["/usr/local/bin/harbor-postgres-ci"]
