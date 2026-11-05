ARG HARBOR_BUSINESS_IMAGE
FROM ${HARBOR_BUSINESS_IMAGE} AS business
FROM public.ecr.aws/docker/library/postgres:18.6-bookworm@sha256:5c4783a8cc3f0074912164f94dfa1be4e043438f0f707cf968d7af9a0d31a2c8
ARG HARBOR_BUSINESS_IMAGE
LABEL com.harbor.infrastructure="postgres18-report-v1" com.harbor.publisher-business-image="${HARBOR_BUSINESS_IMAGE}"
# This is infrastructure, never a replacement for the verified business image.
COPY --from=business /usr/local/bin/node /usr/local/bin/node
COPY --from=business /usr/bin/openssl /usr/bin/openssl
COPY --from=business /app /opt/harbor-storage-report
COPY ci/storage-recovery/postgres-init.sql /docker-entrypoint-initdb.d/10-harbor-private-cluster.sql
COPY ci/storage-recovery/archive-observe.mjs /opt/harbor-storage-report/ci/storage-recovery/archive-observe.mjs
COPY ci/storage-recovery/postgres-publisher.mjs /opt/harbor-storage-report/ci/storage-recovery/postgres-publisher.mjs
COPY ci/storage-recovery/postgres-entrypoint.sh /usr/local/bin/harbor-postgres-infra
COPY ci/storage-recovery/postgres-smoke-entrypoint.sh /usr/local/bin/harbor-postgres-infra-smoke
RUN mkdir -p /opt/product-development/harbor-local-gateway/postgres-infra/db && chmod 0755 /opt/product-development /opt/product-development/harbor-local-gateway /opt/product-development/harbor-local-gateway/postgres-infra && test "$(id -u postgres)" = 999 && node --version && /usr/bin/openssl version && chmod 0755 /usr/local/bin/harbor-postgres-infra /usr/local/bin/harbor-postgres-infra-smoke
ENTRYPOINT ["/usr/local/bin/harbor-postgres-infra"]
# Clear the official image's inherited ["postgres"] arguments: our entry is fixed.
CMD []
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 CMD pg_isready -h /var/run/postgresql -U postgres
