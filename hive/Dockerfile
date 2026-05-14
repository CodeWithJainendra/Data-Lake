# ============================================================================
# Custom Hive Metastore image with required JARs pre-baked.
# Base: apache/hive:4.0.0 (multi-arch: arm64 + amd64)
# Adds: postgres JDBC + hadoop-aws + AWS SDK bundle for s3a:// support
#
# Without these JARs, the metastore fails when:
#   • Connecting to the Postgres backend (no JDBC driver)
#   • Creating tables with LOCATION 's3://...' (no S3A filesystem)
# ============================================================================
FROM apache/hive:4.0.0

USER root

# Pre-bake JARs into Hive's classpath so no runtime download is needed.
# Source: Maven Central. Versions match what Spark uses (compatibility).
RUN curl -sL -o /opt/hive/lib/postgresql-42.7.3.jar \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
 && curl -sL -o /opt/hive/lib/hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && curl -sL -o /opt/hive/lib/aws-java-sdk-bundle-1.12.262.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

USER hive
