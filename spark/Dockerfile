# ============================================================================
# Custom Spark image with OCR + medical-data Python deps preinstalled.
# Base: bitnamilegacy/spark:3.5.6 (multi-arch, includes arm64 for Apple Silicon)
# Adds:  tesseract-ocr, poppler-utils, libreoffice (for .xlsx fallback),
#        boto3, PyPDF2, pdf2image, pytesseract, openpyxl, hl7, fhir.resources
# ============================================================================
FROM bitnamilegacy/spark:3.5.6

USER root

# System packages required for OCR (tesseract) and PDF rendering (poppler)
RUN apt-get update && apt-get install -y --no-install-recommends \
        tesseract-ocr \
        tesseract-ocr-eng \
        poppler-utils \
        ghostscript \
        libxml2-dev \
        libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

# Python deps for ETL jobs (OCR, multi-format ingestion, AWS, healthcare)
RUN pip install --no-cache-dir \
        boto3==1.34.0 \
        PyPDF2==3.0.1 \
        pdf2image==1.17.0 \
        pytesseract==0.3.10 \
        openpyxl==3.1.2 \
        pandas==2.2.2 \
        hl7==0.4.5 \
        lxml==5.2.2

# Preload S3A + Postgres JARs into Spark's jars directory so jobs don't have to
# download them at runtime via --packages (slow on first run).
RUN curl -sL -o /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && curl -sL -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
 && curl -sL -o /opt/bitnami/spark/jars/postgresql-42.7.3.jar \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

# /etc/passwd entry for UID 1001 — Hadoop's UnixLoginModule needs it; without
# an entry, user.home expands to "?" and Ivy/HDFS calls fail.
RUN echo 'spark:x:1001:0:spark:/tmp:/bin/sh' >> /etc/passwd

USER 1001
