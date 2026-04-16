# Stage 1: lấy Spark
FROM thailinh2003/spark:3.0.0 AS spark

FROM openeuler/livy:0.8.0-oe2403sp1 AS livy

# Stage 2: Livy minimal
FROM eclipse-temurin:11-jre-jammy

ENV SPARK_HOME=/opt/spark
ENV LIVY_HOME=/opt/livy
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY --from=livy /opt/livy /opt/livy

RUN mkdir -p /opt/livy/logs && \
    chmod -R 777 /opt/livy

# Copy Spark từ stage 1
COPY --from=spark /opt/spark /opt/spark

ENV PATH="${SPARK_HOME}/bin:${LIVY_HOME}/bin:${PATH}"

WORKDIR ${LIVY_HOME}

EXPOSE 8998

CMD ["bin/livy-server"]