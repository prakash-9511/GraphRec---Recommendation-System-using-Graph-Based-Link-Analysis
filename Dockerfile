FROM apache/spark:3.4.1

USER root

# Install Python dependencies
RUN apt-get update && apt-get install -y python3-pip
RUN pip3 install flask pandas numpy pyspark

# Set working directory
WORKDIR /app

# Copy project files
COPY . /app

# Fix permissions
RUN chmod +x /app/entrypoint.sh

# Environment variables (IMPORTANT FIX)
ENV HADOOP_USER_NAME=root
ENV PYSPARK_SUBMIT_ARGS="--conf spark.driver.extraJavaOptions=--add-opens=java.base/javax.security.auth=ALL-UNNAMED pyspark-shell"

# Start app
CMD ["/app/entrypoint.sh"]