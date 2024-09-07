FROM python:3.9-slim

# Install Spark and necessary dependencies
RUN pip install pyspark findspark

# Create a directory for the Spark example
WORKDIR /app

# Copy the Spark example file
COPY spark_example.py /app/

# Expose the port for Spark UI
EXPOSE 4040

# Run the Spark example when the container starts
CMD ["python", "-m", "findspark", "spark_example.py"]
FROM python:3.9-slim

# Install Spark and necessary dependencies
RUN pip install pyspark findspark

# Install Oracle JDBC driver
RUN curl -O https://download.oracle.com/otn_utilities_drivers/11204/ojdbc8.jar

# Create a directory for the Oracle example
WORKDIR /app

# Copy the Oracle example file
COPY oracle_example.py /app/

# Expose the port for Spark UI
EXPOSE 4040

# Run the Oracle example when the container starts
CMD ["python", "-m", "findspark", "oracle_example.py"]
