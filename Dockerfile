FROM python:3.9-slim

# Install Spark and necessary dependencies
RUN pip install pyspark

# Create a directory for the Spark example
WORKDIR /app

# Copy the Spark example file
COPY spark_example.py /app/

# Expose the port for Spark UI
EXPOSE 4040

# Run the Spark example when the container starts
CMD ["python", "spark_example.py"]
