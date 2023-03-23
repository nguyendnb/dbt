# Use the official Python image as a parent image
FROM python:3.9-slim-buster

# Set environment variables
ENV DBT_VERSION=1.0.0 \
    DBT_PROJECT_DIR=/dbt

# Clone the newest DBT repo
RUN git clone https://myrepo.com/git.git temp \
    mv temp/.git code/.git \
    rm -rf temp

# Copy requirements
COPY requirements.txt packages.yml dbt_project.yml ./

# Install DBT
RUN pip install -r requirements.txt

# Copy the DBT project into the container
COPY . $DBT_PROJECT_DIR

# Set the working directory to the DBT project directory
WORKDIR $DBT_PROJECT_DIR

# Set the entry point to the DBT executable
ENTRYPOINT ["dbt"]