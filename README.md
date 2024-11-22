# Cosmos DB Batch Insertion by Evgenii Bartenev

## Project Description

This project demonstrates how to perform batch insertion of data into an Azure Cosmos DB database using Kotlin and coroutines. It includes functionalities for clearing the container, inserting data asynchronously in batches, and measuring performance through execution time and throughput. Additionally, it introduces a structured data model `AccountData` for insertion, ensuring efficient and realistic data operations.

## Execution Steps

1. Connect to the Cosmos DB database using environment variables.
2. Clear the container to remove existing data.
3. Batch insert data into the container using the `AccountData` model.
4. Retrieve the total item count in the container.

## Data Model

The data being inserted is based on the `AccountData` model. This model includes the following fields:

- **id**: Unique identifier for the record (UUID).
- **account**: Partition key used for batch operations.
- **balance**: A randomly generated balance between 1000.0 and 5000.0.
- **description**: A description for the document.
- **time**: Current system timestamp.
- **timec**: Timestamp for record creation.
- **pid**: A unique identifier for the process.
- **randomValue**: A randomly generated integer between -10,000 and 10,001.

## Required Resources

- Azure Cosmos DB account - [Manual Setup](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/quickstart-portal), [Script Setup])()
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Internet connection to access Azure Cosmos DB
- [Java Platform, Standard Edition 22](https://jdk.java.net/java-se-ri/22) - [Windows Download](https://download.java.net/openjdk/jdk22/ri/openjdk-22+36_windows-x64_bin.zip)

## Environment Variables

The following environment variables need to be set to run the project:

- `COSMOS_URL`: The URL for connecting to your Cosmos DB account.
- `COSMOS_KEY`: The access key for your Cosmos DB account.
- `DATABASE_NAME`: The name of the database.
- `CONTAINER_NAME`: The name of the container.
- `RECORD_QUANTITY`: Total number of records to insert (default is 1000).
- `BATCH_SIZE`: Number of records per batch (default is 100, max is 100).

## Dependencies

This project uses the following dependencies:

- **Kotlin**: version 2.0.21
- **Azure Cosmos SDK**: version 4.64.0
- **Kotlin Coroutines**: version 1.7.3
- **Logback Classic**: version 1.2.11

## Building and Run Locally with Gradle

Clone the repository:

```sh
git clone <your_repository_url>
cd <repository_name>
```

Build the project using gradlew:

```sh
gradlew build
```  

Run the project with the required environment variables:

```sh
COSMOS_URL=<your_cosmos_url> \
COSMOS_KEY=<your_cosmos_key> \
DATABASE_NAME=<database_name> \
CONTAINER_NAME=<container_name> \
RECORD_QUANTITY=1000 \
BATCH_SIZE=100 \
gradlew run
```  

## Running with Docker After Building

1. Build the Docker image:
    ```sh
    docker build -t cosmos-batch-insertion .
    ```

2. Run the container with the required environment variables:
    ```sh
    docker run --rm \
        -e COSMOS_URL=<your_cosmos_url> \
        -e COSMOS_KEY=<your_cosmos_key> \
        -e DATABASE_NAME=<database_name> \
        -e CONTAINER_NAME=<container_name> \
        -e RECORD_QUANTITY=1000 \
        -e BATCH_SIZE=100 \
        cosmos-batch-insertion
    ```

## Usage Example

The default execution inserts 1000 records into the Cosmos DB container in batches of 100 records and measures the total execution time, reporting throughput (TPS).

### Sample Output

- **Total Insert Batch Operations:** Number of processed batches.
- **Total Time:** Execution time in milliseconds.
- **TPS:** Throughput in terms of transactions per second.
- **Container Item Count:** Total items in the container after insertion.

### Example Code Execution

```kotlin
fun main() {
    writeData(1000, 100) // Inserts 1000 records in batches of 100
}
```

### Notes

Ensure that the partition key (account) is consistent within each batch for successful insertion.
