# Cosmos DB Batch Insertion Project by Evgenii Bartenev

## Project Description

This project demonstrates how to perform batch insertion of data into an Azure Cosmos DB database using Kotlin and coroutines. The project includes functions for clearing the container and asynchronously inserting data while measuring execution time.

## Execution Steps

1. Connect to the Cosmos DB database using the provided environment variables.
2. Clear the container of existing data.
3. Batch insert data into the container.
4. Clear the container again after data insertion.

## Required Resources

- Azure Cosmos DB account
- Docker
- Internet connection to access Azure Cosmos DB

## Environment Variables

The following environment variables need to be set to run the project:

- `COSMOS_URL`: The URL to connect to your Cosmos DB account.
- `COSMOS_KEY`: The access key for your Cosmos DB account.
- `DATABASE_NAME`: The name of the database (default is "ToDoList").
- `CONTAINER_NAME`: The name of the container (default is "Items").

## Dependencies

The project uses the following dependencies:

- Kotlin: version 2.0.21
- Azure Cosmos SDK: version 4.64.0
- Kotlin Coroutines: version 1.7.3
- Logback Classic: version 1.2.11

## Installation and Running

1. Clone the repository:
    ```sh
    git clone <your_repository_url>
    cd <repository_name>
    ```

2. Build the Docker image:
    ```sh
    docker build -t cosmos-batch-insertion .
    ```

3. Run the container:
    ```sh
    docker run --rm -e COSMOS_URL=<your_cosmos_url> -e COSMOS_KEY=<your_cosmos_key> -e DATABASE_NAME=<database_name> -e CONTAINER_NAME=<container_name> cosmos-batch-insertion
    ```

## Usage Example

The project inserts 1000 records into the Cosmos DB container in batches of 100 records and measures the total execution time.

```kotlin
fun main() {
    writeAndClearDataConcurrently(1000, 100) // 1000 records, in batches of 100
}
