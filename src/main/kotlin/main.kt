import com.azure.cosmos.*
import com.azure.cosmos.models.*
import kotlinx.coroutines.*
import kotlin.system.measureTimeMillis

// Settings for connecting to Cosmos DB via environment variables
val COSMOS_URL: String = System.getenv("COSMOS_URL") ?: throw IllegalArgumentException("COSMOS_URL environment variable not set")
val COSMOS_KEY: String = System.getenv("COSMOS_KEY") ?: throw IllegalArgumentException("COSMOS_KEY environment variable not set")
val DATABASE_NAME: String = System.getenv("DATABASE_NAME") ?: "ToDoList"
val CONTAINER_NAME: String = System.getenv("CONTAINER_NAME") ?: "Items"

// Creating a client and connecting to the database and container
val cosmosClient = CosmosClientBuilder()
    .endpoint(COSMOS_URL)
    .key(COSMOS_KEY)
    .consistencyLevel(ConsistencyLevel.EVENTUAL)
    .buildClient()

val database = cosmosClient.getDatabase(DATABASE_NAME)
val container = database.getContainer(CONTAINER_NAME)

// Example data to be written
val dataTemplate = mapOf(
    "id" to "",
    "name" to "Example Item"
)

// Batch insertion of data
suspend fun insertItemsBatch(batch: List<Map<String, String>>): String {
    return try {
        val partitionKey = batch.first()["name"] ?: throw IllegalArgumentException("Partition key cannot be null")

        val transactionalBatch = CosmosBatch.createCosmosBatch(PartitionKey(partitionKey))
        batch.forEach { item ->
            transactionalBatch.createItemOperation(item)
        }

        val response = container.executeCosmosBatch(transactionalBatch)
        if (response.isSuccessStatusCode) {
            "Successfully inserted batch of size ${batch.size}"
        } else {
            "Failed to insert batch: ${response.statusCode} - ${response.errorMessage}"
        }
    } catch (e: Exception) {
        "Failed to insert batch due to error: ${e.message}"
    }
}

// Clearing the container
suspend fun clearContainer() {
    try {
        val query = "SELECT * FROM c"
        val items = container.queryItems(query, CosmosQueryRequestOptions(), Map::class.java)
        items.forEach { item ->
            container.deleteItem(item["id"] as String, PartitionKey(item["name"] as String), CosmosItemRequestOptions())
        }
        println("Container cleared successfully.")
    } catch (e: Exception) {
        println("Failed to clear container due to error: ${e.message}")
    }
}

// Asynchronous writing and clearing
fun writeAndClearDataConcurrently(numRecords: Int, batchSize: Int) = runBlocking {
    val items = (0 until numRecords).map { i ->
        dataTemplate.toMutableMap().apply { this["id"] = i.toString() }
    }

    clearContainer()

    val totalTimeMs = measureTimeMillis {
        coroutineScope {
            val batchJobs = items.chunked(batchSize).map { batch ->
                async(Dispatchers.IO) {
                    insertItemsBatch(batch)
                }
            }
            batchJobs.forEach { job -> println(job.await()) }
        }
    }

    println("\nTotal time for inserting $numRecords records: $totalTimeMs ms")

    clearContainer()
}

fun main() {
    writeAndClearDataConcurrently(1000, 100) // 1000 records, in batches of 100
}