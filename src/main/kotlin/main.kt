import com.azure.cosmos.*
import com.azure.cosmos.models.*
import kotlinx.coroutines.*
import java.util.*
import kotlin.random.Random
import kotlin.system.measureTimeMillis

// Settings for connecting to Cosmos DB via environment variables
val COSMOS_URL: String = System.getenv("COSMOS_URL") ?: throw IllegalArgumentException("COSMOS_URL environment variable not set")
val COSMOS_KEY: String = System.getenv("COSMOS_KEY") ?: throw IllegalArgumentException("COSMOS_KEY environment variable not set")
val DATABASE_NAME: String = System.getenv("DATABASE_NAME") ?: throw IllegalArgumentException("DATABASE_NAME environment variable not set")
val CONTAINER_NAME: String = System.getenv("CONTAINER_NAME") ?: throw IllegalArgumentException("CONTAINER_NAME environment variable not set")
val recordQuantity = System.getenv("RECORD_QUANTITY")?.toInt() ?: 1000
val batchSize = System.getenv("BATCH_SIZE")?.toInt() ?: 100

// Creating a client and connecting to the database and container
val cosmosClient = CosmosClientBuilder()
    .endpoint(COSMOS_URL)
    .key(COSMOS_KEY)
    .consistencyLevel(ConsistencyLevel.EVENTUAL)
    .buildClient()

val database = cosmosClient.getDatabase(DATABASE_NAME)
val container = database.getContainer(CONTAINER_NAME)

// Data model
data class AccountData(
    val id: String = UUID.randomUUID().toString(),
    val account: String = UUID.randomUUID().toString(),
    val balance: Double = Random.nextDouble(1000.0, 5000.0),
    val time: Long = System.currentTimeMillis()
)

// Function to generate a single record
fun generateRandomData(): AccountData {
    return AccountData()
}

// Batch insertion of data
suspend fun insertItemsBatch(batch: List<AccountData>): String {
    return try {
        val partitionKey = batch.first().account

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
    println("Preparing for container cleaning. It will take 1-2 minutes.")
    var deletedCount = 0
    try {
        val query = "SELECT * FROM c"
        val items = container.queryItems(query, CosmosQueryRequestOptions(), Map::class.java)
        items.forEach { item ->
            container.deleteItem(item["id"] as String, PartitionKey(item["account"] as String), CosmosItemRequestOptions())
            deletedCount++
        }
        println("Container cleared successfully. Deleted $deletedCount items.")
    } catch (e: Exception) {
        println("Failed to clear container due to error: ${e.message}")
    }
}

// Function to get item count in the container
suspend fun getItemCount(): Int {
    return try {
        val query = "SELECT VALUE COUNT(1) FROM c"
        val countResult = container.queryItems(query, CosmosQueryRequestOptions(), Int::class.java)
        val count = countResult.firstOrNull() ?: 0
        println("Total items in container at the moment: $count")
        count
    } catch (e: Exception) {
        println("Failed to get item count due to error: ${e.message}")
        0
    }
}

// Asynchronous writing and clearing
fun writeAndClearDataConcurrently(numRecords: Int, batchSize: Int) = runBlocking {
    val items = (0 until numRecords).map { generateRandomData() }

    clearContainer()
    var insertCount = 0

    val totalTimeMs = measureTimeMillis {
        coroutineScope {
            val batchJobs = items.chunked(batchSize).map { batch ->
                async(Dispatchers.IO) {
                    insertCount++
                    insertItemsBatch(batch)
                }
            }
            batchJobs.forEach { job -> println(job.await()) }
        }
    }

    println("Total insert batch operations: $insertCount")
    println("\nTotal time for inserting $numRecords records: $totalTimeMs ms")

    val itemsPerSecond = (numRecords / (totalTimeMs / 1000.0)).toInt()
    println("TPS: $itemsPerSecond")

    getItemCount()
}

// Main function
fun main() {
    writeAndClearDataConcurrently(recordQuantity, batchSize)
}
