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
    val account: String = "9ac25829-0152-426b-91ef-492d799bece9",
    val balance: Double = Random.nextDouble(1000.0, 5000.0),
    val description: String = "This is a description of the document",
    val time: Long = System.currentTimeMillis(),
    val timec: Long = System.currentTimeMillis(),
    val pid: String = UUID.randomUUID().toString(),
    val randomValue: Int = Random.nextInt(-10000, 10001),
)

// Function to generate a single record
fun generateRandomData(): AccountData {
    return AccountData()
}

// Batch insertion of data
suspend fun insertItemsBatch(batch: List<AccountData>): Boolean {
    return try {
        val partitionKey = batch.first().account

        val transactionalBatch = CosmosBatch.createCosmosBatch(PartitionKey(partitionKey))
        batch.forEach { item ->
            transactionalBatch.createItemOperation(item)
        }

        val response = container.executeCosmosBatch(transactionalBatch)
        response.isSuccessStatusCode
    } catch (e: Exception) {
        false
    }
}

// Clearing the container
suspend fun clearContainer() {
    println("Preparing for container cleaning. It will take 1-2 minutes.")
    try {
        val query = "SELECT * FROM c"
        val items = container.queryItems(query, CosmosQueryRequestOptions(), Map::class.java)
        items.forEach { item ->
            container.deleteItem(item["id"] as String, PartitionKey(item["account"] as String), CosmosItemRequestOptions())
        }
        println("Container cleared successfully.")
    } catch (e: Exception) {
        println("Failed to clear container due to error: ${e.message}")
    }
}

// Main function for continuous insertion and TPS calculation
fun calculateTPS(batchSize: Int) = runBlocking {
    // clearContainer()

    var totalCount = 0
    val startTime = System.currentTimeMillis()
    val duration = 60 * 1000 // 1 minute in milliseconds
    val semaphore = Semaphore(10)

    println("Starting continuous data insertion for 1 minute...")
    // val deferredResults = mutableListOf<Deferred<Boolean>>()
    val deferredResults = mutableListOf<Deferred<Int>>()

    // Continuous insertion for 1 minute
    while (System.currentTimeMillis() - startTime < duration) {
        val batch = (0 until batchSize).map { generateRandomData() }
        deferredResults.add(async(Dispatchers.IO) {
            if (insertItemsBatch(batch)) {
                batch.size
            } else {
                0
            }
        })
    }

    // Wait for all asynchronous operations to complete
    totalCount = deferredResults.awaitAll().sum()

    val elapsedTime = System.currentTimeMillis() - startTime
    val tps = totalCount / (elapsedTime / 1000.0)
    println("Insertion completed.")
    println("Total records inserted: $totalCount")
    println("Elapsed time: $elapsedTime ms")
    println("TPS: $tps")
}

// Main function
fun main() {
    calculateTPS(batchSize)
}
