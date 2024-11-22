import com.azure.cosmos.*
import com.azure.cosmos.models.*
import kotlinx.coroutines.*
import kotlinx.coroutines.reactor.awaitSingle
import java.util.*
import kotlin.random.Random
import kotlin.system.measureTimeMillis

// Settings for connecting to Cosmos DB via environment variables
val COSMOS_URL: String = System.getenv("COSMOS_URL") ?: throw IllegalArgumentException("COSMOS_URL environment variable not set")
val COSMOS_KEY: String = System.getenv("COSMOS_KEY") ?: throw IllegalArgumentException("COSMOS_KEY environment variable not set")
val DATABASE_NAME: String = System.getenv("DATABASE_NAME") ?: throw IllegalArgumentException("DATABASE_NAME environment variable not set")
val CONTAINER_NAME: String = System.getenv("CONTAINER_NAME") ?: "demo"
val RU_VALUE = 10000
val RECORD_QUANTITY = 5000
val BATCH_SIZE = 100

// Создание клиента Cosmos DB
val cosmosClient = CosmosClientBuilder()
    .endpoint(COSMOS_URL)
    .key(COSMOS_KEY)
    .consistencyLevel(ConsistencyLevel.EVENTUAL)
    .buildAsyncClient()

val database = cosmosClient.getDatabase(DATABASE_NAME)

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

// Function to create a container if it doesn't exist
suspend fun createContainerIfNotExists() {
    try {
        val containerProperties = CosmosContainerProperties(CONTAINER_NAME, "/account")
        val throughputProperties = ThroughputProperties.createManualThroughput(RU_VALUE)
        database.createContainerIfNotExists(containerProperties, throughputProperties).awaitSingle()
        println("Container '$CONTAINER_NAME' created or already exists.")
    } catch (e: Exception) {
        println("Failed to create container: ${e.message}")
    }
}

// Function to delete the container
suspend fun deleteContainer() {
    try {
        database.getContainer(CONTAINER_NAME).delete().awaitSingle()
        println("Container '$CONTAINER_NAME' deleted successfully.")
    } catch (e: Exception) {
        println("Failed to delete container: ${e.message}")
    }
}

// Function to insert items in batches
suspend fun insertItemsBatch(container: CosmosAsyncContainer, batch: List<AccountData>): String {
    return try {
        val partitionKey = batch.first().account
        val transactionalBatch = CosmosBatch.createCosmosBatch(PartitionKey(partitionKey))
        batch.forEach { item ->
            transactionalBatch.createItemOperation(item)
        }

        val response = container.executeCosmosBatch(transactionalBatch).awaitSingle()
        if (response.isSuccessStatusCode) {
            "Successfully inserted batch of size ${batch.size}"
        } else {
            "Failed to insert batch: ${response.statusCode} - ${response.errorMessage}"
        }
    } catch (e: Exception) {
        "Failed to insert batch due to error: ${e.message}"
    }
}

// Function to write data in batches
fun writeData(container: CosmosAsyncContainer, numRecords: Int, batchSize: Int) = runBlocking {
    val items = (0 until numRecords).map { generateRandomData() }

    var insertCount = 0

    val totalTimeMs = measureTimeMillis {
        coroutineScope {
            val batchJobs = items.chunked(batchSize).map { batch ->
                async(Dispatchers.IO) {
                    insertCount++
                    insertItemsBatch(container, batch)
                }
            }
            batchJobs.forEach { job -> println(job.await()) }
        }
    }

    println("Total insert batch operations: $insertCount")
    println("\nTotal time for inserting $numRecords records: $totalTimeMs ms")

    val itemsPerSecond = (numRecords / (totalTimeMs / 1000.0)).toInt()
    println("TPS: $itemsPerSecond")
}

// Function to count items in the container
suspend fun getItemCount(container: CosmosAsyncContainer): Int {
    return try {
        val query = "SELECT VALUE COUNT(1) FROM c"
        val queryOptions = CosmosQueryRequestOptions()

        val count = container.queryItems(query, queryOptions, Int::class.java)
            .byPage()
            .awaitSingle()
            .results
            .firstOrNull() ?: 0

        println("Total items in container: $count")
        count
    } catch (e: Exception) {
        println("Failed to count items: ${e.message}")
        0
    }
}

// Main function
fun main() = runBlocking {
    try {
        println("Creating container...")
        createContainerIfNotExists()

        val container = database.getContainer(CONTAINER_NAME)

        println("Starting data insertion...")
        writeData(container, RECORD_QUANTITY, BATCH_SIZE)

        println("Counting items in container...")
        val itemCount = getItemCount(container)
        println("Total items in container after insertion: $itemCount")
    } catch (e: Exception) {
        println("An error occurred: ${e.message}")
    } finally {
        println("Cleaning up...")
        deleteContainer()
        cosmosClient.close()
    }
}

// e: file:///C:/Dev/New%20folder/src/main/kotlin/main.kt:119:14 Cannot infer type for this parameter. Please specify it explicitly.
// e: file:///C:/Dev/New%20folder/src/main/kotlin/main.kt:119:14 Unresolved reference. None of the following candidates is applicable because of a receiver type mismatch:
// suspend fun <T> Mono<T>.awaitSingle(): T
// e: file:///C:/Dev/New%20folder/src/main/kotlin/main.kt:120:14 Unresolved reference 'results'.