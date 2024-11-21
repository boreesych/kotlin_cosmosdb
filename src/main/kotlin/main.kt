import com.azure.cosmos.*
import com.azure.cosmos.models.*
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
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
    val account: String = UUID.randomUUID().toString(),
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

// Main function for writing 100,000 records
fun writeRecords(totalRecords: Int, batchSize: Int) = runBlocking {
    val semaphore = Semaphore(100) // Limit to 100 concurrent tasks
    val deferredResults = mutableListOf<Deferred<Int>>()

    val startTime = System.currentTimeMillis()

    // Generate and insert 100,000 records in parallel batches
    for (i in 0 until totalRecords step batchSize) {
        val batch = (0 until batchSize).map { generateRandomData() }
        semaphore.withPermit {
            deferredResults.add(async(Dispatchers.IO) {
                if (insertItemsBatch(batch)) batch.size else 0
            })
        }
    }

    // Wait for all asynchronous operations to complete
    val totalInserted = deferredResults.awaitAll().sum()

    val elapsedTime = System.currentTimeMillis() - startTime
    println("Insertion completed.")
    println("Total records inserted: $totalInserted")
    println("Elapsed time: $elapsedTime ms")
    println("TPS: ${totalInserted / (elapsedTime / 1000.0)}")
}

// Main function
fun main() {
    writeRecords(totalRecords = 100_000, batchSize = batchSize)
}
