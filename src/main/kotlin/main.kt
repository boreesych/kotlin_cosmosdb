import com.azure.cosmos.*
import com.azure.cosmos.models.*
import kotlinx.coroutines.*
import kotlinx.coroutines.reactor.awaitSingle
import java.util.*
import kotlin.random.Random
import kotlin.system.measureTimeMillis

val COSMOS_URL = System.getenv("COSMOS_URL") ?: throw IllegalArgumentException("COSMOS_URL environment variable not set")
val COSMOS_KEY = System.getenv("COSMOS_KEY") ?: throw IllegalArgumentException("COSMOS_KEY environment variable not set")
val DATABASE_NAME = System.getenv("DATABASE_NAME") ?: throw IllegalArgumentException("DATABASE_NAME environment variable not set")
val CONTAINER_NAME = System.getenv("CONTAINER_NAME") ?: "demo"
val RECORD_QUANTITY = System.getenv("RECORD_QUANTITY")?.toInt() ?: 1000
val BATCH_SIZE = System.getenv("BATCH_SIZE")?.toInt() ?: 100
val RU_VALUE = System.getenv("RU_VALUE")?.toInt() ?: 10000

val cosmosClient = CosmosClientBuilder()
    .endpoint(COSMOS_URL)
    .key(COSMOS_KEY)
    .consistencyLevel(ConsistencyLevel.EVENTUAL)
    .buildAsyncClient()

val database = cosmosClient.getDatabase(DATABASE_NAME)

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

fun generateRandomData(): AccountData = AccountData()

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

suspend fun deleteContainer() {
    try {
        database.getContainer(CONTAINER_NAME).delete().awaitSingle()
        println("Container '$CONTAINER_NAME' deleted successfully.")
    } catch (e: Exception) {
        println("Failed to delete container: ${e.message}")
    }
}

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

suspend fun getItemCount(container: CosmosAsyncContainer): Int {
    return try {
        val query = "SELECT VALUE COUNT(1) FROM c"
        val queryOptions = CosmosQueryRequestOptions()

        val queryResponse = container.queryItems(query, queryOptions, Int::class.java)
            .byPage()
            .toIterable()
            .firstOrNull()

        val count = queryResponse?.results?.firstOrNull() ?: 0

        count
    } catch (e: Exception) {
        println("Failed to count items: ${e.message}")
        0
    }
}

fun writeData(container: CosmosAsyncContainer, numRecords: Int, batchSize: Int) = runBlocking {
    val items = (0 until numRecords).map { generateRandomData() }
    val tpsValues = mutableListOf<Int>()
    var insertedRecords = 0

    val totalTimeMs = measureTimeMillis {
        coroutineScope {
            items.chunked(batchSize).chunked(1000 / batchSize).forEachIndexed { chunkIndex, batchGroup ->
                val chunkTimeMs = measureTimeMillis {
                    val batchJobs = batchGroup.map { batch ->
                        async(Dispatchers.IO) {
                            insertItemsBatch(container, batch)
                        }
                    }
                    batchJobs.forEach { job -> println(job.await()) }
                }
                val chunkTps = (1000 / (chunkTimeMs / 1000.0)).toInt()
                tpsValues.add(chunkTps)
                println("TPS for batch ${chunkIndex + 1}: $chunkTps")
                insertedRecords += 1000
            }
        }
    }

    println("Total records inserted: $insertedRecords")
    println("Total time for inserting $numRecords records: $totalTimeMs ms")

    val totalTps = (numRecords / (totalTimeMs / 1000.0)).toInt()
    val minTps = tpsValues.minOrNull() ?: 0
    val maxTps = tpsValues.maxOrNull() ?: 0
    val avgTps = if (tpsValues.isNotEmpty()) tpsValues.sum() / tpsValues.size else 0

    println("Overall TPS: $totalTps")
    println("Min TPS: $minTps")
    println("Max TPS: $maxTps")
    println("Avg TPS: $avgTps")
}

fun main() = runBlocking {
    try {
        println("Creating container...")
        createContainerIfNotExists()

        val container = database.getContainer(CONTAINER_NAME)
        val initialItemCount = getItemCount(container)
        println("Total items in container before insertion: $initialItemCount")

        println("Starting data insertion...")
        writeData(container, 50000, 25)

        val itemCount = getItemCount(container)
        println("Total items in container after insertion: $itemCount")

        if (itemCount == initialItemCount + 50000) {
            println("Data insertion verified: $itemCount items present as expected.")
        } else {
            println("Discrepancy in data insertion: expected ${initialItemCount + 50000}, but found $itemCount.")
        }
    } catch (e: Exception) {
        println("An error occurred: ${e.message}")
    } finally {
        println("Cleaning up...")
        deleteContainer()
        cosmosClient.close()
    }
}
