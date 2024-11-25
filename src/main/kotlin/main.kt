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
    } catch (e: Exception) {
        println("Failed to create container: ${e.message}")
    }
}

suspend fun deleteContainer() {
    try {
        database.getContainer(CONTAINER_NAME).delete().awaitSingle()
    } catch (e: Exception) {
        println("Failed to delete container: ${e.message}")
    }
}

suspend fun insertItemsBatch(container: CosmosAsyncContainer, batch: List<AccountData>) {
    try {
        val partitionKey = batch.first().account
        val transactionalBatch = CosmosBatch.createCosmosBatch(PartitionKey(partitionKey))
        batch.forEach { item ->
            transactionalBatch.createItemOperation(item)
        }
        container.executeCosmosBatch(transactionalBatch).awaitSingle()
    } catch (e: Exception) {
        throw RuntimeException("Failed to insert batch due to error: ${e.message}")
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

        queryResponse?.results?.firstOrNull() ?: 0
    } catch (e: Exception) {
        println("Failed to count items: ${e.message}")
        0
    }
}

fun writeData(container: CosmosAsyncContainer, totalRecords: Int, batchSize: Int) = runBlocking {
    val tpsValues = mutableListOf<Int>()
    var insertedRecords = 0

    val totalTimeMs = measureTimeMillis {
        coroutineScope {
            var remainingRecords = totalRecords
            while (remainingRecords > 0) {
                val currentLargeBatchSize = minOf(1000, remainingRecords)
                val largeBatch = (0 until currentLargeBatchSize).map { generateRandomData() }
                val subBatches = largeBatch.chunked(batchSize)

                val writeTimeMs = measureTimeMillis {
                    val batchResults = subBatches.map { subBatch ->
                        async(Dispatchers.IO) {
                            insertItemsBatch(container, subBatch)
                        }
                    }

                    batchResults.forEach { it.await() }
                }

                val totalBatchSize = subBatches.sumOf { it.size }
                val batchTps = (totalBatchSize / (writeTimeMs / 1000.0)).toInt()
                tpsValues.add(batchTps)

                remainingRecords -= currentLargeBatchSize
                insertedRecords += currentLargeBatchSize
            }
        }
    }

    val minTps = tpsValues.minOrNull() ?: 0
    val maxTps = tpsValues.maxOrNull() ?: 0
    val avgTps = if (tpsValues.isNotEmpty()) tpsValues.sum() / tpsValues.size else 0

    println("Total time: $totalTimeMs ms")
    println("Min TPS: $minTps, Max TPS: $maxTps, Avg TPS: $avgTps")
}

fun main() = runBlocking {
    try {
        createContainerIfNotExists()

        val container = database.getContainer(CONTAINER_NAME)
        val initialItemCount = getItemCount(container)

        writeData(container, RECORD_QUANTITY, BATCH_SIZE)

        val itemCount = getItemCount(container)
        if (itemCount != initialItemCount + RECORD_QUANTITY) {
            println("Discrepancy in data insertion: expected ${initialItemCount + RECORD_QUANTITY}, but found $itemCount.")
        }
    } catch (e: Exception) {
        println("An error occurred: ${e.message}")
    } finally {
        deleteContainer()
        cosmosClient.close()
    }
}
