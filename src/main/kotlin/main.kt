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
val P_KEY = System.getenv("P_KEY") ?: "/account"
val RECORD_QUANTITY = System.getenv("RECORD_QUANTITY")?.toInt() ?: 50000
val BATCH_SIZE = System.getenv("BATCH_SIZE")?.toInt() ?: 100
val RU_VALUE = System.getenv("RU_VALUE")?.toInt() ?: 10000
val BUFFER = System.getenv("BUFFER")?.toInt() ?: 1000

val cosmosClient = CosmosClientBuilder()
    .endpoint(COSMOS_URL)
    .key(COSMOS_KEY)
    .consistencyLevel(ConsistencyLevel.EVENTUAL)
    .contentResponseOnWriteEnabled(false)
    .buildAsyncClient()

val database = cosmosClient.getDatabase(DATABASE_NAME)

fun validateEnvironmentVariables() {
    if (RECORD_QUANTITY <= 0) {
        throw IllegalArgumentException("RECORD_QUANTITY must be greater than 0")
    }

    if (BATCH_SIZE <= 0 || BATCH_SIZE > RECORD_QUANTITY || BATCH_SIZE > 100) {
        throw IllegalArgumentException("BATCH_SIZE must be greater than 0, less than or equal to RECORD_QUANTITY, and less than or equal to 100")
    }

    if (RU_VALUE <= 0) {
        throw IllegalArgumentException("RU_VALUE must be greater than 0")
    }

    if (BUFFER < 0) {
        throw IllegalArgumentException("BUFFER must not be negative")
    }

    if (BUFFER > RECORD_QUANTITY) {
        throw IllegalArgumentException("BUFFER must not be greater than RECORD_QUANTITY")
    }

    if (BATCH_SIZE > BUFFER) {
        throw IllegalArgumentException("BATCH_SIZE must not be greater than BUFFER")
    }
}

suspend fun databaseExists(client: CosmosAsyncClient, databaseName: String): Boolean {
    return try {
        val databases = client.readAllDatabases()
            .byPage()
            .toIterable()
            .flatMap { it.results }
        databases.any { it.id == databaseName }
    } catch (e: Exception) {
        println("Failed to check if database exists: ${e.message}")
        false
    }
}

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
        val containerProperties = CosmosContainerProperties(CONTAINER_NAME, P_KEY)
        val throughputProperties = ThroughputProperties.createAutoscaledThroughput(RU_VALUE)
        // val throughputProperties = ThroughputProperties.createManualThroughput(RU_VALUE)
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

suspend fun insertItemsBatch(container: CosmosAsyncContainer, batch: List<AccountData>, errorCounter: MutableMap<String, Int>) {
    try {
        val partitionKey = batch.first().account
        val transactionalBatch = CosmosBatch.createCosmosBatch(PartitionKey(partitionKey))
        batch.forEach { item ->
            transactionalBatch.createItemOperation(item)
        }
        val response = container.executeCosmosBatch(transactionalBatch).awaitSingle()
        if (!response.isSuccessStatusCode) {
            val errorCode = response.statusCode.toString()
            errorCounter[errorCode] = errorCounter.getOrDefault(errorCode, 0) + 1
        }
    } catch (e: Exception) {
        println("Unexpected error during batch insert: ${e.message}")
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
    val errorCounter = mutableMapOf<String, Int>()
    var insertedRecords = 0
    var totalWriteTimeMs = 0L // Переменная для учета времени записи

    val totalTimeMs = measureTimeMillis {
        coroutineScope {
            var remainingRecords = totalRecords
            while (remainingRecords > 0) {
                val currentLargeBatchSize = minOf(BUFFER, remainingRecords)
                val largeBatch = (0 until currentLargeBatchSize).map { generateRandomData() }
                val subBatches = largeBatch.chunked(batchSize)

                val writeTimeMs = measureTimeMillis {
                    val batchResults = subBatches.map { subBatch ->
                        async(Dispatchers.IO) {
                            insertItemsBatch(container, subBatch, errorCounter)
                        }
                    }

                    batchResults.forEach { it.await() }
                }

                totalWriteTimeMs += writeTimeMs // Накопление времени записи

                val totalBatchSize = subBatches.sumOf { it.size }
                val batchTps = (totalBatchSize / (writeTimeMs / 1000.0)).toInt()
                tpsValues.add(batchTps)

                println("Inserted $currentLargeBatchSize records in $writeTimeMs ms (TPS: $batchTps)")
                remainingRecords -= currentLargeBatchSize
                insertedRecords += currentLargeBatchSize
            }
        }
    }

    val minTps = tpsValues.minOrNull() ?: 0
    val maxTps = tpsValues.maxOrNull() ?: 0
    val avgTps = if (tpsValues.isNotEmpty()) tpsValues.sum() / tpsValues.size else 0

    println("Total records inserted: $insertedRecords")
    println("Total time: $totalTimeMs ms")
    println("Total write time: $totalWriteTimeMs ms") // Вывод времени записи
    println("Proportion of write time to total time: ${"%.2f".format(totalWriteTimeMs * 100.0 / totalTimeMs)}%") // Пропорция
    println("Min TPS: $minTps, Max TPS: $maxTps, Avg TPS: $avgTps")
    println("Error summary: $errorCounter")
}

fun main() = runBlocking {
    try {
        validateEnvironmentVariables()
        println("All environment variables are valid")

        println("Checking if database exists...")
        val databaseExists = databaseExists(cosmosClient, DATABASE_NAME)
        if (!databaseExists) {
            println("Database '$DATABASE_NAME' does not exist. Please create it before running the program.")
            return@runBlocking
        }
        
        println("Creating container...")
        createContainerIfNotExists()

        val container = database.getContainer(CONTAINER_NAME)
        val initialItemCount = getItemCount(container)
        println("Total items in container before insertion: $initialItemCount")

        println("Starting data insertion...")
        writeData(container, RECORD_QUANTITY, BATCH_SIZE)

        val itemCount = getItemCount(container)
        println("Total items in container after insertion: $itemCount")

        if (itemCount == initialItemCount + RECORD_QUANTITY) {
            println("Data insertion verified: $itemCount items present as expected.")
        } else {
            println("Discrepancy in data insertion: expected ${initialItemCount + RECORD_QUANTITY}, but found $itemCount.")
        }
    } catch (e: Exception) {
        println("An error occurred: ${e.message}")
    } finally {
        println("Cleaning up...")
        deleteContainer()
        cosmosClient.close()
    }
}
