package org.istepanov.functions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.ServiceBusQueueTrigger;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

/**
 * Azure Functions with ServiceBus Trigger.
 */
public class ServiceBusTriggerFunction {

    public String storageConnectionString =
            "DefaultEndpointsProtocol=https;" +
                    "AccountName=petstorestorage;" +
                    "AccountKey=";


    @FunctionName("reserveOrder")
    public void run(
            @ServiceBusQueueTrigger(name = "reserveOrder",
                    queueName = "is-petstoreapp-orders-queue",
                    connection = "connectionString")
            String message, final ExecutionContext context
    ) throws URISyntaxException, StorageException, InvalidKeyException, IOException {
        context.getLogger().info("Java ServiceBus trigger processing a request.");


        context.getLogger().info("Message received: " + message);

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(message);


        storageConnectionString = storageConnectionString + System.getenv("storageKey");

        // Retrieve storage account from connection-string.
        CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);

        // Create the blob client.
        CloudBlobClient blobClient = storageAccount.createCloudBlobClient();

        if (jsonNode.has("id")) {

            JsonNode orderId = jsonNode.get("id");

            try {
                uploadOrder(message, blobClient, orderId);
            } catch (StorageException | IOException e) {
                context.getLogger().info("Failed to upload order to storage, will try again");
//
                throw new RuntimeException("failed to save");
            }
        }
    }

    private void uploadOrder(String message, CloudBlobClient blobClient, JsonNode orderId) throws URISyntaxException, StorageException, IOException {
        // Get a reference to a container.
        // The container name must be lower case
        CloudBlobContainer container = blobClient.getContainerReference("orders");
        CloudBlockBlob blockBlobReference = container.getBlockBlobReference(orderId.asText() + ".json");
        blockBlobReference.uploadText(message);
    }

}


