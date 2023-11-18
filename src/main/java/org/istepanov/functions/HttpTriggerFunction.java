package org.istepanov.functions;

import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Optional;

/**
 * Azure Functions with HTTP Trigger.
 */
public class HttpTriggerFunction {

    public String storageConnectionString =
            "DefaultEndpointsProtocol=https;" +
                    "AccountName=petstorestorage;" +
                    "AccountKey=";
    /**
     * This function listens at endpoint "/api/reserveOrder". Two ways to invoke it using "curl" command in bash:
     * 1. curl -d "HTTP Body" {your host}/api/reserveOrder
     * 2. curl "{your host}/api/reserveOrder?sessionId=HTTP%20Query"
     */
    @FunctionName("reserveOrder")
    public HttpResponseMessage run(
            @HttpTrigger(
                    name = "req",
                    methods = {HttpMethod.POST},
                    authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) throws URISyntaxException, InvalidKeyException, StorageException {
        context.getLogger().info("Java HTTP trigger processed a request.");

        // Parse query parameter
        final String sessionId = request.getQueryParameters().get("sessionId");
        final String body = request.getBody().orElse("");

        storageConnectionString = storageConnectionString + System.getenv("storageKey");

        // Retrieve storage account from connection-string.
        CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);

        // Create the blob client.
        CloudBlobClient blobClient = storageAccount.createCloudBlobClient();

        // Get a reference to a container.
        // The container name must be lower case
        CloudBlobContainer container = blobClient.getContainerReference("orders");
        CloudBlockBlob blockBlobReference = container.getBlockBlobReference(sessionId);

        try {
            blockBlobReference.uploadText(body);
        } catch (IOException e) {

            return request
                    .createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body("Unable to upload file.\n" + e.getCause())
                    .build();
        }

        return request.createResponseBuilder(HttpStatus.OK)
                .body(String.format("Order reserve for sessionId [%s] saved to %s",
                        sessionId, container.getName()
                )).build();
    }
}
