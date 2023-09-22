using System.Net;
using System.Reflection.Metadata;
using System.Text;
using Azure;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

namespace OleterLock
{
    public class OrderingService
    {
        /// <summary>
        /// Runs action doWork if the provided number is higher than the number stored as metadata with the key metadataKey 
        /// </summary>
        /// <param name="blobClient"></param>
        /// <param name="blobLease"></param>
        /// <param name="metadataKey"></param>
        /// <param name="no"></param>
        /// <param name="doWork"></param>
        /// <returns></returns>
        public static async Task<DoWorkOnHigherResult> DoWorkOnHigher(BlobClient blobClient, BlobLease blobLease, string metadataKey, long no, Action doWork)
        {
            // Get the blob's properties and metadata.
            BlobProperties properties = await blobClient.GetPropertiesAsync(new BlobRequestConditions() { LeaseId = blobLease.LeaseId });

            var metaData = properties.Metadata ?? new Dictionary<string, string>();

            var lastSequenceNo = GetLastSequenceNo(metaData);
            if (lastSequenceNo >= no)
                return DoWorkOnHigherResult.EventOutOfOrder;    // a newer event has already been handled
 
            // We have the next event in sequence
            doWork();

            // Update the blob's metadata.
            metaData[metadataKey] = no.ToString();
            await blobClient.SetMetadataAsync(metaData, new BlobRequestConditions() { LeaseId = blobLease.LeaseId });

            return DoWorkOnHigherResult.EventHandled;

            long GetLastSequenceNo(IDictionary<string, string> metaData)
            {
                if (metaData.TryGetValue(metadataKey, out var lastEvent))
                {
                    if (long.TryParse(lastEvent, out var lastEventNo))
                    {
                        return lastEventNo;
                    }
                }

                return 0;
            }
        }

        public enum DoWorkOnHigherResult
        {
            EventHandled,
            EventOutOfOrder
        }



        public static async Task DemonstratePessimisticConcurrencyBlob(BlobClient blobClient)
        {
            Console.WriteLine("Demonstrate pessimistic concurrency");

            BlobContainerClient containerClient = blobClient.GetParentBlobContainerClient();
            BlobLeaseClient blobLeaseClient = blobClient.GetBlobLeaseClient();

            try
            {
                // Create the container if it does not exist.
                await containerClient.CreateIfNotExistsAsync();

                // Upload text to a blob.
                string blobContents1 = "First update. Overwrite blob if it exists.";
                byte[] byteArray = Encoding.ASCII.GetBytes(blobContents1);
                using (MemoryStream stream = new MemoryStream(byteArray))
                {
                    BlobContentInfo blobContentInfo = await blobClient.UploadAsync(stream, overwrite: true);
                }

                // Acquire a lease on the blob.
                BlobLease blobLease = await blobLeaseClient.AcquireAsync(TimeSpan.FromSeconds(15));
                Console.WriteLine("Blob lease acquired. LeaseId = {0}", blobLease.LeaseId);

                // Set the request condition to include the lease ID.
                BlobUploadOptions blobUploadOptions = new BlobUploadOptions()
                {
                    Conditions = new BlobRequestConditions()
                    {
                        LeaseId = blobLease.LeaseId
                    }
                };

                // Write to the blob again, providing the lease ID on the request.
                // The lease ID was provided, so this call should succeed.
                string blobContents2 = "Second update. Lease ID provided on request.";
                byteArray = Encoding.ASCII.GetBytes(blobContents2);

                using (MemoryStream stream = new MemoryStream(byteArray))
                {
                    BlobContentInfo blobContentInfo = await blobClient.UploadAsync(stream, blobUploadOptions);
                }

                // This code simulates an update by another client.
                // The lease ID is not provided, so this call fails.
                string blobContents3 = "Third update. No lease ID provided.";
                byteArray = Encoding.ASCII.GetBytes(blobContents3);

                using (MemoryStream stream = new MemoryStream(byteArray))
                {
                    // This call should fail with error code 412 (Precondition Failed).
                    BlobContentInfo blobContentInfo = await blobClient.UploadAsync(stream);
                }
            }
            catch (RequestFailedException e)
            {
                if (e.Status == (int)HttpStatusCode.PreconditionFailed)
                {
                    Console.WriteLine(
                        @"Precondition failure as expected. The lease ID was not provided.");
                }
                else
                {
                    Console.WriteLine(e.Message);
                    throw;
                }
            }
            finally
            {
                await blobLeaseClient.ReleaseAsync();
            }
        }
    }
}