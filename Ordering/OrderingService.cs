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
    }
}