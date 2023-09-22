﻿using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Blobs;
using Azure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OleterLock
{
    public static class BlobLock
    {
        public static async Task<bool> TryLockAndDoWork(BlobClient blobClient, TimeSpan lockTime, Action<BlobClient, BlobLease> doWorkWhenLocked)
        {
            BlobLeaseClient blobLeaseClient = blobClient.GetBlobLeaseClient();
            try
            {
                // Acquire a lease on the blob.
                BlobLease blobLease = await blobLeaseClient.AcquireAsync(lockTime);

                try
                {
                    doWorkWhenLocked(blobClient, blobLease);
                }
                finally
                {
                    await blobLeaseClient.ReleaseAsync();
                }
            }
            catch (RequestFailedException)
            {
                return false;
            }

            return true;
        }

    }
}