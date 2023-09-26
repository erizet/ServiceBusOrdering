using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NSubstitute;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OleterLock.Test
{
    [TestClass]
    [DoNotParallelize]
    public class BlobLock_tests
    {
        private BlobContainerClient? _container;

        [TestInitialize]
        public void Init()
        {
            _container = new BlobContainerClient("UseDevelopmentStorage=true", "test");
            _container.CreateIfNotExists();
        }

        private BlobClient GetBlobClient(string blobName)
        {
            var client = _container!.GetBlobClient(blobName);
            if (client.Exists())
                client.SetMetadata(new Dictionary<string, string>());
            else
                client.Upload(Stream.Null);

            return client;
        }

        [TestMethod]
        public async Task Callback_is_run_when_lease_is_acquired()
        {
            var blobName = "test2";
            var client = GetBlobClient(blobName);

            var callbackReached = false;

            var lockReceivedAndWorkRun = await BlobLock.TryLockAndDoWork(client, TimeSpan.FromSeconds(15), (client, lease) =>
            {
                callbackReached = true;
                return Task.CompletedTask;
            });

            Assert.IsTrue(callbackReached);  
            Assert.IsTrue(lockReceivedAndWorkRun);
        }

        [TestMethod]
        public async Task Callback_is_NOT_run_when_lease_is_NOT_acquired()
        {
            var blobName = "test2";
            var client = GetBlobClient(blobName);
            var leaseClient = client.GetBlobLeaseClient();
            var blobLease = leaseClient.Acquire(TimeSpan.FromSeconds(15));
            var callbackReached = false;

            var lockReceivedAndWorkRun = await BlobLock.TryLockAndDoWork(client, TimeSpan.FromSeconds(15), (client, lease) =>
            {
                callbackReached = true;
                return Task.CompletedTask;
            });

            leaseClient.Release();

            Assert.IsFalse(callbackReached);
            Assert.IsFalse(lockReceivedAndWorkRun);
        }

        [TestMethod]
        public async Task Release_is_called_when_callback_throws()
        {
            var lease = BlobsModelFactory.BlobLease(new Azure.ETag("test"), DateTimeOffset.Now, "lease1");

            var bc = Substitute.For<BlobClient>();
            var blc = Substitute.For<BlobLeaseClient>();
            bc.GetBlobLeaseClient().Returns(blc);
            blc.AcquireAsync(Arg.Any<TimeSpan>()).Returns(Task.FromResult(Response.FromValue<BlobLease>(lease, default!)));

            var callbackReached = false;

            await Assert.ThrowsExceptionAsync<Exception>(async () =>
            {
                var lockReceivedAndWorkRun = await BlobLock.TryLockAndDoWork(bc, TimeSpan.FromSeconds(10), (client, lease) =>
                {
                    throw new Exception();
                });
            });

            Assert.IsFalse(callbackReached);
            await blc.Received(1).ReleaseAsync();
        }

    }
}
