using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs;
using Azure;
using System.IO;
using Azure.Identity;
using System.Text;
using Azure.Storage.Blobs.Specialized;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Concurrent;

namespace OleterLock.Test
{
    [TestClass]
    [DoNotParallelize]
    public class UnitTest1
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

        public TestContext TestContext { get; set; }

        [TestMethod]
        public void ParallellTests()
        {
            var _ = GetBlobClient("test1");
            var values = Enumerable.Range(1, 200).Select(_ => Random.Shared.Next(1000));

            var handled = new ConcurrentQueue<long>();

            var threads = values.Select(no =>
            {
                var t = new Thread(() =>
                {
                    try
                    {
                        var bc = _container!.GetBlobClient("test1");
                        var a = testfunc(bc, no).Result;
                        //if (a)
                        //    TestContext.WriteLine($"{no} HANDLED");
                        //else
                        //    TestContext.WriteLine($"{no} is NOT handled");
                    }
                    catch (AggregateException ex)
                    {
                        TestContext.WriteLine(ex.ToString());
                    }
                    catch (Exception ex)
                    {
                        TestContext.WriteLine(ex.ToString());
                    }
                });
                t.Start();
                TestContext.WriteLine($"{no} started");
                return t;
            }
            ).ToList();


            foreach (var t in threads)
                t.Join();

            TestContext.WriteLine($"{handled.Count} is handled");

            var previous = 0L;
            while (handled.TryDequeue(out var no))
            {
                Assert.IsTrue(previous < no);
                previous = no;
            }

            //var opt = new ParallelOptions() { MaxDegreeOfParallelism = 10 };
            //Parallel.ForEach(queue, opt, async (no) =>
            //{
            //    var blobName = "test1";
            //    var bc = _container!.GetBlobClient(blobName);
            //    var isHandled = false;
            //    while(!isHandled)
            //    {
            //        isHandled = await testfunc(bc, no);

            //        if(!isHandled)
            //            System.Threading.Thread.Sleep(10);
            //        else
            //            TestContext.WriteLine($"{no} is handled");
            //    }
            //});


            async Task<bool> testfunc(BlobClient bc, long no)
            {
                //Thread.Sleep(100);
                //TestContext.WriteLine($"{DateTime.Now.TimeOfDay} - {no} in testfunc");
                var lockReceived = false;
                while (!lockReceived)
                {
                    lockReceived = await BlobLock.TryLockAndDoWork(bc, TimeSpan.FromSeconds(59), (client, lease) =>
                    {
                        //Thread.Sleep(10);
                        var handleResult = OrderingService.DoWorkOnHigher(client, lease, "testevent1", no, () =>
                        {
                            TestContext.WriteLine($"{no} is handled");
                            handled.Enqueue(no);
                        }).Result;
                    });
                }
                return lockReceived;
            };
        }


        [TestMethod]
        public async Task HandleEvent()
        {
            var blobName = "test2";
            var bc = GetBlobClient(blobName);
            OrderingService.DoWorkOnHigherResult handleResult = OrderingService.DoWorkOnHigherResult.EventOutOfOrder;
            var eventIsHandled = false;

            var lockReceived = await BlobLock.TryLockAndDoWork(bc, TimeSpan.FromSeconds(59), async (client, lease) =>
            {
                handleResult = await OrderingService.DoWorkOnHigher(client, lease, "testevent1", 1, () =>
                {
                    eventIsHandled = true;
                });
            });

            Assert.IsTrue(lockReceived);
            Assert.IsTrue(eventIsHandled);
            //Assert.AreEqual(MetadataLock.DoWorkOnHigherResult.EventHandled, handleResult);
        }

        //        [TestMethod]
        public void TestMethod1()
        {
            var blobName = "test1";
            //            var container = new BlobContainerClient(_connStr, new DefaultAzureCredential());
            var container = new BlobContainerClient("UseDevelopmentStorage=true", "test");
            var containerInfo = container.CreateIfNotExists();
            var client = container.GetBlobClient(blobName);


            var leaseClient = client.GetBlobLeaseClient();
            var leaseClient2 = client.GetBlobLeaseClient();

            var blobLease = leaseClient.Acquire(TimeSpan.FromSeconds(15));
            leaseClient.Release();
            var blobLease2 = leaseClient.Acquire(TimeSpan.FromSeconds(15));

            var opt = new BlobUploadOptions()
            {
                HttpHeaders = new BlobHttpHeaders() { ContentType = "text/xml" }
            };

            Console.WriteLine(client.BlobContainerName);

            // Upload file data
            //var result = blob.Upload(stream, opt);

            if (!client.Exists())
                client.Upload(BinaryData.FromString("test"));

            client.SetMetadata(new Dictionary<string, string>() { { "senaste", DateTime.UtcNow.ToString() } }, new BlobRequestConditions() { LeaseId = blobLease.Value.LeaseId });

            leaseClient.Release();

            //Console.WriteLine(res);

        }
    }
}