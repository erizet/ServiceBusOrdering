using Azure.Storage.Blobs;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using OleterLock;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ordering.Test
{
    [DoNotParallelize]
    [TestClass]
    public class WorkerTests
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

        [DataTestMethod]
        [DataRow(1, 100)]
        [DataRow(2, 100)]
        [DataRow(5, 100)]
        [DataRow(10, 100)]
        public void MultipleWorkers(int noOfThreads, int noOfValues)
        {
            var _ = GetBlobClient("test1");
            var values = new ConcurrentQueue<int>(Enumerable.Range(1, noOfValues).Select(_ => Random.Shared.Next(noOfValues)));

            var handled = new ConcurrentQueue<long>();

            var threads = Enumerable.Range(1, noOfThreads).Select(no =>
            {
                var t = new Thread(() =>
                {
                    try
                    {
                        while (values.TryDequeue(out var no))
                        {
                            var bc = _container!.GetBlobClient("test1");
                            var a = testfunc(bc, no).Result;
                        }
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

            async Task<bool> testfunc(BlobClient bc, long no)
            {
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
                        });
                    });
                }
                return lockReceived;
            };
        }
    }
}
