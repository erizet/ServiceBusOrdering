using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs;
using NSubstitute;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure;
using NSubstitute.ExceptionExtensions;

namespace OleterLock.Test
{
    [TestClass]
    [DoNotParallelize]
    public class OrderingService_tests
    {
        [TestMethod]
        public async Task DoWork_is_run_when_no_previous_key()
        {
            var key = "testevent1";
            var no = 1;
            var eventIsHandled = false;
            var lease = BlobsModelFactory.BlobLease(new Azure.ETag("test"), DateTimeOffset.Now, "lease1");
            var metadata = new Dictionary<string, string>();
            var props = BlobsModelFactory.BlobProperties(metadata: metadata);
            var bc = Substitute.For<BlobClient>();
            bc.GetPropertiesAsync(Arg.Any<BlobRequestConditions>()).Returns(Response.FromValue<BlobProperties>(props, default!));

            var handleResult = await OrderingService.DoWorkOnHigher(bc, lease, key, no, () =>
            {
                eventIsHandled = true;
            });

            Assert.AreEqual(OrderingService.DoWorkOnHigherResult.EventHandled, handleResult);
            Assert.IsTrue(eventIsHandled);

            await bc.Received().SetMetadataAsync(Arg.Is<IDictionary<string, string>>(x => x[key] == no.ToString()), Arg.Any<BlobRequestConditions>());
        }

        [TestMethod]
        public async Task DoWork_is_run_when_lower_value_exists()
        {
            var key = "testevent1";
            var no = 100;
            var eventIsHandled = false;
            var lease = BlobsModelFactory.BlobLease(new Azure.ETag("test"), DateTimeOffset.Now, "lease1");
            var metadata = new Dictionary<string, string>();
            metadata[key] = (no - 1).ToString();
            var props = BlobsModelFactory.BlobProperties(metadata: metadata);
            var bc = Substitute.For<BlobClient>();
            bc.GetPropertiesAsync(Arg.Any<BlobRequestConditions>()).Returns(Response.FromValue<BlobProperties>(props, default!));

            var handleResult = await OrderingService.DoWorkOnHigher(bc, lease, key, no, () =>
            {
                eventIsHandled = true;
            });

            Assert.AreEqual(OrderingService.DoWorkOnHigherResult.EventHandled, handleResult);
            Assert.IsTrue(eventIsHandled);

            await bc.Received().SetMetadataAsync(Arg.Is<IDictionary<string, string>>(x => x[key] == no.ToString()), Arg.Any<BlobRequestConditions>());
        }

        [TestMethod]
        public async Task DoWork_doesnt_run_when_higher_value_exists()
        {
            var key = "testevent1";
            var no = 1;
            var eventIsHandled = false;
            var lease = BlobsModelFactory.BlobLease(new Azure.ETag("test"), DateTimeOffset.Now, "lease1");
            var metadata = new Dictionary<string, string>();
            metadata[key] = (no + 1).ToString();
            var props = BlobsModelFactory.BlobProperties(metadata: metadata);
            var bc = Substitute.For<BlobClient>();
            bc.GetPropertiesAsync(Arg.Any<BlobRequestConditions>()).Returns(Response.FromValue<BlobProperties>(props, default!));

            var handleResult = await OrderingService.DoWorkOnHigher(bc, lease, key, no, () =>
            {
                eventIsHandled = true;
            });

            Assert.AreEqual(OrderingService.DoWorkOnHigherResult.EventOutOfOrder, handleResult);
            Assert.IsFalse(eventIsHandled);

            await bc.DidNotReceiveWithAnyArgs().SetMetadataAsync(Arg.Any<IDictionary<string, string>>(), Arg.Any<BlobRequestConditions>());
        }

        [TestMethod]
        [ExpectedException(typeof(RequestFailedException))]
        public async Task DoWork_throws_if_metadata_dont_can_be_updated()
        {
            var key = "testevent1";
            var no = 1;
            var lease = BlobsModelFactory.BlobLease(new Azure.ETag("test"), DateTimeOffset.Now, "lease1");
            var metadata = new Dictionary<string, string>();
            var props = BlobsModelFactory.BlobProperties(metadata: metadata);
            var bc = Substitute.For<BlobClient>();
            bc.GetPropertiesAsync(Arg.Any<BlobRequestConditions>()).Returns(Response.FromValue<BlobProperties>(props, default!));
            bc.SetMetadataAsync(Arg.Any<IDictionary<string, string>>(), Arg.Any<BlobRequestConditions>()).Throws(new RequestFailedException("test"));

            var handleResult = await OrderingService.DoWorkOnHigher(bc, lease, key, no, () =>
            {
            });
        }

    }
}
