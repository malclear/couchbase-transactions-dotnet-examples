using System;
using System.Threading.Tasks;
using Couchbase.KeyValue;
using Couchbase.Transactions.Config;
using Couchbase.Transactions.Error;
using Newtonsoft.Json.Linq;

namespace Couchbase.Transactions.Examples
{
    class Program : IDisposable
    {
        private readonly Transactions _transactions;
        private readonly ICluster _cluster;
        private readonly IBucket _bucket;
        private readonly ICouchbaseCollection _collection;

        public Program(ICluster cluster, IBucket bucket, ICouchbaseCollection collection, Transactions transactions)
        {
            _cluster = cluster;
            _bucket = bucket;
            _collection = collection;
            _transactions = transactions;
        }

        static async Task Main(string[] args)
        {
            var options = new ClusterOptions().WithCredentials("Administrator", "password");
            var cluster = await Cluster.ConnectAsync("couchbase://localhost", options).ConfigureAwait(false);
            var bucket = await cluster.BucketAsync("default").ConfigureAwait(false);
            var collection = bucket.DefaultCollection();

            var transactions = Transactions.Create(cluster, TransactionConfigBuilder.Create());
            using var program = new Program(cluster, bucket, collection, transactions);


            Console.WriteLine("Hello World!");
        }

        void Config()
        {
            // #tag::config[]
            var transactions = Transactions.Create(_cluster,
                TransactionConfigBuilder.Create()
                    .DurabilityLevel(DurabilityLevel.PersistToMajority)
                   /* // #tag::config_warn[]
                    .LogOnFailure(true, Event.Severity.WARN)
                    // #end::config_warn[]*/
                    .Build());
            // #end::config[]
        }

        async Task CreateAsync()
        {
            // #tag::create[]
            try
            {
                await _transactions.RunAsync(async (ctx)=>
                {
                    // 'ctx' is an AttemptContext, which permits getting, inserting,
                    // removing and replacing documents, along with committing and
                    // rolling back the transaction.

                    // ... Your transaction logic here ...

                    // This call is optional - if you leave it off, the transaction
                    // will be committed anyway.
                    await ctx.CommitAsync().ConfigureAwait(false);
                }).ConfigureAwait(false);
                // #tag::logging[]
            }
            catch (TransactionCommitAmbiguousException e)
            {
                // The application will of course want to use its own logging rather
                // than Console.WriteLine
                Console.Error.WriteLine("Transaction possibly committed");
                Console.Error.WriteLine(e);
            }
            catch (TransactionFailedException e)
            {
                Console.Error.WriteLine("Transaction did not reach commit point");
                Console.Error.WriteLine(e);
            }
            // #tag::logging[]
            // #end::create[]
        }

        async Task Examples()
        {
            // #tag::examples[]
            try
            {
                var result = await _transactions.RunAsync(async (ctx) =>
                {
                    // Inserting a doc:
                    await ctx.InsertAsync(_collection, "doc-a", new {}).ConfigureAwait(false);

                    // Getting documents:
                    // Use ctx.GetAsync if the document should exist, and the transaction
                    // will fail if it does not
                    var docA = await ctx.GetAsync(_collection, "doc-a").ConfigureAwait(false);

                    // Replacing a doc:
                    var docB = await ctx.GetAsync(_collection, "doc-b").ConfigureAwait(false);
                    var content = docB.ContentAs<dynamic>();
                    content.put("transactions", "are awesome");
                    await ctx.ReplaceAsync(docB, content);

                    // Removing a doc:
                    var docC = await ctx.GetAsync(_collection, "doc-c").ConfigureAwait(false);
                    await ctx.RemoveAsync(docC).ConfigureAwait(false);

                    await ctx.CommitAsync().ConfigureAwait(false);
                }).ConfigureAwait(false);
            }
            catch (TransactionCommitAmbiguousException e)
            {
               Console.WriteLine("Transaction possibly committed");
               Console.WriteLine(e);
            }
            catch (TransactionFailedException e)
            {
                Console.WriteLine("Transaction did not reach commit point");
                Console.WriteLine(e);
            }
            // #end::examples[]
        }

        private async Task InsertAsync()
        {
            // #tag::insert[]
            await _transactions.RunAsync(async ctx =>
            {
                await ctx.InsertAsync(_collection, "docId", new { }).ConfigureAwait(false);
            }).ConfigureAwait(false);

            // #end::insert[]
        }

        private async Task GetAsync()
        {
            // #tag::get[]
            await _transactions.RunAsync(async ctx =>
            {
                var docId = "a-doc";
                var docOpt = await ctx.GetAsync(_collection, docId).ConfigureAwait(false);
            }).ConfigureAwait(false);
            // #end::get[]
        }

        private async Task GetReadOwnWritesAsync()
        {
            // #tag::getReadOwnWrites[]
            await _transactions.RunAsync(async ctx =>
            {
                var docId = "docId";
                await ctx.InsertAsync(_collection, docId, new { }).ConfigureAwait(false);
                var doc = await ctx.GetAsync(_collection, docId).ConfigureAwait(false);
                Console.WriteLine((object) doc.ContentAs<dynamic>());
            }).ConfigureAwait(false);
            // #end::getReadOwnWrites[]
        }

        async Task ReplaceAsync()
        {
            // #tag::replace[]
            await _transactions.RunAsync(async ctx =>
            {
                var anotherDoc = await ctx.GetAsync(_collection, "anotherDoc").ConfigureAwait(false);
                var content = anotherDoc.ContentAs<dynamic>();
                content.put("transactions", "are awesome");
                await ctx.ReplaceAsync(anotherDoc, content);
            }).ConfigureAwait(false);
            // #end::replace[]
        }

        private async Task RemoveAsync()
        {
            // #tag::remove[]
            await _transactions.RunAsync(async ctx =>
            {
                var anotherDoc = await ctx.GetAsync(_collection, "anotherDoc").ConfigureAwait(false);
                await ctx.RemoveAsync(anotherDoc).ConfigureAwait(false);
            }).ConfigureAwait(false);
            // #end::remove[]
        }

        private async Task CommitAsync()
        {
            // #tag::commit[]
            var result = await _transactions.RunAsync(async (ctx) =>
            {
                var doc = await ctx.GetAsync(_collection, "anotherDoc").ConfigureAwait(false);
                var content = doc.ContentAs<JObject>();
                content.Add("transactions", "are awesome");

                await ctx.ReplaceAsync(doc, content).ConfigureAwait(false);
            }).ConfigureAwait(false);
            // #end::commit[]
        }

        public void Dispose()
        {
            _cluster.Dispose();
            _transactions.Dispose();
        }
    }
}
