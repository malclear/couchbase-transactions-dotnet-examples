using CommandLine;
using Couchbase;
using Couchbase.KeyValue;
using Couchbase.Transactions;
using Couchbase.Transactions.Config;

namespace AdditionalTests
{
    public class CbStuff
    {
        private Options options;
        private TransactionConfigBuilder config;
        
        public static int InstanceCount = 0;
        public CbStuff()
        { 
            Parser.Default.ParseArguments<Options>(new string[] { }).WithParsed(RunOptions);
            config = TransactionConfigBuilder.Create();
            config.DurabilityLevel(DurabilityLevel.None);
            
            Cluster = Couchbase.Cluster.ConnectAsync(options.Cluster, options.UserName, options.Password).Result;
            Bucket = Cluster.BucketAsync(options.Bucket).Result;
            Collection = Bucket.DefaultCollection();
            Transactions = Transactions.Create(Cluster, config);
            Cluster.Buckets.FlushBucketAsync("default").GetAwaiter().GetResult();
        }
        
        public ICluster Cluster { get; }
        public IBucket Bucket { get; }
        public ICouchbaseCollection Collection { get; }
        public Transactions Transactions { get; }

        private void RunOptions(Options o)
        {
            options = o;
        }
    }
}