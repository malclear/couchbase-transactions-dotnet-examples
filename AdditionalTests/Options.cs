using CommandLine;

namespace AdditionalTests
{

    public class Options
    {
        [Option('c', "cluster", Required = false, HelpText = "Specify Couchbase cluster address", Default = "couchbase://localhost")]
        public string Cluster { get; set; }

        [Option('u', "username", Required = false, HelpText = "Specify username of Couchbase user", Default = "Administrator")]
        public string UserName { get; set; }

        [Option('p', "password", Required = false, HelpText = "Specify password of Couchbase user", Default = "password")]
        public string Password { get; set; }

        [Option('b', "bucket", Required = false, HelpText = "Specify name of Couchbase bucket", Default = "default")]
        public string Bucket{ get; set; }

        [Option('d', "durability", Required = false, HelpText = "Durability setting to use: majority,none,persist_to_majority,majority_and_persist (default:majority)", Default = "majority")]
        public string Durability { get; set; }

        [Option('v', "verbose", Required = false, HelpText = "Logs all transaction trace to stdout (very heavy).", Default = false)]
        public bool Verbose { get; set; }
    }
}