using System;
using System.Threading.Tasks;
using CommandLine;
using Couchbase;
using Couchbase.Core.Exceptions;
using Couchbase.KeyValue;
using Couchbase.Transactions;
using Couchbase.Transactions.Config;
using Couchbase.Transactions.Examples.Game;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ILoggerFactory = Microsoft.Extensions.Logging.ILoggerFactory;

namespace game
{
    public class Program
    {
        private static Options _options;

        static async Task Main(string[] args)
        {
            Parser.Default.ParseArguments<Options>(args).WithParsed(RunOptions);

            var config = TransactionConfigBuilder.Create();
            config.DurabilityLevel(ParseDurability(_options.Durability));

            // Initialize the Couchbase cluster
            var cluster = await Cluster.ConnectAsync(_options.Cluster, _options.UserName, _options.Password).ConfigureAwait(false);
            var bucket = await cluster.BucketAsync(_options.Bucket).ConfigureAwait(false);
            var collection = bucket.DefaultCollection();

            // Initialize transactions.  Must only be one Transactions object per app as it creates background resources.
            var transactions = Transactions.Create(cluster, config);

            //Logging dependencies
            var services = new ServiceCollection();
            services.AddLogging(builder =>
            {
                builder.AddFile(AppContext.BaseDirectory);
                builder.AddConsole();
            });
            await using var provider = services.BuildServiceProvider();
            var loggerFactory = provider.GetService<ILoggerFactory>();
            var logger = loggerFactory.CreateLogger<Program>();

            var gameServer = new GameServer(transactions, collection, logger);

            // Initialise some sample data - a player and a monster.  This is based on the Game Simulation sample bucket
            // provided with Couchbase, though that does not have to be installed.
            var playerId = "player_jane";
            var player = new
            {
                experiance = 14248,
                hitpoints = 23832,
                jsonType = "player",
                level = 141,
                loggedIn = true,
                name = "Jane",
                uuid = Guid.NewGuid()
            };

            var monsterId = "a_grue";
            var monster = new
            {
                experienceWhenKilled = 91,
                hitpoints = 4000,
                itemProbability = 0.19239324085462631,
                name = "grue",
                uuid = Guid.NewGuid()
            };

            await collection.UpsertAsync(playerId, player).ConfigureAwait(false);

            logger.LogInformation($"Upserted sample player document {playerId}");

            await collection.UpsertAsync(monsterId, monster).ConfigureAwait(false);

            logger.LogInformation($"Upserted sample monster document {monsterId}");

            // Now perform the transaction
            // The player is hitting the monster for a certain amount of damage
            await gameServer.PlayerHitsMonster(

                // This UUID identifies this action from the player's client
                Guid.NewGuid().ToString(),

                // This has a 50% chance of killing the monster, which has 4000 hitpoints
                new Random().Next(0, 8000),

                playerId,
                monsterId).ConfigureAwait(false);

            // Shutdown resources cleanly
            transactions.Dispose();
            await cluster.DisposeAsync().ConfigureAwait(false);
            Console.Read();
        }

        public static void RunOptions(Options o)
        {
            Console.WriteLine("Current arguments:");
            Console.WriteLine($"-c {o.Cluster}");
            Console.WriteLine($"-u {o.UserName}");
            Console.WriteLine($"-p {o.Password}");
            Console.WriteLine($"-b {o.Bucket}");
            Console.WriteLine($"-d {o.Durability}");
            Console.WriteLine($"-v {o.Verbose}");

            _options = o;
        }

        private static DurabilityLevel ParseDurability(string durability)
        {
            switch (durability)
            {
                case "none":
                    return DurabilityLevel.None;
                case "majority":
                    return DurabilityLevel.Majority;
                case "persist_to_majority":
                    return DurabilityLevel.PersistToMajority;
                case "majority_and_persist":
                    return DurabilityLevel.MajorityAndPersistToActive;
                default:
                {
                    throw new InvalidArgumentException($"Unknown durability setting {durability}");
                }
            }
        }


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
}
