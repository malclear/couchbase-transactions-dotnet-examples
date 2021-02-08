using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Reflection;
using System.Threading.Tasks;
using Chill;
using Couchbase.Core.Exceptions.KeyValue;
using Couchbase.Transactions;
using Couchbase.Transactions.Error;
using Couchbase.Transactions.Support;
using Xunit;
using Xunit.Abstractions;
using FluentAssertions;

namespace AdditionalTests
{
    [CollectionDefinition("CouchbaseTx.Tests")]
    public class MyCollectionDefinition : ICollectionFixture<CbStuff>
    {
    }

    [Collection("CouchbaseTx.Tests")]
    public class Given_a_configured_Couchbase_instance : GivenSubject<CbStuff>
    {
        public static CbStuff couchbase;

        public Given_a_configured_Couchbase_instance(CbStuff couchbaseStuff, ITestOutputHelper helper)
        {
            
            couchbase = couchbaseStuff;

            TestName = GetName(helper);
            WithSubject(_ => couchbase);
        }

        public string TestName { get; set; }

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
        
        private static Options _options;
        
        private string GetName(ITestOutputHelper helper)
        {
            var type = helper.GetType();
            var testMember = type.GetField("test", BindingFlags.Instance | BindingFlags.NonPublic);
            return  ((ITest) testMember.GetValue(helper)).DisplayName;
        }
    }

    public class When_using_standard_ops: Given_a_configured_Couchbase_instance
    {
        public When_using_standard_ops(CbStuff cbstuff, ITestOutputHelper helper): base(cbstuff, helper)
        {
            UseThe(new Player()
            {
                experience = 14248,
                hitpoints = 23832,
                level = 141,
                loggedIn = true,
                name = "Jane",
                uuid = new Guid("9F4DF68C-1DC7-4CD2-82BA-66E187C5146A")
            });
            Given(() =>
            {
                var p = Subject.Collection.InsertAsync("playerId_Jane", The<Player>()).Result;
            });
        }
        
        [Fact]
        public async Task Then_upsert_and_get_should_work()
        {
            var playerReadResult = await Subject.Collection.GetAsync("playerId_Jane").ConfigureAwait(false);
            var playerRead = playerReadResult.ContentAs<Player>();
            playerRead.Should().BeEquivalentTo(The<Player>());
        }
    }

    public class When_using_transactions : Given_a_configured_Couchbase_instance
    {
        private ITestOutputHelper _helper;

        public When_using_transactions(CbStuff cbstuff, ITestOutputHelper helper): base(cbstuff, helper)
        {
            _helper = helper;
            UseThe(new Player[] {
                new Player()
            {
                experience = 14248,
                hitpoints = 23832,
                level = 141,
                loggedIn = true,
                name = "Jane",
                uuid = new Guid("9F4DF68C-1DC7-4CD2-82BA-66E187C5146A")
            },
                new Player()
                {
                    experience = 14,
                    hitpoints = 32,
                    level = 141,
                    loggedIn = true,
                    name = "Fred",
                    uuid = new Guid("AAAAAAAA-1DC7-4CD2-82BA-66E187C5146A")
                }
            });
        }

        [Fact]
        public async Task Then_Replacing_the_same_existing_doc_twice_should_commit()
        {
            var playerId = $"{TestName}_playerId_Jane";
            await Subject.Collection.InsertAsync(playerId, The<Player[]>()[0])
                .ConfigureAwait(false);
            
            await Subject.Transactions.RunAsync(async (ctx) =>
            {
                var txGetResult = await ctx.GetAsync(Subject.Collection, playerId).ConfigureAwait(false);
                var player = txGetResult.ContentAs<Player>();
                
                player.name = "Janey";
                await ctx.ReplaceAsync(txGetResult, player).ConfigureAwait(false);

                txGetResult = await ctx.GetAsync(Subject.Collection, playerId).ConfigureAwait(false);
                Assert.Equal("Janey", txGetResult.ContentAs<Player>().name);
                
                player.name = "Janey2";
                await ctx.ReplaceAsync(txGetResult, player).ConfigureAwait(false);
                
            });

            var getResult = await Subject.Collection.GetAsync(playerId).ConfigureAwait(false);
            Assert.Equal("Janey2", getResult.ContentAs<Player>().name);
        }
        
        [Fact]
        public async Task Then_exceptions_should_rollback_mutations()
        {
            _helper.WriteLine($"Running test --> \"{TestName}\"");

            Exception exceptionThrown = null;
            var playerId = $"{TestName}_playerId_Fred";
            
            //Store the "Fred" player in bucket:
            await Subject.Collection.InsertAsync(playerId, The<Player[]>()[1]).ConfigureAwait(false);

            try
            {
                await Subject.Transactions.RunAsync(async (ctx) =>
                {
                    // Get the stored version of "Fred"
                    var txGetResult = await ctx.GetAsync(Subject.Collection, playerId).ConfigureAwait(false);
                    var player = txGetResult.ContentAs<Player>();

                    // Change his name
                    player.name = "Freddy";
                    await ctx.ReplaceAsync(txGetResult, player).ConfigureAwait(false);

                    // Force an exception
                    throw new Exception("Arbitrary exception");
                    //await Task.Delay(15000);
                    
                });
            }
            catch (TransactionCommitAmbiguousException e)
            {
                exceptionThrown = e;
                _helper.WriteLine("Transaction possibly committed:{0}{1}", Environment.NewLine, e);
            }
            catch (TransactionFailedException e)
            {
                exceptionThrown = e;
                _helper.WriteLine("Transaction did not reach commit:{0}{1}", Environment.NewLine, e);
            }
            catch (Exception e)
            {
                exceptionThrown = e;
            }
            
            exceptionThrown.Should().NotBeNull();
            
            // Retrieve player as persisted
            var storedPlayer = await Subject.Collection.GetAsync(playerId)
                .ConfigureAwait(false);
            
            // Assert the changed name was NOT persisted.
            Assert.Equal("Fred", storedPlayer.ContentAs<Player>().name);
        }
        
        [Fact]
        public async Task Then_Insert_followed_by_Replace_should_commit()
        {
            var playerId = $"{TestName}_playerId_Jane";
            var thePlayer = The<Player[]>()[0];
            var exceptionOccurred = false;
            
            try
            {
                var txResult = await Subject.Transactions.RunAsync(async (ctx) =>
                {
                    var txGetResult = await ctx.InsertAsync(Subject.Collection, playerId, thePlayer).ConfigureAwait(false);
                    var uowPlayer = txGetResult.ContentAs<Player>();
                    uowPlayer.name = "Janey";
                    var newTxGetResult = await ctx.ReplaceAsync(txGetResult, uowPlayer).ConfigureAwait(false);

                }).ConfigureAwait(false);

                var transactionAttempt = ((List<TransactionAttempt>)txResult.Attempts)[0];
                transactionAttempt.FinalState.Should().BeEquivalentTo(AttemptStates.COMPLETED);
            }
            catch (Exception e)
            {
                _helper.WriteLine("Transaction did not reach commit:{0}{1}", Environment.NewLine, e);
                exceptionOccurred = true;
            }

            exceptionOccurred.Should().BeFalse();
            
            var getResult = await Subject.Collection.GetAsync(playerId).ConfigureAwait(false);
            var storedPlayer = getResult.ContentAs<Player>();
            storedPlayer.Should().NotBeNull();
        }

        [Fact]
        public async Task Then_Replacing_and_Removing_existing_doc_should_commit()
        {
            var playerId = $"{TestName}_playerId_Jane";
            await Subject.Collection.InsertAsync(playerId, The<Player[]>()[0])
                .ConfigureAwait(false);
            
            await Subject.Transactions.RunAsync(async (ctx) =>
            {
                var txGetResult = await ctx.GetAsync(Subject.Collection, playerId).ConfigureAwait(false);
                var player = txGetResult.ContentAs<Player>();
                
                player.name = "Janey";
                await ctx.ReplaceAsync(txGetResult, player).ConfigureAwait(false);

                txGetResult = await ctx.GetAsync(Subject.Collection, playerId).ConfigureAwait(false);
                Assert.Equal("Janey", txGetResult.ContentAs<Player>().name);

                await ctx.RemoveAsync(txGetResult).ConfigureAwait(false);
                
            });

            var exceptionThrown = false;
            try
            {
                var getResult = await Subject.Collection.GetAsync(playerId).ConfigureAwait(false);
            }
            catch (DocumentNotFoundException e)
            {
                exceptionThrown = true;
            }

            exceptionThrown.Should().BeTrue();
        }
    }
    
    
}