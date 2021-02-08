using System;
using System.Threading.Tasks;
using Couchbase.KeyValue;
using Couchbase.Transactions.Error;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace Couchbase.Transactions.Examples.Game
{
    public class GameServer
    {
        private readonly Transactions _transactions;
        private readonly ICouchbaseCollection _gameSim;
        private readonly ILogger _logger;

        public GameServer(Transactions transactions, ICouchbaseCollection gameSim, ILogger logger)
        {
            _transactions = transactions;
            _gameSim = gameSim;
            _logger = logger;
        }

        public async Task PlayerHitsMonster(string actionUuid, int damage, string playerId, string monsterId)
        {
            try
            {
                await _transactions.RunAsync(async (ctx) =>
                {
                    _logger.LogInformation(
                        "Starting transaction, player {playerId} is hitting monster {monsterId} for {damage} points of damage.",
                        playerId, monsterId, damage);

                    var monster = await ctx.GetAsync(_gameSim, monsterId).ConfigureAwait(false);
                    var player = await ctx.GetAsync(_gameSim, playerId).ConfigureAwait(false);

                    var monsterContent = monster.ContentAs<JObject>();
                    var playerContent = player.ContentAs<JObject>();

                    var monsterHitPoints = monsterContent.GetValue("hitpoints").ToObject<int>();
                    var monsterNewHitPoints = monsterHitPoints - damage;

                    _logger.LogInformation(
                        "Monster {monsterId} had {monsterHitPoints} hitpoints, took {damage} damage, now has {monsterNewHitPoints} hitpoints.",
                        monsterId, monsterHitPoints, damage, monsterNewHitPoints);

                    if (monsterNewHitPoints <= 0)
                    {
                        // Monster is killed.  The remove is just for demoing, and a more realistic example would set a
                        // "dead" flag or similar.

                        await ctx.RemoveAsync(monster).ConfigureAwait(false);

                        // The player earns experience for killing the monster
                        var experienceForKillingMonster =
                            monsterContent.GetValue("experienceWhenKilled").ToObject<int>();
                        var playerExperience = playerContent.GetValue("experiance").ToObject<int>();
                        var playerNewExperience = playerExperience + experienceForKillingMonster;
                        var playerNewLevel = CalculateLevelForExperience(playerNewExperience);

                        _logger.LogInformation(
                            "Monster {monsterId} was killed.  Player {playerId} gains {experienceForKillingMonster} experience, now has level {playerNewLevel}.",
                            monsterId, playerId, experienceForKillingMonster, playerNewLevel);

                        playerContent["experience"] = playerNewExperience;
                        playerContent["level"] = playerNewLevel;

                        await ctx.ReplaceAsync(player, playerContent).ConfigureAwait(false);
                    }
                    else
                    {
                        _logger.LogInformation("Monster {monsterId} is damaged but alive.", monsterId);

                        // Monster is damaged but still alive
                        monsterContent["hitpoints"] = monsterNewHitPoints;

                        await ctx.ReplaceAsync(monster, monsterContent).ConfigureAwait(false);
                    }

                    _logger.LogInformation("About to commit transaction");

                }).ConfigureAwait(false);
            }
            catch (TransactionCommitAmbiguousException e)
            {
                _logger.LogWarning("Transaction possibly    committed:{0}{1}", Environment.NewLine, e);
            }
            catch (TransactionFailedException e)
            {
                // The operation timed out (the default timeout is 15 seconds) despite multiple attempts to commit the
                // transaction logic.   Both the monster and the player will be untouched.

                // This situation should be very rare.  It may be reasonable in this situation to ignore this particular
                // failure, as the downside is limited to the player experiencing a temporary glitch in a fast-moving MMO.

                // So, we will just log the error
                _logger.LogWarning("Transaction did not reach commit:{0}{1}", Environment.NewLine, e);
            }

            _logger.LogInformation("Transaction is complete");
        }

        private int CalculateLevelForExperience(int exp)
        {
            return exp / 100;
        }
    }
}
