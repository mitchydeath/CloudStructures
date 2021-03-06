﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CloudStructures.Internals;
using StackExchange.Redis;



namespace CloudStructures.Structures
{
    /// <summary>
    /// Provides HyperLogLog related commands.
    /// </summary>
    /// <typeparam name="T">Data type</typeparam>
    public readonly struct RedisHyperLogLog<T> : IRedisStructure
    {
        #region IRedisStructure implementations
        /// <summary>
        /// Gets connection.
        /// </summary>
        public RedisConnection Connection { get; }


        /// <summary>
        /// Gets key.
        /// </summary>
        public RedisKey Key { get; }


        /// <summary>
        /// Gets default expiration time.
        /// </summary>
        public TimeSpan? DefaultExpiry { get; }
        #endregion


        #region Constructors
        /// <summary>
        /// Creates instance.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="key"></param>
        /// <param name="defaultExpiry"></param>
        public RedisHyperLogLog(RedisConnection connection, RedisKey key, TimeSpan? defaultExpiry)
        {
            this.Connection = connection ?? throw new ArgumentNullException(nameof(connection));
            this.Key = key;
            this.DefaultExpiry = defaultExpiry;
        }
        #endregion


        #region Commands
        //- [x] HyperLogLogAddAsync
        //- [x] HyperLogLogLengthAsync
        //- [x] HyperLogLogMergeAsync


        /// <summary>
        /// PFADD : http://redis.io/commands/pfadd
        /// </summary>
        public Task<bool> Add(T value, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry = expiry ?? this.DefaultExpiry;
            var serialized = this.Connection.Converter.Serialize(value);
            return this.ExecuteWithExpiry
            (
                (db, a) => db.HyperLogLogAddAsync(a.key, a.serialized, a.flags),
                (key: this.Key, serialized, flags),
                expiry,
                flags
            );
        }


        /// <summary>
        /// PFADD : http://redis.io/commands/pfadd
        /// </summary>
        public Task<bool> Add(IEnumerable<T> values, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            expiry = expiry ?? this.DefaultExpiry;
            var serialized = values.Select(this.Connection.Converter.Serialize).ToArray();
            return this.ExecuteWithExpiry
            (
                (db, a) => db.HyperLogLogAddAsync(a.key, a.serialized, a.flags),
                (key: this.Key, serialized, flags),
                expiry,
                flags
            );
        }


        /// <summary>
        /// PFCOUNT : http://redis.io/commands/pfcount
        /// </summary>
        public Task<long> Length(CommandFlags flags = CommandFlags.None)
            => this.Connection.Database.HyperLogLogLengthAsync(this.Key, flags);


        /// <summary>
        /// PFMERGE : https://redis.io/commands/pfmerge
        /// </summary>
        public Task Merge(RedisKey first, RedisKey second, CommandFlags flags = CommandFlags.None)
            => this.Connection.Database.HyperLogLogMergeAsync(this.Key, first, second, flags);


        /// <summary>
        /// PFMERGE : https://redis.io/commands/pfmerge
        /// </summary>
        public Task Merge(RedisKey[] sourceKeys, CommandFlags flags = CommandFlags.None)
            => this.Connection.Database.HyperLogLogMergeAsync(this.Key, sourceKeys, flags);
        #endregion
    }
}
