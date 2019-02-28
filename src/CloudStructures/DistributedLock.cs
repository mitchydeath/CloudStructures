using System;
using System.Collections.Generic;
using System.Text;
using StackExchange.Redis;

namespace CloudStructures
{
    /// <summary>
    /// Thrown when conflicts with lock acquisition
    /// </summary>
    public class DistributedLockAlreadyExistsException : Exception
    {
        public DistributedLockAlreadyExistsException(string key)
            : base("LockKey:" + key)
        { }
    }

    /// <summary>
    /// Perform Distributed Lock using LockTake, LockRelease
    /// </summary>
    public class DistributedLock : IDisposable
    {
        /// <summary>
        /// True if the lock was successfully taken, false otherwise.
        /// </summary>
        public bool IsAcquiredLock { get; private set; }

        /// <summary>
        /// The key of the lock
        /// </summary>
        private readonly string key;
        /// <summary>
        /// The expiration of the lock key
        /// </summary>
        private readonly int expirySeconds;
        /// <summary>
        /// The value to set at the key
        /// </summary>
        private readonly string tokenValue;
        /// <summary>
        /// The redis connection for lock
        /// </summary>
        private readonly RedisConnection connection;

        private bool disposed;

        private DistributedLock(RedisConnection connection, string key, int expirySeconds)
        {
            this.key = key;
            this.expirySeconds = expirySeconds;
            this.tokenValue = DateTime.Now.Ticks.ToString();

            this.IsAcquiredLock = ((IDatabase)connection.Database).LockTake(key, tokenValue, TimeSpan.FromSeconds(expirySeconds));
        }

        /// <summary>
        ///  Try to acquire the lock
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="key"></param>
        /// <param name="expirySeconds"></param>
        /// <returns></returns>
        public static DistributedLock Acquire(RedisConnection connection, string key, int expirySeconds)
        {
            return new DistributedLock(connection, key, expirySeconds);
        }

        /// <summary>
        /// Throws <see cref="DistributedLockAlreadyExistsException"> if Lock is not released
        /// </summary>
        public void ThrowIfLockAlreadyExists()
        {
            if (!IsAcquiredLock)
            {
                throw new DistributedLockAlreadyExistsException(key);
            }
        }

        /// <summary>
        /// Try to acquire the lock and execute <see cref="ThrowIfLockAlreadyExists"/>
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="key"></param>
        /// <param name="expirySeconds"></param>
        /// <returns></returns>
        public static DistributedLock AcquireAndCheck(RedisConnection connection, string key, int expirySeconds)
        {
            var @lock = new DistributedLock(connection, key, expirySeconds);
            try
            {
                @lock.ThrowIfLockAlreadyExists();
            }
            catch
            {
                @lock.Dispose();
                throw;
            }
            return @lock;
        }

        public void Dispose()
        {
            if (!disposed && IsAcquiredLock)
            {
                disposed = true;
                // synchronous wait
                ((IDatabase)connection.Database).LockRelease(key, tokenValue);
            }
        }
    }
}
