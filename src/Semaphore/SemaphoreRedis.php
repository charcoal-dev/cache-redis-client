<?php
/**
 * Part of the "charcoal-dev/cache-redis-client" package.
 * @link https://github.com/charcoal-dev/cache-redis-client
 */

declare(strict_types=1);

namespace Charcoal\Cache\Adapters\Redis\Semaphore;

use Charcoal\Cache\Adapters\Redis\Internal\RedisClientInterface;
use Charcoal\Contracts\Storage\Cache\Adapter\LocksInterface;
use Charcoal\Semaphore\Contracts\SemaphoreProviderInterface;
use Charcoal\Semaphore\Exceptions\SemaphoreLockException;

/**
 * Class SemaphoreRedis
 * Provides a Redis-based implementation of the SemaphoreProviderInterface.
 */
final readonly class SemaphoreRedis implements SemaphoreProviderInterface
{
    public function __construct(public RedisClientInterface&LocksInterface $redisClient)
    {
    }

    /**
     * Acquires a semaphore lock for the given lock identifier with optional parameters
     * to control concurrent checks and timeout duration.
     * @throws SemaphoreLockException
     */
    public function obtainLock(
        string  $lockId,
        ?float  $concurrentCheckEvery = null,
        int     $concurrentTimeout = 0,
        ?string $namespace = null
    ): RedisLock
    {
        return new RedisLock($this, $lockId, $concurrentCheckEvery, $concurrentTimeout, namespace: $namespace);
    }
}