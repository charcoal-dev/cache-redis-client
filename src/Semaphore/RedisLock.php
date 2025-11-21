<?php
/**
 * Part of the "charcoal-dev/cache-redis-client" package.
 * @link https://github.com/charcoal-dev/cache-redis-client
 */

declare(strict_types=1);

namespace Charcoal\Cache\Adapters\Redis\Semaphore;

use Charcoal\Base\Objects\Traits\NoDumpTrait;
use Charcoal\Base\Objects\Traits\NotCloneableTrait;
use Charcoal\Base\Objects\Traits\NotSerializableTrait;
use Charcoal\Semaphore\Contracts\SemaphoreLockInterface;
use Charcoal\Semaphore\Enums\SemaphoreLockError;
use Charcoal\Semaphore\Exceptions\SemaphoreLockException;
use Charcoal\Semaphore\Exceptions\SemaphoreUnlockException;
use Random\RandomException;

/**
 * RedisLock provides a distributed locking mechanism using Redis.
 * It acts as a semaphore lock ensuring exclusive access to resources
 * in a concurrent system environment.
 */
final class RedisLock implements SemaphoreLockInterface
{
    use NotSerializableTrait;
    use NotCloneableTrait;
    use NoDumpTrait;

    public readonly string $lockKey;
    public readonly string $prevKey;
    private bool $prevLoaded = false;

    protected ?float $previousTimestamp = null;
    protected ?float $acquiredAt = null;
    protected bool $isLocked = false;
    protected bool $autoReleaseSet = false;
    private string $token;

    /**
     * @throws SemaphoreLockException
     */
    public function __construct(
        private readonly SemaphoreRedis $redis,
        public readonly string          $lockId,
        public readonly ?float          $concurrentCheckEvery = null,
        public readonly int             $concurrentTimeout = 0,
        public readonly int             $ttlMs = 30000,
        public readonly ?string         $namespace = null
    )
    {
        if (!preg_match("/^\w+$/", $lockId)) {
            throw new \InvalidArgumentException("Invalid resource identifier for semaphore lock");
        }

        if (!preg_match('/^\w+$/', $this->namespace)) {
            throw new \InvalidArgumentException("Invalid namespace for semaphore lock");
        }

        if ($this->ttlMs <= 0) {
            throw new \InvalidArgumentException("Lock TTL must be > 0 ms");
        }

        $namespace = $this->namespace ? $this->namespace . ":" : "";
        $this->lockKey = "sem:" . $namespace . $this->lockId;
        $this->prevKey = $this->lockKey . ":prev";

        try {
            $this->token = bin2hex(random_bytes(16));
        } catch (RandomException $e) {
            throw new \RuntimeException("Failed to generate random token", previous: $e);
        }

        $sleepUs = ($this->concurrentCheckEvery && $this->concurrentCheckEvery > 0)
            ? (int)($this->concurrentCheckEvery * 1_000_000)
            : null;

        $start = microtime(true);

        while (true) {
            try {
                if ($this->redis->redisClient->acquire($this->lockKey, $this->token, $this->ttlMs)) {
                    $this->isLocked = true;
                    $this->acquiredAt = microtime(true);
                    break;
                }
            } catch (\Throwable $e) {
                throw new SemaphoreLockException(
                    SemaphoreLockError::LOCK_OBTAIN_ERROR,
                    "Redis lock acquire failed",
                    previous: $e
                );
            }

            if (!$sleepUs) {
                throw new SemaphoreLockException(SemaphoreLockError::CONCURRENT_REQUEST_BLOCKED);
            }

            usleep($sleepUs);
            if ($this->concurrentTimeout > 0 && (microtime(true) - $start) >= $this->concurrentTimeout) {
                throw new SemaphoreLockException(SemaphoreLockError::CONCURRENT_REQUEST_TIMEOUT);
            }
        }
    }

    /**
     * @throws SemaphoreUnlockException
     */
    public function releaseLock(): void
    {
        if (!$this->isLocked) {
            return;
        }

        $this->isLocked = false;

        try {
            // release only if token matches
            if (!$this->redis->redisClient->release($this->lockKey, $this->token)) {
                throw new SemaphoreUnlockException("Redis release failed (token mismatch or key missing)");
            }

            // persist last-held timestamp for next acquirer
            if ($this->acquiredAt !== null) {
                $this->redis->redisClient->set($this->prevKey, (string)$this->acquiredAt, null);
            }
        } catch (\Throwable $e) {
            throw new SemaphoreUnlockException("Redis unlock error", 0, $e);
        }
    }

    /**
     * @return bool
     */
    public function isLocked(): bool
    {
        return $this->isLocked;
    }

    /**
     * @return void
     */
    public function setAutoRelease(): void
    {
        if ($this->autoReleaseSet) {
            return;
        }

        $that = $this;

        register_shutdown_function(static function () use ($that): void {
            try {
                $that->releaseLock();
            } catch (\Throwable) {
            }
        });

        $this->autoReleaseSet = true;
    }

    /**
     * @return void
     */
    private function loadPreviousOnce(): void
    {
        if ($this->prevLoaded) {
            return;
        }
        try {
            $prev = $this->redis->redisClient->get($this->prevKey);
            if (is_string($prev) || is_int($prev)) {
                $this->previousTimestamp = (float)$prev;
            }
        } catch (\Throwable) {
            // ignore read errors; treat as unavailable
        } finally {
            $this->prevLoaded = true;
        }
    }

    /**
     * @return float|null
     */
    public function previousTimestamp(): ?float
    {
        $this->loadPreviousOnce();
        return $this->previousTimestamp;
    }

    /**
     * @param float $seconds
     * @return bool
     */
    public function checkElapsedTime(float $seconds): bool
    {
        $this->loadPreviousOnce();
        if ($this->previousTimestamp === null) {
            return true;
        }
        return (microtime(true) - $this->previousTimestamp) >= $seconds;
    }

    /**
     * @return string
     */
    public function lockId(): string
    {
        return $this->lockId;
    }

    /**
     * @return string|null
     */
    public function namespace(): ?string
    {
        return $this->namespace;
    }
}