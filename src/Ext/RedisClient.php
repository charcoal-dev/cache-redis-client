<?php
/**
 * Part of the "charcoal-dev/cache-redis-client" package.
 * @link https://github.com/charcoal-dev/cache-redis-client
 * @noinspection PhpComposerExtensionStubsInspection
 */

declare(strict_types=1);

namespace Charcoal\Cache\Adapters\Redis\Ext;

use Charcoal\Cache\Adapters\Redis\Internal\RedisClientInterface;
use Charcoal\Cache\Adapters\Redis\Internal\RedisClientTrait;
use Charcoal\Contracts\Storage\Cache\Adapter\AtomicCountersInterface;
use Charcoal\Contracts\Storage\Cache\Adapter\ExpirableKeysInterface;
use Charcoal\Contracts\Storage\Cache\Adapter\LocksInterface;
use Charcoal\Cache\Adapters\Redis\Exceptions\RedisConnectionException;
use Charcoal\Cache\Adapters\Redis\Exceptions\RedisOpException;

/**
 * This class provides an interface for Redis-based operations such as
 * setting and retrieving keys, atomic operations, handling expiration
 * of keys, and distributed locking.
 *
 * It implements a connection to a Redis backend using the PHP `\Redis`
 * extension and ensures error handling through exceptions. The class
 * adheres to various interfaces to guarantee extensibility and compliance
 * with caching and atomic counter standards.
 */
final class RedisClient implements
    RedisClientInterface,
    AtomicCountersInterface,
    ExpirableKeysInterface,
    LocksInterface
{
    use RedisClientTrait;

    /**
     * @throws RedisConnectionException
     */
    public function connect(): void
    {
        if (!class_exists("\Redis")) {
            throw new RedisConnectionException("ext-redis not available");
        }

        try {
            $r = new \Redis();
            if (!$r->connect($this->hostname, $this->port, $this->timeOut)) {
                throw new RedisConnectionException("Failed to connect to Redis");
            }
            $this->backend = $r;
        } catch (\RedisException $e) {
            throw new RedisConnectionException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @return void
     */
    public function disconnect(): void
    {
        if ($this->backend instanceof \Redis) {
            try {
                $this->backend->close();
            } catch (\Throwable) {
            }
        }
        $this->backend = null;
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function ping(): bool
    {
        $this->ensure();

        try {
            $pong = $this->backend->ping();
            $pong = is_string($pong) ? ltrim($pong, "+") : $pong;
            return is_string($pong) && strtolower($pong) === "pong";
        } catch (\RedisException $e) {
            throw new RedisOpException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function set(string $key, int|string $value, ?int $ttl = null): void
    {
        $this->ensure();
        try {
            $ok = $ttl && $ttl > 0
                ? $this->backend->setex($key, $ttl, (string)$value)
                : $this->backend->set($key, (string)$value);
            if ($ok !== true) {
                throw new RedisOpException("SET failed");
            }
        } catch (\RedisException $e) {
            throw new RedisOpException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function get(string $key): int|string|null|bool
    {
        $this->ensure();
        try {
            $v = $this->backend->get($key);
            return $v === false ? null : $v;
        } catch (\RedisException $e) {
            throw new RedisOpException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function has(string $key): bool
    {
        $this->ensure();
        try {
            return $this->backend->exists($key) === 1;
        } catch (\RedisException $e) {
            throw new RedisOpException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function delete(string $key): bool
    {
        $this->ensure();
        try {
            return $this->backend->del($key) === 1;
        } catch (\RedisException $e) {
            throw new RedisOpException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function truncate(): bool
    {
        $this->ensure();
        try {
            $r = $this->backend->flushAll();
            return $r === true || $r === "OK";
        } catch (\RedisException $e) {
            throw new RedisOpException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function incr(string $key): int
    {
        $this->ensure();
        try {
            return (int)$this->backend->incr($key);
        } catch (\RedisException $e) {
            throw new RedisOpException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function incrBy(string $key, int $delta): int
    {
        $this->ensure();
        try {
            return (int)$this->backend->incrBy($key, $delta);
        } catch (\RedisException $e) {
            throw new RedisOpException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function decr(string $key): int
    {
        $this->ensure();
        try {
            return (int)$this->backend->decr($key);
        } catch (\RedisException $e) {
            throw new RedisOpException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function decrBy(string $key, int $delta): int
    {
        $this->ensure();
        try {
            return (int)$this->backend->decrBy($key, $delta);
        } catch (\RedisException $e) {
            throw new RedisOpException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function getSet(string $key, int|string $value): int|string|bool|null
    {
        $this->ensure();

        try {
            $r = $this->backend->getSet($key, (string)$value);
            return $r === false ? null : $r;
        } catch (\RedisException $e) {
            throw new RedisOpException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function expire(string $key, int $ttlSec): bool
    {
        $this->ensure();

        try {
            return $this->backend->expire($key, $ttlSec);
        } catch (\RedisException $e) {
            throw new RedisOpException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function expireMs(string $key, int $ttlMs): bool
    {
        $this->ensure();
        try {
            return $this->backend->pexpire($key, $ttlMs);
        } catch (\RedisException $e) {
            throw new RedisOpException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function ttl(string $key): null|false|int
    {
        $this->ensure();
        try {
            $r = $this->backend->ttl($key);
            return $r === -2 ? null : ($r === -1 ? false : $r);
        } catch (\RedisException $e) {
            throw new RedisOpException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function ttlMs(string $key): null|false|int
    {
        $this->ensure();
        try {
            $r = $this->backend->pttl($key);
            return $r === -2 ? null : ($r === -1 ? false : $r);
        } catch (\RedisException $e) {
            throw new RedisOpException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function persist(string $key): bool
    {
        $this->ensure();
        try {
            return $this->backend->persist($key);
        } catch (\RedisException $e) {
            throw new RedisOpException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function acquire(string $key, string $token, int $ttlMs): bool
    {
        $this->ensure();

        try {
            $ok = $this->backend->set($key, $token, ["nx", "px" => $ttlMs]);
            if ($ok === true) {
                return true;
            }
            if ($ok === false) {
                return false;
            }
            throw new RedisOpException("Unexpected SET NX reply");
        } catch (\RedisException $e) {
            throw new RedisOpException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function release(string $key, string $token): bool
    {
        $this->ensure();
        $script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
        try {
            $r = $this->backend->eval($script, [$key, $token], 1);
            return (int)$r === 1;
        } catch (\RedisException $e) {
            throw new RedisOpException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function refresh(string $key, string $token, int $ttlMs): bool
    {
        $this->ensure();
        $script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end";
        try {
            $r = $this->backend->eval($script, [$key, $token, (string)$ttlMs], 1);
            return (int)$r === 1;
        } catch (\RedisException $e) {
            throw new RedisOpException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @return void
     * @throws RedisConnectionException
     */
    private function ensure(): void
    {
        if (!$this->isConnected()) {
            $this->connect();
        }
    }
}
