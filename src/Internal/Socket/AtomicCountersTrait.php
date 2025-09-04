<?php
/**
 * Part of the "charcoal-dev/cache-redis-client" package.
 * @link https://github.com/charcoal-dev/cache-redis-client
 */

declare(strict_types=1);

namespace Charcoal\Cache\Adapters\Redis\Internal\Socket;

use Charcoal\Cache\Adapters\Redis\Exceptions\RedisConnectionException;
use Charcoal\Cache\Adapters\Redis\Exceptions\RedisOpException;

/**
 * Trait AtomicCountersTrait
 * Provides methods for atomic counter operations.
 * @internal
 */
trait AtomicCountersTrait
{
    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function incr(string $key): int
    {
        $r = $this->sendArgs("INCR", $key);
        if (!is_int($r)) {
            throw new RedisOpException("Unexpected INCR reply");
        }
        return $r;
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function incrBy(string $key, int $delta): int
    {
        $r = $this->sendArgs("INCRBY", $key, (string)$delta);
        if (!is_int($r)) {
            throw new RedisOpException("Unexpected INCRBY reply");
        }
        return $r;
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function decr(string $key): int
    {
        $r = $this->sendArgs("DECR", $key);
        if (!is_int($r)) {
            throw new RedisOpException("Unexpected DECR reply");
        }
        return $r;
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function decrBy(string $key, int $delta): int
    {
        $r = $this->sendArgs("DECRBY", $key, (string)$delta);
        if (!is_int($r)) {
            throw new RedisOpException("Unexpected DECRBY reply");
        }
        return $r;
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function getSet(string $key, int|string $value): int|string|bool|null
    {
        return $this->sendArgs("GETSET", $key, (string)$value);
    }
}