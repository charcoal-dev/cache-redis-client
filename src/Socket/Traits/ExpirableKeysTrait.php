<?php
/**
 * Part of the "charcoal-dev/cache-redis-client" package.
 * @link https://github.com/charcoal-dev/cache-redis-client
 */

declare(strict_types=1);

namespace Charcoal\Cache\Adapters\Redis\Socket\Traits;

use Charcoal\Cache\Adapters\Redis\Exceptions\RedisConnectionException;
use Charcoal\Cache\Adapters\Redis\Exceptions\RedisOpException;

/**
 * Trait ExpirableKeysTrait
 * Provides methods for managing key expiration.
 */
trait ExpirableKeysTrait
{
    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function expire(string $key, int $ttlSec): bool
    {
        $r = $this->sendArgs("EXPIRE", $key, (string)$ttlSec);
        if (!is_int($r)) {
            throw new RedisOpException("Unexpected EXPIRE reply");
        }
        return $r === 1;
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function expireMs(string $key, int $ttlMs): bool
    {
        $r = $this->sendArgs("PEXPIRE", $key, (string)$ttlMs);
        if (!is_int($r)) {
            throw new RedisOpException("Unexpected PEXPIRE reply");
        }
        return $r === 1;
    }

    /**
     * @return null|false|int  null=no key, false=no expire, int=seconds
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function ttl(string $key): null|false|int
    {
        $r = $this->sendArgs("TTL", $key);
        if (!is_int($r)) {
            throw new RedisOpException("Unexpected TTL reply");
        }
        return $r === -2 ? null : ($r === -1 ? false : $r);
    }

    /**
     * @return null|false|int  null=no key, false=no expire, int=milliseconds
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function ttlMs(string $key): null|false|int
    {
        $r = $this->sendArgs("PTTL", $key);
        if (!is_int($r)) {
            throw new RedisOpException("Unexpected PTTL reply");
        }
        return $r === -2 ? null : ($r === -1 ? false : $r);
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function persist(string $key): bool
    {
        $r = $this->sendArgs("PERSIST", $key);
        if (!is_int($r)) {
            throw new RedisOpException("Unexpected PERSIST reply");
        }
        return $r === 1;
    }
}