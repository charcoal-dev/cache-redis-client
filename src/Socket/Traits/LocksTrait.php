<?php
/**
 * Part of the "charcoal-dev/cache-redis-client" package.
 * @link https://github.com/charcoal-dev/cache-redis-client
 */

declare(strict_types=1);

namespace Charcoal\Cache\Adapters\Redis\Socket\Traits;

use Charcoal\Cache\Adapters\Redis\Exceptions\RedisConnectionException;
use Charcoal\Cache\Adapters\Redis\Exceptions\RedisOpException;

trait LocksTrait
{
    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function acquire(string $key, string $token, int $ttlMs): bool
    {
        $r = $this->sendArgs("SET", $key, $token, "NX", "PX", (string)$ttlMs);
        if ($r === "OK") {
            return true;
        }
        if ($r === null) {
            return false;
        }
        throw new RedisOpException("Unexpected SET NX reply");
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function release(string $key, string $token): bool
    {
        $script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
        $r = $this->sendArgs("EVAL", $script, "1", $key, $token);
        if (!is_int($r)) {
            throw new RedisOpException("Unexpected EVAL reply (release)");
        }
        return $r === 1;
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function refresh(string $key, string $token, int $ttlMs): bool
    {
        $script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end";
        $r = $this->sendArgs("EVAL", $script, "1", $key, $token, (string)$ttlMs);
        if (!is_int($r)) {
            throw new RedisOpException("Unexpected EVAL reply (refresh)");
        }
        return $r === 1;
    }
}