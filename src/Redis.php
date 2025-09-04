<?php
/**
 * Part of the "charcoal-dev/cache-redis-client" package.
 * @link https://github.com/charcoal-dev/cache-redis-client
 */

declare(strict_types=1);

namespace Charcoal\Cache\Adapters\Redis;

use Charcoal\Cache\Adapters\Redis\Internal\RedisClientInterface;
use Charcoal\Cache\Adapters\Redis\Socket\RedisClient as RedisClientSocket;
use Charcoal\Cache\Adapters\Redis\Ext\RedisClient as RedisClientExt;
use Charcoal\Cache\Adapters\Redis\Socket\RedisSocketV1;
use Charcoal\Cache\Adapters\Redis\Socket\RedisSocketV2;

/**
 * Class Redis
 * Provides a factory method to create a Redis client based on the selected implementation.
 */
enum Redis
{
    case Socket;
    case Socket_V1;
    case Socket_V2;
    case Extension;

    public function create(string $hostname, int $port, int $timeout): RedisClientInterface
    {
        return match ($this) {
            self::Socket => new RedisClientSocket($hostname, $port, $timeout),
            self::Socket_V1 => new RedisSocketV1($hostname, $port, $timeout),
            self::Socket_V2 => new RedisSocketV2($hostname, $port, $timeout),
            self::Extension => new RedisClientExt($hostname, $port, $timeout),
        };
    }
}