<?php
/**
 * Part of the "charcoal-dev/cache-redis-client" package.
 * @link https://github.com/charcoal-dev/cache-redis-client
 */

declare(strict_types=1);

namespace Charcoal\Cache\Adapters\Redis\Internal\Socket;

use Charcoal\Cache\Adapters\Redis\Exceptions\RedisConnectionException;
use Charcoal\Cache\Adapters\Redis\Internal\RedisClientTrait;

/**
 * Trait RedisSocketTrait
 * Provides common methods for managing a Redis socket connection.
 * @internal
 */
trait RedisSocketTrait
{
    use RedisClientTrait;

    /**
     * @throws RedisConnectionException
     */
    public function connect(): void
    {
        $errorNum = 0;
        $errorMsg = "";
        $socket = @stream_socket_client(
            "tcp://" . $this->hostname . ":" . $this->port,
            $errorNum,
            $errorMsg,
            $this->timeOut
        );

        if (!is_resource($socket)) {
            throw new RedisConnectionException($errorMsg, $errorNum);
        }

        $this->backend = $socket;
        stream_set_timeout($this->backend, $this->timeOut);
    }
}
