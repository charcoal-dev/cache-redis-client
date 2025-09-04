<?php
/**
 * Part of the "charcoal-dev/cache-redis-client" package.
 * @link https://github.com/charcoal-dev/cache-redis-client
 */

declare(strict_types=1);

namespace Charcoal\Cache\Adapters\Redis\Socket\Traits;

use Charcoal\Cache\Adapters\Redis\Exceptions\RedisConnectionException;
use Charcoal\Contracts\Storage\Cache\CacheClientInterface;

/**
 * Trait RedisSocketTrait
 * Provides common methods for managing a Redis socket connection.
 */
trait RedisSocketTrait
{
    private mixed $sock = null;
    protected ?CacheClientInterface $cacheClient = null;

    public function __construct(
        public readonly string $hostname,
        public readonly int    $port = 6379,
        public readonly int    $timeOut = 1
    )
    {
    }

    public function createLink(CacheClientInterface $cache): void
    {
        $this->cacheClient = $cache;
    }

    public function __debugInfo(): array
    {
        return [
            static::class,
            $this->hostname,
            $this->port,
        ];
    }

    public function __clone(): void
    {
        $this->sock = null;
    }

    public function __serialize(): array
    {
        return [
            "hostname" => $this->hostname,
            "port" => $this->port,
            "timeOut" => $this->timeOut
        ];
    }

    public function __unserialize(array $data): void
    {
        $this->hostname = $data["hostname"];
        $this->port = $data["port"];
        $this->timeOut = $data["timeOut"];
        $this->sock = null;
        $this->cacheClient = null;
    }

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

        $this->sock = $socket;
        stream_set_timeout($this->sock, $this->timeOut);
    }

    public function isConnected(): bool
    {
        if ($this->sock) {
            $timedOut = @stream_get_meta_data($this->sock)["timed_out"] ?? true;
            if ($timedOut) {
                $this->sock = null;
                return false;
            }
            return true;
        }
        return false;
    }

    public function getId(): string
    {
        return "redis_" . md5($this->hostname . ":" . $this->port);
    }

    public function supportsPing(): bool
    {
        return true;
    }
}
