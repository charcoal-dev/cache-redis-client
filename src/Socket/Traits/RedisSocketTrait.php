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
 */
trait RedisSocketTrait
{
    private mixed $sock = null;

    /**
     * @param string $hostname
     * @param int $port
     * @param int $timeOut
     */
    public function __construct(
        public readonly string $hostname,
        public readonly int    $port = 6379,
        public readonly int    $timeOut = 1
    )
    {
    }

    /**
     * @param CacheClientInterface $cache
     * @return void
     */
    public function createLink(CacheClientInterface $cache): void
    {
    }

    /**
     * @return array
     */
    public function __debugInfo(): array
    {
        return [
            get_called_class(),
            $this->hostname,
            $this->port,
        ];
    }

    /**
     * @return void
     */
    public function __clone()
    {
        $this->sock = null;
    }

    /**
     * @return array
     */
    public function __serialize(): array
    {
        return [
            "hostname" => $this->hostname,
            "port" => $this->port,
            "timeOut" => $this->timeOut
        ];
    }

    /**
     * @param array $data
     * @return void
     */
    public function __unserialize(array $data): void
    {
        $this->hostname = $data["hostname"];
        $this->port = $data["port"];
        $this->timeOut = $data["timeOut"];
        $this->sock = null;
    }

    /**
     * @return void
     * @throws RedisConnectionException
     */
    public function connect(): void
    {
        // Establish connection
        $errorNum = 0;
        $errorMsg = "";
        $socket = stream_socket_client(
            "tcp://" . $this->hostname . ":" . $this->port,
            $errorNum,
            $errorMsg,
            $this->timeOut
        );

        // Connected?
        if (!is_resource($socket)) {
            throw new RedisConnectionException($errorMsg, $errorNum);
        }

        $this->sock = $socket;
        stream_set_timeout($this->sock, $this->timeOut);
    }

    /**
     * @return void
     */
    public function disconnect(): void
    {
        if ($this->isConnected()) {
            try {
                $this->send("QUIT");
            } catch (\Exception) {
            }
        }

        $this->sock = null;
    }

    /**
     * @return bool
     */
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

    /**
     * @return string
     */
    public function getId(): string
    {
        return "redis_" . md5($this->hostname . ":" . $this->port);
    }

    /**
     * @return bool
     */
    public function supportsPing(): bool
    {
        return true;
    }
}