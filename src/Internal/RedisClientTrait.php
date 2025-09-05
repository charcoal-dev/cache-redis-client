<?php
/**
 * Part of the "charcoal-dev/cache-redis-client" package.
 * @link https://github.com/charcoal-dev/cache-redis-client
 */

declare(strict_types=1);

namespace Charcoal\Cache\Adapters\Redis\Internal;

use Charcoal\Contracts\Storage\Cache\CacheClientInterface;

/**
 * Trait RedisClientTrait
 * Provides common methods for managing a Redis client connection.
 * @internal
 */
trait RedisClientTrait
{
    private mixed $backend = null;
    private ?CacheClientInterface $cacheClient = null;

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
        $this->cacheClient = $cache;
    }

    /**
     * @return array
     */
    public function __debugInfo(): array
    {
        return [
            static::class,
            $this->hostname,
            $this->port,
        ];
    }

    /**
     * @return void
     */
    public function __clone(): void
    {
        $this->backend = null;
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
        $this->backend = null;
        $this->cacheClient = null;
    }

    /**
     * @return bool
     */
    public function isConnected(): bool
    {
        $b = $this->backend;

        if (is_resource($b)) {
            $meta = @stream_get_meta_data($b);
            return !($meta["timed_out"] ?? true);
        }

        if (is_object($b) && method_exists($b, "isConnected")) {
            try {
                return (bool)$b->isConnected();
            } catch (\Throwable) {
                return false;
            }
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

    /**
     * Ensure that connection goes through the cache client, for events to work.
     * @return void
     */
    protected function ensure(): void
    {
        if (!$this->isConnected()) {
            $this->cacheClient->connect();
        }
    }
}