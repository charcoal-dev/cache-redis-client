<?php
/*
 * This file is a part of "charcoal-dev/cache-redis-client" package.
 * https://github.com/charcoal-dev/cache-redis-client
 *
 * Copyright (c) Furqan A. Siddiqui <hello@furqansiddiqui.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code or visit following link:
 * https://github.com/charcoal-dev/cache-redis-client/blob/master/LICENSE
 */

declare(strict_types=1);

namespace Charcoal\Cache\Drivers;

use Charcoal\Cache\Cache;
use Charcoal\Cache\CacheDriverInterface;
use Charcoal\Cache\Drivers\Exception\RedisConnectionException;
use Charcoal\Cache\Drivers\Exception\RedisOpException;
use Charcoal\Cache\Exception\CacheDriverException;

/**
 * Class RedisClient
 * @package Charcoal\Cache\Drivers
 */
class RedisClient implements CacheDriverInterface
{
    /** @var \Charcoal\Cache\Cache|null */
    private ?Cache $cache = null;
    /** @var null|resource */
    private $sock = null;

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
     * @param \Charcoal\Cache\Cache $cache
     * @return void
     */
    public function createLink(Cache $cache): void
    {
        $this->cache = $cache;
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
    public function __clone(): void
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
     */
    public function __unserialize(array $data): void
    {
        $this->hostname = $data["hostname"];
        $this->port = $data["port"];
        $this->timeOut = $data["timeOut"];
        $this->sock = null;
        $this->cache = null;
    }

    /**
     * @return void
     * @throws \Charcoal\Cache\Drivers\Exception\RedisConnectionException
     * @throws \Throwable
     */
    public function connect(): void
    {
        // Establish connection
        $errorNum = 0;
        $errorMsg = "";
        $socket = stream_socket_client(
            sprintf('%s:%d', $this->hostname, $this->port),
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

        // Event trigger
        $this->cache->events->onConnected()->trigger([$this]);
    }

    /**
     * @return void
     * @throws \Throwable
     */
    public function disconnect(): void
    {
        if ($this->isConnected()) {
            try {
                $this->send("QUIT");
            } catch (CacheDriverException) {
            }
        }

        $this->sock = null;

        // Event trigger
        $this->cache->events->onDisconnect()->trigger([$this]);
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
    public function metaUniqueId(): string
    {
        return "redis_" . md5($this->hostname . ":" . $this->port);
    }

    /**
     * @return bool
     */
    public function metaPingSupported(): bool
    {
        return true;
    }

    /**
     * @return bool
     * @throws \Charcoal\Cache\Drivers\Exception\RedisConnectionException
     * @throws \Charcoal\Cache\Drivers\Exception\RedisOpException
     * @throws \Throwable
     */
    public function ping(): bool
    {
        // Check if connected
        if (!$this->isConnected()) {
            throw new RedisConnectionException('Lost connection with server');
        }

        $ping = $this->send("PING");
        if (!is_string($ping) || strtolower($ping) !== "pong") {
            throw new RedisOpException('Did not receive PONG back');
        }

        return true;
    }

    /**
     * @param string $key
     * @param int|string $value
     * @param int|null $ttl
     * @return void
     * @throws \Charcoal\Cache\Drivers\Exception\RedisConnectionException
     * @throws \Charcoal\Cache\Drivers\Exception\RedisOpException
     * @throws \Throwable
     */
    public function store(string $key, int|string $value, ?int $ttl = null): void
    {
        $query = is_int($ttl) && $ttl > 0 ?
            sprintf('SETEX %s %d "%s"', $key, $ttl, $value) :
            sprintf('SET %s "%s"', $key, $value);

        $exec = $this->send($query);
        if ($exec !== "OK") {
            throw new RedisOpException('Failed to store data on REDIS server');
        }
    }

    /**
     * @param string $key
     * @return int|string|bool|null
     * @throws \Charcoal\Cache\Drivers\Exception\RedisConnectionException
     * @throws \Charcoal\Cache\Drivers\Exception\RedisOpException
     * @throws \Throwable
     */
    public function resolve(string $key): int|string|null|bool
    {
        return $this->send(sprintf('GET %s', $key));
    }

    /**
     * @param string $key
     * @return bool
     * @throws \Charcoal\Cache\Drivers\Exception\RedisConnectionException
     * @throws \Charcoal\Cache\Drivers\Exception\RedisOpException
     * @throws \Throwable
     */
    public function isStored(string $key): bool
    {
        return $this->send(sprintf('EXISTS %s', $key)) === 1;
    }

    /**
     * @param string $key
     * @return bool
     * @throws \Charcoal\Cache\Drivers\Exception\RedisConnectionException
     * @throws \Charcoal\Cache\Drivers\Exception\RedisOpException
     * @throws \Throwable
     */
    public function delete(string $key): bool
    {
        return $this->send(sprintf('DEL %s', $key)) === 1;
    }

    /**
     * @return bool
     * @throws \Charcoal\Cache\Drivers\Exception\RedisConnectionException
     * @throws \Charcoal\Cache\Drivers\Exception\RedisOpException
     * @throws \Throwable
     */
    public function truncate(): bool
    {
        return (bool)$this->send('FLUSHALL');
    }

    /**
     * @param string $command
     * @return string
     */
    private function prepareCommand(string $command): string
    {
        $parts = str_getcsv($command, " ", '"');
        $prepared = "*" . count($parts) . "\r\n";
        foreach ($parts as $part) {
            $prepared .= "$" . strlen($part) . "\r\n" . $part . "\r\n";
        }

        return $prepared;
    }

    /**
     * @param string $command
     * @return int|string|bool|null
     * @throws \Charcoal\Cache\Drivers\Exception\RedisConnectionException
     * @throws \Charcoal\Cache\Drivers\Exception\RedisOpException
     * @throws \Throwable
     */
    private function send(string $command): int|string|null|bool
    {
        if (!$this->sock) {
            $this->connect();
        }

        $command = trim($command);
        if (strtolower($command) == "disconnect") {
            return @fclose($this->sock);
        }

        $write = fwrite($this->sock, $this->prepareCommand($command));
        if ($write === false) {
            throw new RedisOpException(sprintf('Failed to send "%1$s" command', explode(" ", $command)[0]));
        }

        return $this->response();
    }

    /**
     * @return int|string|null
     * @throws RedisOpException
     */
    private function response(): int|string|null
    {
        // Get response from stream
        $response = fgets($this->sock);
        if (!is_string($response)) {
            $timedOut = @stream_get_meta_data($this->sock)["timed_out"] ?? null;
            if ($timedOut === true) {
                throw new RedisOpException('Redis stream has timed out');
            }

            throw new RedisOpException('No response received from server');
        }

        // Prepare response for parsing
        $response = trim($response);
        $responseType = substr($response, 0, 1);
        $data = substr($response, 1);

        // Check response
        switch ($responseType) {
            case "-": // Error
                throw new RedisOpException(substr($data, 4));
            case "+": // Simple String
                return $data;
            case ":": // Integer
                return intval($data);
            case "$": // Bulk String
                $bytes = intval($data);
                if ($bytes > 0) {
                    $data = stream_get_contents($this->sock, $bytes + 2);
                    if (!is_string($data)) {
                        throw new RedisOpException('Failed to read REDIS bulk-string response');
                    }

                    return trim($data); // Return trimmed
                } elseif ($bytes === 0) {
                    return ""; // Empty String
                } elseif ($bytes === -1) {
                    return null; // NULL
                } else {
                    throw new RedisOpException('Invalid number of REDIS response bytes');
                }
        }

        throw new RedisOpException('Unexpected response from REDIS server');
    }
}