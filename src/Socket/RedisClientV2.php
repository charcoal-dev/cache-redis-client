<?php
/**
 * Part of the "charcoal-dev/cache-redis-client" package.
 * @link https://github.com/charcoal-dev/cache-redis-client
 */

declare(strict_types=1);

namespace Charcoal\Cache\Adapters\Redis\Socket;

use Charcoal\Cache\Adapters\Redis\Exceptions\RedisConnectionException;
use Charcoal\Cache\Adapters\Redis\Exceptions\RedisOpException;
use Charcoal\Cache\Adapters\Redis\Internal\Socket\AtomicCountersTrait;
use Charcoal\Cache\Adapters\Redis\Internal\Socket\ExpirableKeysTrait;
use Charcoal\Cache\Adapters\Redis\Internal\Socket\LocksTrait;
use Charcoal\Cache\Adapters\Redis\Internal\Socket\RedisSocketTrait;
use Charcoal\Contracts\Storage\Cache\Adapter\AtomicCountersInterface;
use Charcoal\Contracts\Storage\Cache\Adapter\ExpirableKeysInterface;
use Charcoal\Contracts\Storage\Cache\Adapter\LocksInterface;
use Charcoal\Contracts\Storage\Cache\CacheAdapterInterface;

/**
 * Class RedisClientV2
 * A client for interacting with a Redis server, providing methods to connect,
 * execute commands, and manage data within the Redis store.
 */
final class RedisClientV2 implements
    CacheAdapterInterface,
    AtomicCountersInterface,
    ExpirableKeysInterface,
    LocksInterface
{
    use RedisSocketTrait;
    use AtomicCountersTrait;
    use ExpirableKeysTrait;
    use LocksTrait;

    /**
     * @throws RedisConnectionException
     */
    public function connect(): void
    {
        $errorNum = 0;
        $errorMsg = "";
        $socket = @stream_socket_client(
            sprintf("tcp://%s:%d", $this->hostname, $this->port),
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

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function ping(): bool
    {
        if (!$this->isConnected()) {
            throw new RedisConnectionException("Lost connection with server");
        }

        $pong = $this->sendArgs("PING");
        if (!is_string($pong) || strtolower($pong) !== "pong") {
            throw new RedisOpException("Did not receive PONG back");
        }
        return true;
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function set(string $key, int|string $value, ?int $ttl = null): void
    {
        if (is_int($ttl) && $ttl > 0) {
            $ok = $this->sendArgs("SETEX", $key, (string)$ttl, (string)$value);
        } else {
            $ok = $this->sendArgs("SET", $key, (string)$value);
        }
        if ($ok !== "OK") {
            throw new RedisOpException("Failed to store data on REDIS server");
        }
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function get(string $key): int|string|null|bool
    {
        return $this->sendArgs("GET", $key);
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function has(string $key): bool
    {
        return $this->sendArgs("EXISTS", $key) === 1;
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function delete(string $key): bool
    {
        return $this->sendArgs("DEL", $key) === 1;
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function truncate(): bool
    {
        return $this->sendArgs("FLUSHALL") === "OK";
    }

    /**
     * @return void
     */
    public function disconnect(): void
    {
        if ($this->isConnected()) {
            try {
                $this->sendArgs("QUIT");
            } catch (\Throwable) {
            }
        }
        $this->backend = null;
    }

    /**
     * @param array<int, string|int> $args
     * @throws RedisConnectionException
     */
    private function writeCommand(array $args): void
    {
        $buf = "*" . count($args) . "\r\n";
        foreach ($args as $arg) {
            $s = (string)$arg;
            $buf .= "$" . strlen($s) . "\r\n" . $s . "\r\n";
        }

        $len = strlen($buf);
        $written = 0;
        while ($written < $len) {
            $n = @fwrite($this->backend, substr($buf, $written));
            if ($n === false || $n === 0) {
                throw new RedisConnectionException("Failed to write to Redis socket");
            }
            $written += $n;
        }
    }

    /**
     * @throws RedisOpException
     */
    private function readLine(): string
    {
        $line = fgets($this->backend);
        if ($line === false) {
            $meta = @stream_get_meta_data($this->backend);
            if (($meta["timed_out"] ?? false) === true) {
                throw new RedisOpException("Redis stream has timed out");
            }
            throw new RedisOpException("No response received from server");
        }
        return rtrim($line, "\r\n");
    }

    /**
     * @throws RedisOpException
     */
    private function readExact(int $n): string
    {
        $buf = "";
        while ($n > 0) {
            $chunk = @stream_get_contents($this->backend, $n);
            if ($chunk === false || $chunk === "") {
                throw new RedisOpException("Failed to read bulk response bytes");
            }
            $buf .= $chunk;
            $n -= strlen($chunk);
        }
        return $buf;
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    private function sendArgs(string ...$args): int|string|null|bool
    {
        if (!$this->backend) {
            $this->connect();
        }

        $this->writeCommand($args);

        $header = $this->readLine();
        $t = $header[0] ?? "";
        $rest = substr($header, 1);

        switch ($t) {
            case "+":
                return $rest;

            case "-":
                throw new RedisOpException($rest);

            case ":":
                return (int)$rest;

            case "$":
            {
                $len = (int)$rest;
                if ($len === -1) {
                    return null;
                }
                if ($len < 0) {
                    throw new RedisOpException("Invalid bulk length");
                }
                $data = $this->readExact($len);
                $this->readExact(2);
                return $data;
            }
        }

        throw new RedisOpException("Unexpected response: " . $header);
    }
}