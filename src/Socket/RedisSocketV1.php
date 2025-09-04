<?php
/**
 * Part of the "charcoal-dev/cache-redis-client" package.
 * @link https://github.com/charcoal-dev/cache-redis-client
 */

declare(strict_types=1);

namespace Charcoal\Cache\Adapters\Redis\Socket;

use Charcoal\Cache\Adapters\Redis\Exceptions\RedisConnectionException;
use Charcoal\Cache\Adapters\Redis\Exceptions\RedisOpException;
use Charcoal\Cache\Adapters\Redis\Internal\Socket\RedisSocketTrait;
use Charcoal\Contracts\Storage\Cache\CacheAdapterInterface;

/**
 * Class RedisClientV1
 * A client for interacting with a Redis server, providing methods to connect,
 * execute commands, and manage data within the Redis store.
 */
final class RedisSocketV1 implements CacheAdapterInterface
{
    use RedisSocketTrait;

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
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
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function set(string $key, int|string $value, ?int $ttl = null): void
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
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function get(string $key): int|string|null|bool
    {
        return $this->send(sprintf('GET %s', $key));
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function has(string $key): bool
    {
        return $this->send(sprintf('EXISTS %s', $key)) === 1;
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function delete(string $key): bool
    {
        return $this->send(sprintf('DEL %s', $key)) === 1;
    }

    /**
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    public function truncate(): bool
    {
        return (bool)$this->send('FLUSHALL');
    }

    /**
     * @return void
     */
    public function disconnect(): void
    {
        if ($this->isConnected()) {
            try {
                $this->send("QUIT");
            } catch (\Throwable) {
            }
        }
        $this->backend = null;
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
     * @throws RedisConnectionException
     * @throws RedisOpException
     */
    private function send(string $command): int|string|null|bool
    {
        if (!$this->backend) {
            $this->connect();
        }

        $command = trim($command);
        if (strtolower($command) == "disconnect") {
            return @fclose($this->backend);
        }

        error_clear_last();
        $write = @fwrite($this->backend, $this->prepareCommand($command));
        if ($write === false) {
            throw new RedisConnectionException(sprintf('Failed to send "%1$s" command', explode(" ", $command)[0]));
        }

        return $this->response();
    }

    /**
     * @throws RedisOpException
     */
    private function response(): int|string|null
    {
        // Get response from stream
        $response = fgets($this->backend);
        if (!is_string($response)) {
            $timedOut = @stream_get_meta_data($this->backend)["timed_out"] ?? null;
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
                    $data = stream_get_contents($this->backend, $bytes + 2);
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