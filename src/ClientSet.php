<?php

declare(strict_types=1);

namespace ClickHouseDB;

use ClickHouseDB\Client;
use ClickHouseDB\Exception\TransportException;

/**
 * Class Client
 * @package ClickHouseDB
 */
class ClientSet {
    /** @var Client */
    private $clients = [];

    /**
     * @param mixed[] $connectParams
     * @param mixed[] $settings
     */
    public function __construct(array $connectParams, array $settings = [])
    {
        $connectTimeout = 0.01;
        if (array_key_exists('connect_timeout', $settings)) {
            $connectTimeout = $settings['connect_timeout'];
        }
        foreach ($connectParams as $param) {
            $client = new Client($param, $settings);
            $client->setConnectTimeOut($connectTimeout);
            $this->clients[] = $client;
        }
    }

    public function getAllNodes() {
        return $this->clients;
    }

    public function select(
        string $dbname,
        string $sql,
        array $bindings = [],
        WhereInFile $whereInFile = null,
        WriteToFile $writeToFile = null
    ) {
        $exception = null;
        $si = random_int(0, count($this->clients));
        for ($i = 0; $i < count($this->clients); $i ++) {
            $client = $this->clients[($si + $i) % count($this->clients)];
            try {
                $client->database($dbname);
                $stmt = $client->select($sql, $bindings, $whereInFile, $writeToFile);
                $stmt->init();
                return $stmt;
            } catch (TransportException $e) {
                $exception = $e;
            }
        }
        throw $exception;
    }

    /**
     * Query CREATE/DROP
     *
     * @param mixed[] $bindings
     * @return Statement
     */
    public function write($dbname, string $sql, array $bindings = [], bool $exception = true)
    {
        $exception = null;
        for ($i = 0; $i < count($this->clients); $i ++) {
            $client = $this->clients[$i];
            try {
                $client->database($dbname);
                return $client->write($sql, $bindings, $exception);
            } catch (TransportException $e) {
                $exception = $e;
            }
        }
        throw $exception;
    }

    /**
     * Query CREATE/DROP
     *
     * @param mixed[] $bindings
     */
    public function writeAllNode($dbname, string $sql, array $bindings = [], bool $exception = true)
    {
        for ($i = 0; $i < count($this->clients); $i ++) {
            $client = $this->clients[$i];
            $client->database($dbname);
            $client->write($sql, $bindings, $exception);
        }
    }

    public function setReadOnlyUser(bool $flag)
    {
        foreach ($this->clients AS $client) $client->setReadOnlyUser($flag);
    }
};