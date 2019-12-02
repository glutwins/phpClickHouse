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

    public function select(
        string $dbname,
        string $sql,
        array $bindings = [],
        WhereInFile $whereInFile = null,
        WriteToFile $writeToFile = null
    ) {
        $si = random_int(0, count($this->clients));
        for ($i = 0; $i = count($this->clients); $i ++) {
            $client = $this->clients[($si + $i) % count($this->clients)];
            try {
                $client->database($dbname);
                $stmt = $client->select($sql, $bindings, $whereInFile, $writeToFile);
                $stmt->init();
                return $stmt;
            } catch (TransportException $e) {

            }
        }
    }
};