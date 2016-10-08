<?php

namespace Arm;

use Arm\Exception\ConfigurationException;
use Arm\Exception\QueryException;
use Arm\Exception\ResultException;
use PDO;
use PDOException;

/**
 * Arm: ArrayRelationalMapping
 * A database abstraction class with behavior similar to an ORM
 *
 * @author Nick Wakeman <nick.wakeman@gmail.com>
 * @since  2016-10-07
 *
 * @todo add param matching and adding of ':' for all query types (just like put())
 */
class Arm
{
    protected $db;
    protected $numQueries = 0;
    protected $primaryKeys = [];
    protected $tables = [];
    protected $type;
    protected $created = false;
    protected $updated = false;

    public function __construct($databaseInfo)
    {
        $this->type = $databaseInfo->type;
        $this->setTimestampedFields($databaseInfo);
        switch (strtolower($databaseInfo->type)) {
            case 'mysql':
                $this->loadMysql($databaseInfo->dsn, $databaseInfo->credentials);
            break;
            case 'sqlite':
                $this->loadSqlite($databaseInfo->filename);
            break;
            default:
                Throw new ConfigurationException("Database type must be 'mysql' or 'sqlite'");
            break;
        }
    }

    protected function setTimestampedFields($databaseInfo)
    {
        if (isset($databaseInfo->timestampedFields)) {
            $tf = $databaseInfo->timestampedFields;
            foreach (['created', 'updated'] as $field) {
                if (!isset($tf->$field)) {
                    Throw new ConfigurationException("You must set values for both 'created' and 'updated'");
                } elseif (!(false === $tf->$field || is_string($tf->$field))) {
                    Throw new ConfigurationException("timestampedFields for $field must be a string or false");
                } else {
                    $this->$field = $databaseInfo->timestampedFields->$field;
                }
            }
        }
    }

    protected function loadMysql($dsn, $credentials)
    {
        $pdoOptions  = array(
            PDO::MYSQL_ATTR_FOUND_ROWS   => true,
            PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC
        );
        $this->db = new PDO(
            "mysql:host={$dsn->hostname};dbname={$dsn->database}",
            $credentials->username,
            $credentials->password,
            $pdoOptions
        );

        $query = 'SELECT TABLE_NAME, COLUMN_NAME, COLUMN_KEY FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = :tableSchema';
        $result = $this->select($query, [':tableSchema' => $dsn->database]);
        foreach ($result as $tableColumn) {
            if ($tableColumn['COLUMN_KEY'] === 'PRI') {
              $this->primaryKeys[$tableColumn['TABLE_NAME']] = $tableColumn['COLUMN_NAME'];
            } else {
              $this->tables[$tableColumn['TABLE_NAME']][] = $tableColumn['COLUMN_NAME'];
            }
        }
    }

    protected function loadSqlite($filename)
    {
        $filename = 'sqlite::' . $filename;
        $pdoOptions  = array(
            PDO::MYSQL_ATTR_FOUND_ROWS   => true,
            PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC
        );
        $this->db = new PDO($filename, null, null, $pdoOptions);

        $query = 'SELECT name FROM sqlite_master WHERE type="table"';
        $tableNames = $this->selectOneField($query);
        if (is_array($tableNames) && count($tableNames)) {
            foreach ($tableNames as $tableName) {
                $tableStructure = $this->select("PRAGMA table_info({$tableName})");

                if (is_array($tableStructure) && count($tableStructure)) {
                    foreach ($tableStructure as $column) {
                        if ($column['pk'] == 1) {
                            $this->primaryKeys[$tableName] = $column['name'];
                        }
                        $this->tables[$tableName][] = $column['name'];
                    }
                }
            }
        }
    }

    protected function countQuery()
    {
        $this->numQueries++;
    }

    public function getNumQueries()
    {
        return $this->numQueries;
    }

    public function command($query)
    {
        $stmt = $this->db->prepare($query);
        $stmt->execute();
        $this->countQuery();

        return true;
    }

    public function insert($query, $params)
    {
        $stmt = $this->db->prepare($query);
        $stmt->execute($params);
        $this->countQuery();

        return $this->db->lastInsertId();
    }

    public function select($query, $params = array())
    {
        $stmt = $this->db->prepare($query);
        $stmt->execute($params);
        $this->countQuery();

        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    }

    public function selectOne($query, $params = array())
    {
        $result = $this->select($query, $params);
        if (count($result) > 1) {
            Throw new ResultException('Database returned more than one result');
        }
        return array_shift($result);
    }

    public function update($query, $params = array())
    {
        $stmt = $this->db->prepare($query);
        $stmt->execute($params);
        $this->countQuery();

        return $stmt->rowCount();
    }

    public function delete($query, $params = array())
    {
        return $this->update($query, $params);
    }

    public function put($tableName, $object, $id = null)
    {
        if (!array_key_exists($tableName, $this->tables)) {
            throw new QueryException("Table $tableName does not exist");
        } else {
            $fieldsAndTokens = [];
            foreach ($object as $key => $value) {
                if (in_array($key, $this->tables[$tableName])) {
                    $fieldsAndTokens[$key] = ':' . $key;
                }
            }
            if (empty($fieldsAndTokens)) {
                throw new QueryException("No matching columns in table $tableName");
            } else {
                if ($id) {
                    $updateParams = [];
                    foreach ($fieldsAndTokens as $field => $token) {
                        $updateParams[] = "$field = $token";
                    }
                    if ($this->updated && in_array($this->updated, $this->tables[$tableName])) {
                        $updateParams[] = "$this->updated = " . (($this->type == 'SQLITE') ? 'DATETIME()' : 'NOW()');
                    }
                    $query = "UPDATE $tableName SET " . implode(', ', $updateParams) . " WHERE {$this->primaryKeys[$tableName]} = :{$this->primaryKeys[$tableName]}";
                    $fieldsAndTokens[$this->primaryKeys[$tableName]] = ":{$this->primaryKeys[$tableName]}";
                    $object[$this->primaryKeys[$tableName]] = $id;
                } else {
                    if ($this->created && in_array($this->created, $this->tables[$tableName])) {
                        $fieldsAndTokens[$this->created] = ($this->type == 'SQLITE') ? 'DATETIME()' : 'NOW()';
                    }
                    if ($this->updated && in_array($this->updated, $this->tables[$tableName])) {
                        $fieldsAndTokens[$this->updated] = ($this->type == 'SQLITE') ? 'DATETIME()' : 'NOW()';
                    }
                    $query = "INSERT INTO $tableName (" . implode(', ', array_keys($fieldsAndTokens)) . ') VALUES (' . implode(', ', $fieldsAndTokens) . ')';
                }
                $bind = [];
                foreach ($fieldsAndTokens as $field => $token) {
                    if (array_key_exists($field, $object)) {
                      $bind[$token] = $object[$field];
                    }
                }
                if ($id) {
                    return ($this->update($query, $bind)) ? $id : false;
                } else {
                    return $this->insert($query, $bind);
                }
            }
        }
    }

    public function getOneById($tableName, $id)
    {
        return $this->selectOne("SELECT * FROM $tableName WHERE {$this->primaryKeys[$tableName]} = :{$this->primaryKeys[$tableName]}", array(":{$this->primaryKeys[$tableName]}" => $id));
    }

    public function deleteOneById($tableName, $id)
    {
        return $this->update("DELETE FROM $tableName WHERE {$this->primaryKeys[$tableName]} = :{$this->primaryKeys[$tableName]} LIMIT 1", array(":{$this->primaryKeys[$tableName]}" => $id));
    }

    public function existsById($tableName, $id)
    {
        return $this->selectOneValue("SELECT EXISTS(SELECT * FROM {$tableName} WHERE {$this->primaryKeys[$tableName]} = :id LIMIT 1) AS e", [':id' => $id]);
    }

    public function selectOneField($query, $params = [])
    {
        $array = [];
        $result = $this->select($query, $params);
        if (count($result)) {
            foreach ($result as $row) {
                if (count($row) > 1) {
                    Throw new QueryException('Your query selected more than one field');
                } else {
                    foreach ($row as $value) {
                        $array[] = $value;
                    }
                }
            }
        }
        return $array;
    }

    public function selectOneValue($query, $params = [])
    {
        $result = $this->selectOne($query, $params);
        if (count($result) != 1) {
            Throw new ResultException('Database returned more than one value');
        }
        return array_shift($result);
    }
}
