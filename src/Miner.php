<?php

  /**
   * A dead simple PHP class for building SQL statements. No manual string
   * concatenation necessary.
   *
   * @author    Justin Stayton
   * @copyright Copyright 2013 by Justin Stayton
   * @license   https://github.com/jstayton/Miner/blob/master/LICENSE-MIT MIT
   * @package   Miner
   * @version   0.9.2
   */
  class Miner {

    /**
     * INNER JOIN type.
     */
    const INNER_JOIN = "INNER JOIN";

    /**
     * LEFT JOIN type.
     */
    const LEFT_JOIN = "LEFT JOIN";

    /**
     * RIGHT JOIN type.
     */
    const RIGHT_JOIN = "RIGHT JOIN";

    /**
     * AND logical operator.
     */
    const LOGICAL_AND = "AND";

    /**
     * OR logical operator.
     */
    const LOGICAL_OR = "OR";

    /**
     * Equals comparison operator.
     */
    const EQUALS = "=";

    /**
     * Not equals comparison operator.
     */
    const NOT_EQUALS = "!=";

    /**
     * Less than comparison operator.
     */
    const LESS_THAN = "<";

    /**
     * Less than or equal to comparison operator.
     */
    const LESS_THAN_OR_EQUAL = "<=";

    /**
     * Greater than comparison operator.
     */
    const GREATER_THAN = ">";

    /**
     * Greater than or equal to comparison operator.
     */
    const GREATER_THAN_OR_EQUAL = ">=";

    /**
     * IN comparison operator.
     */
    const IN = "IN";

    /**
     * NOT IN comparison operator.
     */
    const NOT_IN = "NOT IN";

    /**
     * LIKE comparison operator.
     */
    const LIKE = "LIKE";

    /**
     * NOT LIKE comparison operator.
     */
    const NOT_LIKE = "NOT LIKE";

    /**
     * ILIKE comparison operator.
     */
    const ILIKE = "ILIKE";

    /**
     * REGEXP comparison operator.
     */
    const REGEX = "REGEXP";

    /**
     * NOT REGEXP comparison operator.
     */
    const NOT_REGEX = "NOT REGEXP";

    /**
     * BETWEEN comparison operator.
     */
    const BETWEEN = "BETWEEN";

    /**
     * NOT BETWEEN comparison operator.
     */
    const NOT_BETWEEN = "NOT BETWEEN";

    /**
     * IS comparison operator.
     */
    const IS = "IS";

    /**
     * IS NOT comparison operator.
     */
    const IS_NOT = "IS NOT";

    /**
     * Ascending ORDER BY direction.
     */
    const ORDER_BY_ASC = "ASC";

    /**
     * Descending ORDER BY direction.
     */
    const ORDER_BY_DESC = "DESC";

    /**
     * Open bracket for grouping criteria.
     */
    const BRACKET_OPEN = "(";

    /**
     * Closing bracket for grouping criteria.
     */
    const BRACKET_CLOSE = ")";

    /**
      * Specifies that the where() column contains a subquery
      */
    const SUB_QUERY = "subquery";

    /**
      * Specifies that the where() column contains a subquery IN
      */
    const SUB_QUERY_IN = "subquery_in";

    /**
      * Handles raw where clauses
      */
    const RAW_WHERE = "RAW_WHERE";

    /**
     * PDO database connection to use in executing the statement.
     *
     * @var PDO|null
     */
    protected $PdoConnection;

    /**
     * Whether to automatically escape values.
     *
     * @var bool|null
     */
    protected $autoQuote;

    /**
     * Execution options like DISTINCT and SQL_CALC_FOUND_ROWS.
     *
     * @var array
     */
    protected $option;

    /**
     * Execution options that are appended to the end of the query like FOR UPDATE
     * @var array
     */
    protected $end_option;

    /**
     * Columns, tables, and expressions to SELECT from.
     *
     * @var array
     */
    protected $select;

    /**
     * Table to INSERT into.
     *
     * @var string
     */
    protected $insert;

    /**
     * Table to REPLACE into.
     *
     * @var string
     */
    protected $replace;

    /**
     * Table to UPDATE.
     *
     * @var string
     */
    protected $update;

    /**
     * Tables to DELETE from, or true if deleting from the FROM table.
     *
     * @var array|true
     */
    protected $delete;

    /**
     * Column values to INSERT or UPDATE.
     *
     * @var array
     */
    protected $set;

    /**
     * Table to select FROM.
     *
     * @var array
     */
    protected $from;

    /**
     * JOIN tables and ON criteria.
     *
     * @var array
     */
    protected $join;

    /**
     * WHERE criteria.
     *
     * @var array
     */
    protected $where;

    /**
     * Columns to GROUP BY.
     *
     * @var array
     */
    protected $groupBy;

    /**
     * HAVING criteria.
     *
     * @var array
     */
    protected $having;

    /**
     * Columns to ORDER BY.
     *
     * @var array
     */
    protected $orderBy;

    /**
     * Number of rows to return from offset.
     *
     * @var array
     */
    protected $limit;

    /**
     * SET placeholder values.
     *
     * @var array
     */
    protected $setPlaceholderValues;

    /**
     * WHERE placeholder values.
     *
     * @var array
     */
    protected $wherePlaceholderValues;

    /**
     * HAVING placeholder values.
     *
     * @var array
     */
    protected $havingPlaceholderValues;

    private $_rowsFound;

	/**
	 * On Duplicate Key values
	 * @var Miner
	 */
	protected $onDuplicateKeyValue;

    /**
     * Constructor.
     *
     * @param  PDO|null $PdoConnection optional PDO database connection
     * @param  bool $autoQuote optional auto-escape values, default true
     * @return Miner
     */
    public function __construct(PDO $PdoConnection = null, $autoQuote = true) {
      $this->option = array();
      $this->end_option = array();
      $this->select = array();
      $this->delete = array();
      $this->set = array();
      $this->from = array();
      $this->join = array();
      $this->where = array();
      $this->groupBy = array();
      $this->having = array();
      $this->orderBy = array();
      $this->limit = array();

      $this->setPlaceholderValues = array();
      $this->wherePlaceholderValues = array();
      $this->havingPlaceholderValues = array();

      $this->setPdoConnection($PdoConnection)
           ->setAutoQuote($autoQuote);
    }

    /**
     * Set the PDO database connection to use in executing this statement.
     *
     * @param  PDO|null $PdoConnection optional PDO database connection
     * @return Miner
     */
    public function setPdoConnection(PDO $PdoConnection = null) {
      $this->PdoConnection = $PdoConnection;

      return $this;
    }

    /**
     * Get the PDO database connection to use in executing this statement.
     *
     * @return PDO|null
     */
    public function getPdoConnection() {
      return $this->PdoConnection;
    }

    /**
     * Set whether to automatically escape values.
     *
     * @param  bool|null $autoQuote whether to automatically escape values
     * @return Miner
     */
    public function setAutoQuote($autoQuote) {
      $this->autoQuote = $autoQuote;

      return $this;
    }

    /**
     * Get whether values will be automatically escaped.
     *
     * The $override parameter is for convenience in checking if a specific
     * value should be quoted differently than the rest. 'null' defers to the
     * global setting.
     *
     * @param  bool|null $override value-specific override for convenience
     * @return bool
     */
    public function getAutoQuote($override = null) {
      return $override === null ? $this->autoQuote : $override;
    }

    /**
     * Safely escape a value if auto-quoting is enabled, or do nothing if
     * disabled.
     *
     * The $override parameter is for convenience in checking if a specific
     * value should be quoted differently than the rest. 'null' defers to the
     * global setting.
     *
     * @param  mixed $value value to escape (or not)
     * @param  bool|null $override value-specific override for convenience
     * @return mixed|false value (escaped or original) or false if failed
     */
    public function autoQuote($value, $override = null) {
      return $this->getAutoQuote($override) ? $this->quote($value) : $value;
    }

    /**
     * Safely escape a value for use in a statement.
     *
     * @param  mixed $value value to escape
     * @return mixed|false escaped value or false if failed
     */
    public function quote($value) {
      $PdoConnection = $this->getPdoConnection();

      if($value instanceof \DateTime){
      	$value = $value->format('Y-m-d H:i');
      }

      // If a PDO database connection is set, use it to quote the value using
      // the underlying database. Otherwise, quote it manually.
      if ($PdoConnection) {
        return $PdoConnection->quote($value);
      }
      elseif (is_numeric($value)) {
        return $value;
      }
      elseif (is_null($value)) {
        return "NULL";
      }
      else {
        return "'" . addslashes($value) . "'";
      }
    }

    /**
     * Add an execution option like DISTINCT or SQL_CALC_FOUND_ROWS.
     *
     * @param  string $option execution option to add
     * @return Miner
     */
    public function option($option) {
      $this->option[] = $option;

      return $this;
    }

    /**
     * Add an execution option to the end of the query like FOR UPDATE
     * @param string $option execution option to add
     * @return Miner
     */
    public function endOption($option){
	$this->end_option[] = $option;

	return $this;
    }

    /**
     * Get the execution options portion of the statement as a string.
     *
     * @param  bool $includeTrailingSpace optional include space after options
     * @return string execution options portion of the statement
     */
    public function getOptionsString($includeTrailingSpace = false) {
      $statement = "";

      if (!$this->option) {
        return $statement;
      }

      $statement .= implode(' ', $this->option);

      if ($includeTrailingSpace) {
        $statement .= " ";
      }

      return $statement;
    }

    /**
     * Get the end execution options portion of the statement as a string.
     *
     * @param  bool $includeTrailingSpace optional include space after options
     * @return string execution options portion of the statement
     */
    public function getEndOptionsString($includeTrailingSpace = false) {
      $statement = "";

      if (!$this->end_option) {
        return $statement;
      }

      $statement .= implode(' ', $this->end_option);

      if ($includeTrailingSpace) {
        $statement .= " ";
      }

      return $statement;
    }

    /**
     * Merge this Miner's execution options into the given Miner.
     *
     * @param  Miner $Miner to merge into
     * @return Miner
     */
    public function mergeOptionsInto(Miner $Miner) {
      foreach ($this->option as $option) {
        $Miner->option($option);
      }

      return $Miner;
    }

    /**
     * Merge this Miner's end execution options into the given Miner.
     *
     * @param  Miner $Miner to merge into
     * @return Miner
     */
    public function mergeEndOptionsInto(Miner $Miner) {
      foreach ($this->end_option as $option) {
        $Miner->end_option($option);
      }

      return $Miner;
    }

    /**
     * Add SQL_CALC_FOUND_ROWS execution option.
     *
     * @return Miner
     */
    public function calcFoundRows() {
      return $this->option('SQL_CALC_FOUND_ROWS');
    }

    /**
     * Add DISTINCT execution option.
     *
     * @return Miner
     */
    public function distinct() {
      return $this->option('DISTINCT');
    }

    /**
     * Add a SELECT column, table, or expression with optional alias.
     *
     * @param  string $column column name, table name, or expression
     * @param  string $alias optional alias
     * @return Miner
     */
    public function select($column, $alias = null) {
      if(is_array($column)){
      	foreach($column as $column => $alias){
      		if(is_int($column)){
      			$this->select[$alias] = null;
      		}else{
      			$this->select[$column] = $alias;
      		}
      	}
      }else if($column instanceof self){
      	$this->select["(" . $column->getQueryString(false) . ")"] = $alias;
      }else{
      	$this->select[$column] = $alias;
      }
      return $this;
    }

    /**
     * Merge this Miner's SELECT into the given Miner.
     *
     * @param  Miner $Miner to merge into
     * @return Miner
     */
    public function mergeSelectInto(Miner $Miner) {
      $this->mergeOptionsInto($Miner);

      foreach ($this->select as $column => $alias) {
        $Miner->select($column, $alias);
      }

      return $Miner;
    }

    /**
     * Get the SELECT portion of the statement as a string.
     *
     * @param  bool $includeText optional include 'SELECT' text, default true
     * @return string SELECT portion of the statement
     */
    public function getSelectString($includeText = true) {
      $statement = "";

      if (!$this->select) {
        return $statement;
      }

      $statement .= $this->getOptionsString(true);

      foreach ($this->select as $column => $alias) {
        $statement .= $column;

        if ($alias) {
          $statement .= " AS " . $alias;
        }

        $statement .= ", ";
      }

      $statement = substr($statement, 0, -2);

      if ($includeText && $statement) {
        $statement = "SELECT " . $statement;
      }

      return $statement;
    }

    /**
     * Set the INSERT table.
     *
     * @param  string $table INSERT table
     * @return Miner
     */
    public function insert($table) {
      $this->insert = $table;
      $this->set('syscreated', 'NULL', FALSE);
      if (isset($_SESSION) && isset($_SESSION['user']['id'])) {
          $this->set('syscreator', $_SESSION['user']['id']);
      }
      else {
          $this->set('syscreator', 0);
      }
      return $this;
    }

    /**
     * Merge this Miner's INSERT into the given Miner.
     *
     * @param  Miner $Miner to merge into
     * @return Miner
     */
    public function mergeInsertInto(Miner $Miner) {
      $this->mergeOptionsInto($Miner);

      if ($this->insert) {
        $Miner->insert($this->getInsert());
      }

      return $Miner;
    }

    /**
     * Get the INSERT table.
     *
     * @return string INSERT table
     */
    public function getInsert() {
      return $this->insert;
    }

    /**
     * Get the INSERT portion of the statement as a string.
     *
     * @param  bool $includeText optional include 'INSERT' text, default true
     * @return string INSERT portion of the statement
     */
    public function getInsertString($includeText = true) {
      $statement = "";

      if (!$this->insert) {
        return $statement;
      }

      $statement .= $this->getOptionsString(true);

      $statement .= $this->getInsert();

      if ($includeText && $statement) {
        $statement = "INSERT " . $statement;
      }

      return $statement;
    }

    /**
     * Set the REPLACE table.
     *
     * @param  string $table REPLACE table
     * @return Miner
     */
    public function replace($table) {
      $this->replace = $table;

      return $this;
    }

    /**
     * Merge this Miner's REPLACE into the given Miner.
     *
     * @param  Miner $Miner to merge into
     * @return Miner
     */
    public function mergeReplaceInto(Miner $Miner) {
      $this->mergeOptionsInto($Miner);

      if ($this->replace) {
        $Miner->replace($this->getReplace());
      }

      return $Miner;
    }

    /**
     * Get the REPLACE table.
     *
     * @return string REPLACE table
     */
    public function getReplace() {
      return $this->replace;
    }

    /**
     * Get the REPLACE portion of the statement as a string.
     *
     * @param  bool $includeText optional include 'REPLACE' text, default true
     * @return string REPLACE portion of the statement
     */
    public function getReplaceString($includeText = true) {
      $statement = "";

      if (!$this->replace) {
        return $statement;
      }

      $statement .= $this->getOptionsString(true);

      $statement .= $this->getReplace();

      if ($includeText && $statement) {
        $statement = "REPLACE " . $statement;
      }

      return $statement;
    }

    /**
     * Set the UPDATE table.
     *
     * @param  string $table UPDATE table
     * @return Miner
     */
    public function update($table) {
      $this->update = $table;
      $this->set('sysmodified', 'NULL', FALSE);
      if (isset($_SESSION) && isset($_SESSION['user']['id'])) {
          $this->set('sysmodifier', $_SESSION['user']['id']);
      }
      else {
          $this->set('sysmodifier', 0);
      }

      return $this;
    }

    /**
     * Merge this Miner's UPDATE into the given Miner.
     *
     * @param  Miner $Miner to merge into
     * @return Miner
     */
    public function mergeUpdateInto(Miner $Miner) {
      $this->mergeOptionsInto($Miner);

      if ($this->update) {
        $Miner->update($this->getUpdate());
      }

      return $Miner;
    }

    /**
     * Get the UPDATE table.
     *
     * @return string UPDATE table
     */
    public function getUpdate() {
      return $this->update;
    }

    /**
     * Get the UPDATE portion of the statement as a string.
     *
     * @param  bool $includeText optional include 'UPDATE' text, default true
     * @return string UPDATE portion of the statement
     */
    public function getUpdateString($includeText = true) {
      $statement = "";

      if (!$this->update) {
        return $statement;
      }

      $statement .= $this->getOptionsString(true);

      $statement .= $this->getUpdate();

      // Add any JOINs.
      $statement .= " " . $this->getJoinString();

      $statement  = rtrim($statement);

      if ($includeText && $statement) {
        $statement = "UPDATE " . $statement;
      }

      return $statement;
    }

    /**
     * Add a table to DELETE from, or false if deleting from the FROM table.
     *
     * @param  string|false $table optional table name, default false
     * @return Miner
     */
    public function delete($table = false) {
      if ($table === false) {
        $this->delete = true;
      }
      else {
        // Reset the array in case the class variable was previously set to a
        // boolean value.
        if (!is_array($this->delete)) {
          $this->delete = array();
        }

        $this->delete[] = $table;
      }

      return $this;
    }

    /**
     * Merge this Miner's DELETE into the given Miner.
     *
     * @param  Miner $Miner to merge into
     * @return Miner
     */
    public function mergeDeleteInto(Miner $Miner) {
      $this->mergeOptionsInto($Miner);

      if ($this->isDeleteTableFrom()) {
        $Miner->delete();
      }
      else {
        foreach ($this->delete as $delete) {
          $Miner->delete($delete);
        }
      }

      return $Miner;
    }

    /**
     * Get the DELETE portion of the statement as a string.
     *
     * @param  bool $includeText optional include 'DELETE' text, default true
     * @return string DELETE portion of the statement
     */
    public function getDeleteString($includeText = true) {
      $statement = "";

      if (!$this->delete && !$this->isDeleteTableFrom()) {
        return $statement;
      }

      $statement .= $this->getOptionsString(true);

      if (is_array($this->delete)) {
        $statement .= implode(', ', $this->delete);
      }

      if ($includeText && ($statement || $this->isDeleteTableFrom())) {
        $statement = "DELETE " . $statement;

        // Trim in case the table is specified in FROM.
        $statement = trim($statement);
      }

      return $statement;
    }

    /**
     * Whether the FROM table is the single table to delete from.
     *
     * @return bool whether the delete table is FROM
     */
    protected function isDeleteTableFrom() {
      return $this->delete === true;
    }

    /**
     * Add a column value to INSERT or UPDATE.
     *
     * @param  string $column column name
     * @param  mixed $value value
     * @param  bool|null $quote optional auto-escape value, default to global
     * @return Miner
     */
    public function set($column, $value, $quote = null) {
      $this->set[] = array('column' => $column,
                           'value'  => $value,
                           'quote'  => $quote);

      return $this;
    }

    /**
     * Merge this Miner's SET into the given Miner.
     *
     * @param  Miner $Miner to merge into
     * @return Miner
     */
    public function mergeSetInto(Miner $Miner) {
      foreach ($this->set as $set) {
        $Miner->set($set['column'], $set['value'], $set['quote']);
      }

      return $Miner;
    }

    /**
     * Get the SET portion of the statement as a string.
     *
     * @param  bool $usePlaceholders optional use ? placeholders, default true
     * @param  bool $includeText optional include 'SET' text, default true
     * @return string SET portion of the statement
     */
    public function getSetString($usePlaceholders = true, $includeText = true) {
      $statement = "";
      $this->setPlaceholderValues = array();

      foreach ($this->set as $set) {
        $autoQuote = $this->getAutoQuote($set['quote']);

        if ($usePlaceholders && $autoQuote) {
          $statement .= $set['column'] . " " . self::EQUALS . " ?, ";

          $this->setPlaceholderValues[] = $set['value'];
        }
        else {
          $statement .= $set['column'] . " " . self::EQUALS . " " . $this->autoQuote($set['value'], $autoQuote) . ", ";
        }
      }

      $statement = substr($statement, 0, -2);

      if ($includeText && $statement) {
        $statement = "SET " . $statement;
      }

      return $statement;
    }

    /**
     * Get the SET placeholder values.
     *
     * @return array SET placeholder values
     */
    public function getSetPlaceholderValues() {
      return $this->setPlaceholderValues;
    }

	  /**
	   *  Provides support for ON DUPLICATE KEY
	   * @param Miner $miner A miner instance with SET values to update
	   */
    public function onDuplicateKey(Miner $miner){
	    $this->onDuplicateKeyValue = $miner;
	    return $this;
    }

    /**
     * Set the FROM table with optional alias.
     *
     * @param  string $table table name
     * @param  string $alias optional alias
     * @return Miner
     */
    public function from($table, $alias = null) {
      $this->from['table'] = $table;
      $this->from['alias'] = $alias;

      return $this;
    }

    /**
     * Merge this Miner's FROM into the given Miner.
     *
     * @param  Miner $Miner to merge into
     * @return Miner
     */
    public function mergeFromInto(Miner $Miner) {
      if ($this->from) {
        $Miner->from($this->getFrom(), $this->getFromAlias());
      }

      return $Miner;
    }

    /**
     * Get the FROM table.
     *
     * @return string FROM table
     */
    public function getFrom() {
      return $this->from['table'];
    }

    /**
     * Get the FROM table alias.
     *
     * @return string FROM table alias
     */
    public function getFromAlias() {
      return $this->from['alias'];
    }

    /**
     * Add a JOIN table with optional ON criteria.
     *
     * @param  string $table table name
     * @param  string|array $criteria optional ON criteria
     * @param  string $type optional type of join, default INNER JOIN
     * @param  string $alias optional alias
     * @return Miner
     */
    public function join($table, $criteria = null, $type = self::INNER_JOIN, $alias = null) {
      if (is_string($criteria)) {
        $criteria = array($criteria);
      }

      $join = array('table'    => $table,
                            'criteria' => $criteria,
                            'type'     => $type,
                            'alias'    => $alias);

      // Dont add double joins to the same table
      if((array_search($join, $this->join) === false)){
      	$this->join[] = $join;
      }

      return $this;
    }

    /**
     * Add an INNER JOIN table with optional ON criteria.
     *
     * @param  string $table table name
     * @param  string|array $criteria optional ON criteria
     * @param  string $alias optional alias
     * @return Miner
     */
    public function innerJoin($table, $criteria = null, $alias = null) {
      return $this->join($table, $criteria, self::INNER_JOIN, $alias);
    }

    /**
     * Add a LEFT JOIN table with optional ON criteria.
     *
     * @param  string $table table name
     * @param  string|array $criteria optional ON criteria
     * @param  string $alias optional alias
     * @return Miner
     */
    public function leftJoin($table, $criteria = null, $alias = null) {
      return $this->join($table, $criteria, self::LEFT_JOIN, $alias);
    }

    /**
     * Add a RIGHT JOIN table with optional ON criteria.
     *
     * @param  string $table table name
     * @param  string|array $criteria optional ON criteria
     * @param  string $alias optional alias
     * @return Miner
     */
    public function rightJoin($table, $criteria = null, $alias = null) {
      return $this->join($table, $criteria, self::RIGHT_JOIN, $alias);
    }

    /**
     * Merge this Miner's JOINs into the given Miner.
     *
     * @param  Miner $Miner to merge into
     * @return Miner
     */
    public function mergeJoinInto(Miner $Miner) {
      foreach ($this->join as $join) {
        $Miner->join($join['table'], $join['criteria'], $join['type'], $join['alias']);
      }

      return $Miner;
    }

    /**
     * Get an ON criteria string joining the specified table and column to the
     * same column of the previous JOIN or FROM table.
     *
     * @param  int $joinIndex index of current join
     * @param  string $table current table name
     * @param  string $column current column name
     * @return string ON join criteria
     */
    protected function getJoinCriteriaUsingPreviousTable($joinIndex, $table, $column) {
      $joinCriteria = "";
      $previousJoinIndex = $joinIndex - 1;

      // If the previous table is from a JOIN, use that. Otherwise, use the
      // FROM table.
      if (array_key_exists($previousJoinIndex, $this->join)) {
        $previousTable = $this->join[$previousJoinIndex]['table'];
      }
      elseif ($this->isSelect()) {
        $previousTable = $this->getFrom();
      }
      elseif ($this->isUpdate()) {
        $previousTable = $this->getUpdate();
      }
      else {
        $previousTable = false;
      }

      // In the off chance there is no previous table.
      if ($previousTable) {
        $joinCriteria .= $previousTable . ".";
      }

      $joinCriteria .= $column . " " . self::EQUALS . " " . $table . "." . $column;

      return $joinCriteria;
    }

    /**
     * Get the JOIN portion of the statement as a string.
     *
     * @return string JOIN portion of the statement
     */
    public function getJoinString() {
      $statement = "";

      foreach ($this->join as $i => $join) {
        $statement .= " " . $join['type'] . " " . $join['table'];

        if ($join['alias']) {
          $statement .= " AS " . $join['alias'];
        }

        // Add ON criteria if specified.
        if ($join['criteria']) {
          $statement .= " ON ";

          foreach ($join['criteria'] as $x => $criterion) {
            // Logically join each criterion with AND.
            if ($x != 0) {
              $statement .= " " . self::LOGICAL_AND . " ";
            }

            // If the criterion does not include an equals sign, assume a
            // column name and join against the same column from the previous
            // table.
            if (strpos($criterion, '=') === false) {
              $statement .= $this->getJoinCriteriaUsingPreviousTable($i, $join['table'], $criterion);
            }
            else {
              $statement .= $criterion;
            }
          }
        }
      }

      $statement = trim($statement);

      return $statement;
    }

    /**
     * Get the FROM portion of the statement, including all JOINs, as a string.
     *
     * @param  bool $includeText optional include 'FROM' text, default true
     * @return string FROM portion of the statement
     */
    public function getFromString($includeText = true) {
      $statement = "";

      if (!$this->from) {
        return $statement;
      }

      $from = $this->getFrom();
      if($from instanceof self){
	$from = self::BRACKET_OPEN . $from . self::BRACKET_CLOSE;
      }

      $statement .= $from;

      if ($this->getFromAlias()) {
        $statement .= " AS " . $this->getFromAlias();
      }

      // Add any JOINs.
      $statement .= " " . $this->getJoinString();

      $statement  = rtrim($statement);

      if ($includeText && $statement) {
        $statement = "FROM " . $statement;
      }

      return $statement;
    }

    /**
     * Add an open bracket for nesting conditions to the specified WHERE or
     * HAVING criteria.
     *
     * @param  array $criteria WHERE or HAVING criteria
     * @param  string $connector optional logical connector, default AND
     * @return Miner
     */
    protected function openCriteria(array &$criteria, $connector = self::LOGICAL_AND) {
      $criteria[] = array('bracket'   => self::BRACKET_OPEN,
                          'connector' => $connector);

      return $this;
    }

    /**
     * Add a closing bracket for nesting conditions to the specified WHERE or
     * HAVING criteria.
     *
     * @param  array $criteria WHERE or HAVING criteria
     * @return Miner
     */
    protected function closeCriteria(array &$criteria) {
      $criteria[] = array('bracket'   => self::BRACKET_CLOSE,
                          'connector' => null);

      return $this;
    }

    /**
     * Add a condition to the specified WHERE or HAVING criteria.
     *
     * @param  array $criteria WHERE or HAVING criteria
     * @param  string $column column name
     * @param  mixed $value value
     * @param  string $operator optional comparison operator, default =
     * @param  string $connector optional logical connector, default AND
     * @param  bool|null $quote optional auto-escape value, default to global
     * @return Miner
     */
    protected function criteria(array &$criteria, $column, $value, $operator = self::EQUALS,
                              $connector = self::LOGICAL_AND, $quote = null) {
      $criteria[] = array('column'    => $column,
                          'value'     => $value,
                          'operator'  => $operator,
                          'connector' => $connector,
                          'quote'     => $quote);

      return $this;
    }

    /**
     * Add an OR condition to the specified WHERE or HAVING criteria.
     *
     * @param  array $criteria WHERE or HAVING criteria
     * @param  string $column column name
     * @param  mixed $value value
     * @param  string $operator optional comparison operator, default =
     * @param  bool|null $quote optional auto-escape value, default to global
     * @return Miner
     */
    protected function orCriteria(array &$criteria, $column, $value, $operator = self::EQUALS, $quote = null) {
      return $this->criteria($criteria, $column, $value, $operator, self::LOGICAL_OR, $quote);
    }

    /**
     * Add an IN condition to the specified WHERE or HAVING criteria.
     *
     * @param  array $criteria WHERE or HAVING criteria
     * @param  string $column column name
     * @param  array $values values
     * @param  string $connector optional logical connector, default AND
     * @param  bool|null $quote optional auto-escape value, default to global
     * @return Miner
     */
    protected function criteriaIn(array &$criteria, $column, array $values, $connector = self::LOGICAL_AND,
                                $quote = null) {
      return $this->criteria($criteria, $column, $values, self::IN, $connector, $quote);
    }

    /**
     * Add a NOT IN condition to the specified WHERE or HAVING criteria.
     *
     * @param  array $criteria WHERE or HAVING criteria
     * @param  string $column column name
     * @param  array $values values
     * @param  string $connector optional logical connector, default AND
     * @param  bool|null $quote optional auto-escape value, default to global
     * @return Miner
     */
    protected function criteriaNotIn(array &$criteria, $column, array $values, $connector = self::LOGICAL_AND,
                                   $quote = null) {
      return $this->criteria($criteria, $column, $values, self::NOT_IN, $connector, $quote);
    }

    /**
     * Add a BETWEEN condition to the specified WHERE or HAVING criteria.
     *
     * @param  array $criteria WHERE or HAVING criteria
     * @param  string $column column name
     * @param  mixed $min minimum value
     * @param  mixed $max maximum value
     * @param  string $connector optional logical connector, default AND
     * @param  bool|null $quote optional auto-escape value, default to global
     * @return Miner
     */
    protected function criteriaBetween(array &$criteria, $column, $min, $max, $connector = self::LOGICAL_AND,
                                     $quote = null) {
      return $this->criteria($criteria, $column, array($min, $max), self::BETWEEN, $connector, $quote);
    }

    /**
     * Add a NOT BETWEEN condition to the specified WHERE or HAVING criteria.
     *
     * @param  array $criteria WHERE or HAVING criteria
     * @param  string $column column name
     * @param  mixed $min minimum value
     * @param  mixed $max maximum value
     * @param  string $connector optional logical connector, default AND
     * @param  bool|null $quote optional auto-escape value, default to global
     * @return Miner
     */
    protected function criteriaNotBetween(array &$criteria, $column, $min, $max, $connector = self::LOGICAL_AND,
                                        $quote = null) {
      return $this->criteria($criteria, $column, array($min, $max), self::NOT_BETWEEN, $connector, $quote);
    }

    /**
     * Get the WHERE or HAVING portion of the statement as a string.
     *
     * @param  array $criteria WHERE or HAVING criteria
     * @param  bool $usePlaceholders optional use ? placeholders, default true
     * @param  array $placeholderValues optional placeholder values array
     * @return string WHERE or HAVING portion of the statement
     */
    protected function getCriteriaString(array &$criteria, $usePlaceholders = true,
                                       array &$placeholderValues = array()) {
      $statement = "";
      $placeholderValues = array();

      $useConnector = false;

      foreach ($criteria as $i => $criterion) {
        if (array_key_exists('bracket', $criterion)) {
          // If an open bracket, include the logical connector.
          if (strcmp($criterion['bracket'], self::BRACKET_OPEN) == 0) {
            if ($useConnector) {
              $statement .= " " . $criterion['connector'] . " ";
            }

            $useConnector = false;
          }
          else {
            $useConnector = true;
          }

          $statement .= $criterion['bracket'];
        }
        else {
          if ($useConnector) {
            $statement .= " " . $criterion['connector'] . " ";
          }

          $useConnector = true;
          $autoQuote = $this->getAutoQuote($criterion['quote']);

          switch ($criterion['operator']) {
            case self::BETWEEN:
            case self::NOT_BETWEEN:
              if ($usePlaceholders && $autoQuote) {
                $value = "? " . self::LOGICAL_AND . " ?";

                $placeholderValues[] = $criterion['value'][0];
                $placeholderValues[] = $criterion['value'][1];
              }
              else {
                $value = $this->autoQuote($criterion['value'][0], $autoQuote) . " " . self::LOGICAL_AND . " " .
                         $this->autoQuote($criterion['value'][1], $autoQuote);
              }

              break;

            case self::IN:
            case self::NOT_IN:
              if ($usePlaceholders && $autoQuote) {
                $value = self::BRACKET_OPEN . substr(str_repeat('?, ', count($criterion['value'])), 0, -2) .
                         self::BRACKET_CLOSE;

                $placeholderValues = array_merge($placeholderValues, $criterion['value']);
              }
              else {
                $value = self::BRACKET_OPEN;

                foreach ($criterion['value'] as $criterionValue) {
                  $value .= $this->autoQuote($criterionValue, $autoQuote) . ", ";
                }

                $value  = substr($value, 0, -2);
                $value .= self::BRACKET_CLOSE;
              }

              break;

            case self::IS:
            case self::IS_NOT:
              $value = $criterion['value'];

              break;

              case self::SUB_QUERY_IN:
                $value = "";
                $criterion['operator'] = self::IN;

                // Test if the subquery is another QueryBuilder
                if($criterion['value'] instanceof self){
                  if($usePlaceholders){
                    $value        = $criterion['value']->getQueryString();
                    $placeholderValues  = array_merge($placeholderValues, $criterion['value']->getPlaceholderValues());
                  }else{
                    $value =  $criterion['value']->getQueryString(false);
                  }
                }else{
                  // Raw sql subquery
                  $value = $criterion['value'];
                }

                // Wrap the subquery
                $value = self::BRACKET_OPEN . $value . self::BRACKET_CLOSE;

                break;

		case self::RAW_WHERE:
			$criterion['operator'] = '';
			$value = $criterion['value'];

			if($usePlaceholders){
				$placeholderValues[] = $criterion['value'];
			}else{
				$criterion['column'] = str_replace("?", $this->quote($criterion['value']), $criterion['column']);
			}

		break;

            default:
              if ($usePlaceholders && $autoQuote) {
                $value = "?";

                $placeholderValues[] = $criterion['value'];
              }
              else {
                $value = $this->autoQuote($criterion['value'], $autoQuote);
              }

              break;
          }

          $statement .= $criterion['column'] . " " . $criterion['operator'] . " " . $value;
        }
      }

      return $statement;
    }

    /**
     * Add an open bracket for nesting WHERE conditions.
     *
     * @param  string $connector optional logical connector, default AND
     * @return Miner
     */
    public function openWhere($connector = self::LOGICAL_AND) {
      return $this->openCriteria($this->where, $connector);
    }

    /**
     * Add a closing bracket for nesting WHERE conditions.
     *
     * @return Miner
     */
    public function closeWhere() {
      return $this->closeCriteria($this->where);
    }

    /**
     * Add a WHERE condition.
     *
     * @param  string $column column name
     * @param  mixed $value value
     * @param  string $operator optional comparison operator, default =
     * @param  string $connector optional logical connector, default AND
     * @param  bool|null $quote optional auto-escape value, default to global
     * @return Miner
     */
    public function where($column, $value, $operator = self::EQUALS, $connector = self::LOGICAL_AND, $quote = null) {
      return $this->criteria($this->where, $column, $value, $operator, $connector, $quote);
    }

  	/**
     * Add an AND WHERE condition.
     *
     * @param  string $column colum name
     * @param  mixed $value value
     * @param  string $operator optional comparison operator, default =
     * @param  bool|null $quote optional auto-escape value, default to global
     * @return Miner
     */
    public function andWhere($column, $value, $operator = self::EQUALS, $quote = null) {
      return $this->criteria($this->where, $column, $value, $operator, self::LOGICAL_AND, $quote);
    }

    /**
     * Add an OR WHERE condition.
     *
     * @param  string $column colum name
     * @param  mixed $value value
     * @param  string $operator optional comparison operator, default =
     * @param  bool|null $quote optional auto-escape value, default to global
     * @return Miner
     */
    public function orWhere($column, $value, $operator = self::EQUALS, $quote = null) {
      return $this->orCriteria($this->where, $column, $value, $operator, self::LOGICAL_OR, $quote);
    }

    /**
     * Add an IN WHERE condition.
     *
     * @param  string $column column name
     * @param  array $values values
     * @param  string $connector optional logical connector, default AND
     * @param  bool|null $quote optional auto-escape value, default to global
     * @return Miner
     */
    public function whereIn($column, array $values, $connector = self::LOGICAL_AND, $quote = null) {
      return $this->criteriaIn($this->where, $column, $values, $connector, $quote);
    }

    /**
     * Add a NOT IN WHERE condition.
     *
     * @param  string $column column name
     * @param  array $values values
     * @param  string $connector optional logical connector, default AND
     * @param  bool|null $quote optional auto-escape value, default to global
     * @return Miner
     */
    public function whereNotIn($column, array $values, $connector = self::LOGICAL_AND, $quote = null) {
      return $this->criteriaNotIn($this->where, $column, $values, $connector, $quote);
    }

    /**
     * Add a BETWEEN WHERE condition.
     *
     * @param  string $column column name
     * @param  mixed $min minimum value
     * @param  mixed $max maximum value
     * @param  string $connector optional logical connector, default AND
     * @param  bool|null $quote optional auto-escape value, default to global
     * @return Miner
     */
    public function whereBetween($column, $min, $max, $connector = self::LOGICAL_AND, $quote = null) {
      return $this->criteriaBetween($this->where, $column, $min, $max, $connector, $quote);
    }

    /**
     * Add a NOT BETWEEN WHERE condition.
     *
     * @param  string $column column name
     * @param  mixed $min minimum value
     * @param  mixed $max maximum value
     * @param  string $connector optional logical connector, default AND
     * @param  bool|null $quote optional auto-escape value, default to global
     * @return Miner
     */
    public function whereNotBetween($column, $min, $max, $connector = self::LOGICAL_AND, $quote = null) {
      return $this->criteriaNotBetween($this->where, $column, $min, $max, $connector, $quote);
    }

    /**
     * Merge this Miner's WHERE into the given Miner.
     *
     * @param  Miner $Miner to merge into
     * @return Miner
     */
    public function mergeWhereInto(Miner $Miner) {
      foreach ($this->where as $where) {
        // Handle open/close brackets differently than other criteria.
        if (array_key_exists('bracket', $where)) {
          if (strcmp($where['bracket'], self::BRACKET_OPEN) == 0) {
            $Miner->openWhere($where['connector']);
          }
          else {
            $Miner->closeWhere();
          }
        }
        else {
          $Miner->where($where['column'], $where['value'], $where['operator'], $where['connector'], $where['quote']);
        }
      }

      return $Miner;
    }

    /**
     * Get the WHERE portion of the statement as a string.
     *
     * @param  bool $usePlaceholders optional use ? placeholders, default true
     * @param  bool $includeText optional include 'WHERE' text, default true
     * @return string WHERE portion of the statement
     */
    public function getWhereString($usePlaceholders = true, $includeText = true) {
      $statement = $this->getCriteriaString($this->where, $usePlaceholders, $this->wherePlaceholderValues);

      if ($includeText && $statement) {
        $statement = "WHERE " . $statement;
      }

      return $statement;
    }

    /**
     * Get the WHERE placeholder values.
     *
     * @return array WHERE placeholder values
     */
    public function getWherePlaceholderValues() {
      return $this->wherePlaceholderValues;
    }

    /**
     * Add a GROUP BY column.
     *
     * @param  string $column column name
     * @param  string|null $order optional order direction, default none
     * @return Miner
     */
    public function groupBy($column, $order = null) {
      $this->groupBy[] = array('column' => $column,
                               'order'  => $order);

      return $this;
    }

    /**
     * Merge this Miner's GROUP BY into the given Miner.
     *
     * @param  Miner $Miner to merge into
     * @return Miner
     */
    public function mergeGroupByInto(Miner $Miner) {
      foreach ($this->groupBy as $groupBy) {
        $Miner->groupBy($groupBy['column'], $groupBy['order']);
      }

      return $Miner;
    }

    /**
     * Get the GROUP BY portion of the statement as a string.
     *
     * @param  bool $includeText optional include 'GROUP BY' text, default true
     * @return string GROUP BY portion of the statement
     */
    public function getGroupByString($includeText = true) {
      $statement = "";

      foreach ($this->groupBy as $groupBy) {
        $statement .= $groupBy['column'];

        if ($groupBy['order']) {
          $statement .= " " . $groupBy['order'];
        }

        $statement .= ", ";
      }

      $statement = substr($statement, 0, -2);

      if ($includeText && $statement) {
        $statement = "GROUP BY " . $statement;
      }

      return $statement;
    }

    /**
     * Add an open bracket for nesting HAVING conditions.
     *
     * @param  string $connector optional logical connector, default AND
     * @return Miner
     */
    public function openHaving($connector = self::LOGICAL_AND) {
      return $this->openCriteria($this->having, $connector);
    }

    /**
     * Add a closing bracket for nesting HAVING conditions.
     *
     * @return Miner
     */
    public function closeHaving() {
      return $this->closeCriteria($this->having);
    }

    /**
     * Add a HAVING condition.
     *
     * @param  string $column colum name
     * @param  mixed $value value
     * @param  string $operator optional comparison operator, default =
     * @param  string $connector optional logical connector, default AND
     * @param  bool|null $quote optional auto-escape value, default to global
     * @return Miner
     */
    public function having($column, $value, $operator = self::EQUALS, $connector = self::LOGICAL_AND, $quote = null) {
      return $this->criteria($this->having, $column, $value, $operator, $connector, $quote);
    }

  	/**
     * Add an AND HAVING condition.
     *
     * @param  string $column colum name
     * @param  mixed $value value
     * @param  string $operator optional comparison operator, default =
     * @param  bool|null $quote optional auto-escape value, default to global
     * @return Miner
     */
    public function andHaving($column, $value, $operator = self::EQUALS, $quote = null) {
      return $this->criteria($this->having, $column, $value, $operator, self::LOGICAL_AND, $quote);
    }

    /**
     * Add an OR HAVING condition.
     *
     * @param  string $column colum name
     * @param  mixed $value value
     * @param  string $operator optional comparison operator, default =
     * @param  bool|null $quote optional auto-escape value, default to global
     * @return Miner
     */
    public function orHaving($column, $value, $operator = self::EQUALS, $quote = null) {
      return $this->orCriteria($this->having, $column, $value, $operator, self::LOGICAL_OR, $quote);
    }

    /**
     * Add an IN WHERE condition.
     *
     * @param  string $column column name
     * @param  array $values values
     * @param  string $connector optional logical connector, default AND
     * @param  bool|null $quote optional auto-escape value, default to global
     * @return Miner
     */
    public function havingIn($column, array $values, $connector = self::LOGICAL_AND, $quote = null) {
      return $this->criteriaIn($this->having, $column, $values, $connector, $quote);
    }

    /**
     * Add a NOT IN HAVING condition.
     *
     * @param  string $column column name
     * @param  array $values values
     * @param  string $connector optional logical connector, default AND
     * @param  bool|null $quote optional auto-escape value, default to global
     * @return Miner
     */
    public function havingNotIn($column, array $values, $connector = self::LOGICAL_AND, $quote = null) {
      return $this->criteriaNotIn($this->having, $column, $values, $connector, $quote);
    }

    /**
     * Add a BETWEEN HAVING condition.
     *
     * @param  string $column column name
     * @param  mixed $min minimum value
     * @param  mixed $max maximum value
     * @param  string $connector optional logical connector, default AND
     * @param  bool|null $quote optional auto-escape value, default to global
     * @return Miner
     */
    public function havingBetween($column, $min, $max, $connector = self::LOGICAL_AND, $quote = null) {
      return $this->criteriaBetween($this->having, $column, $min, $max, $connector, $quote);
    }

    /**
     * Add a NOT BETWEEN HAVING condition.
     *
     * @param  string $column column name
     * @param  mixed $min minimum value
     * @param  mixed $max maximum value
     * @param  string $connector optional logical connector, default AND
     * @param  bool|null $quote optional auto-escape value, default to global
     * @return Miner
     */
    public function havingNotBetween($column, $min, $max, $connector = self::LOGICAL_AND, $quote = null) {
      return $this->criteriaNotBetween($this->having, $column, $min, $max, $connector, $quote);
    }

    /**
     * Merge this Miner's HAVING into the given Miner.
     *
     * @param  Miner $Miner to merge into
     * @return Miner
     */
    public function mergeHavingInto(Miner $Miner) {
      foreach ($this->having as $having) {
        // Handle open/close brackets differently than other criteria.
        if (array_key_exists('bracket', $having)) {
          if (strcmp($having['bracket'], self::BRACKET_OPEN) == 0) {
            $Miner->openHaving($having['connector']);
          }
          else {
            $Miner->closeHaving();
          }
        }
        else {
          $Miner->having($having['column'], $having['value'], $having['operator'],
                         $having['connector'], $having['quote']);
        }
      }

      return $Miner;
    }

    /**
     * Get the HAVING portion of the statement as a string.
     *
     * @param  bool $usePlaceholders optional use ? placeholders, default true
     * @param  bool $includeText optional include 'HAVING' text, default true
     * @return string HAVING portion of the statement
     */
    public function getHavingString($usePlaceholders = true, $includeText = true) {
      $statement = $this->getCriteriaString($this->having, $usePlaceholders, $this->havingPlaceholderValues);

      if ($includeText && $statement) {
        $statement = "HAVING " . $statement;
      }

      return $statement;
    }

    /**
     * Get the HAVING placeholder values.
     *
     * @return array HAVING placeholder values
     */
    public function getHavingPlaceholderValues() {
      return $this->havingPlaceholderValues;
    }

    /**
     * Add a column to ORDER BY.
     *
     * @param  string $column column name
     * @param  string $order optional order direction, default ASC
     * @return Miner
     */
    public function orderBy($column, $order = self::ORDER_BY_ASC) {
      $this->orderBy[] = array('column' => $column,
                               'order'  => $order);

      return $this;
    }

    /**
     * Merge this Miner's ORDER BY into the given Miner.
     *
     * @param  Miner $Miner to merge into
     * @return Miner
     */
    public function mergeOrderByInto(Miner $Miner) {
      foreach ($this->orderBy as $orderBy) {
        $Miner->orderBy($orderBy['column'], $orderBy['order']);
      }

      return $Miner;
    }

    /**
     * Get the ORDER BY portion of the statement as a string.
     *
     * @param  bool $includeText optional include 'ORDER BY' text, default true
     * @return string ORDER BY portion of the statement
     */
    public function getOrderByString($includeText = true) {
      $statement = "";

      foreach ($this->orderBy as $orderBy) {
        $statement .= $orderBy['column'] . " " . $orderBy['order'] . ", ";
      }

      $statement = substr($statement, 0, -2);

      if ($includeText && $statement) {
        $statement = "ORDER BY " . $statement;
      }

      return $statement;
    }

    /**
     * Set the LIMIT on number of rows to return with optional offset.
     *
     * @param  int|string $limit number of rows to return
     * @param  int|string $offset optional row number to start at, default 0
     * @return Miner
     */
    public function limit($limit, $offset = 0) {
      $this->limit['limit'] = $limit;
      $this->limit['offset'] = $offset;

      return $this;
    }

    /**
     * Merge this Miner's LIMIT into the given Miner.
     *
     * @param  Miner $Miner to merge into
     * @return Miner
     */
    public function mergeLimitInto(Miner $Miner) {
      if ($this->limit) {
        $Miner->limit($this->getLimit(), $this->getLimitOffset());
      }

      foreach ($this->end_option as $option) {
        $Miner->endOption($option);
      }

      return $Miner;
    }

    /**
     * Get the LIMIT on number of rows to return.
     *
     * @return int|string LIMIT on number of rows to return
     */
    public function getLimit() {
      return $this->limit['limit'];
    }

    /**
     * Get the LIMIT row number to start at.
     *
     * @return int|string LIMIT row number to start at
     */
    public function getLimitOffset() {
      return $this->limit['offset'];
    }

    /**
     * Get the LIMIT portion of the statement as a string.
     *
     * @param  bool $includeText optional include 'LIMIT' text, default true
     * @return string LIMIT portion of the statement
     */
    public function getLimitString($includeText = true) {
      $statement = "";

      if (!$this->limit) {
        return $statement;
      }

      $statement .= $this->limit['limit'];

      if ($this->limit['offset'] !== 0) {
        $statement .= " OFFSET " . $this->limit['offset'];
      }

      if ($includeText && $statement) {
        $statement = "LIMIT " . $statement;
      }

      return $statement;
    }

    /**
     * Whether this is a SELECT statement.
     *
     * @return bool whether this is a SELECT statement
     */
    public function isSelect() {
      return !empty($this->select);
    }

    /**
     * Whether this is an INSERT statement.
     *
     * @return bool whether this is an INSERT statement
     */
    public function isInsert() {
      return !empty($this->insert);
    }

    /**
     * Whether this is a REPLACE statement.
     *
     * @return bool whether this is a REPLACE statement
     */
    public function isReplace() {
      return !empty($this->replace);
    }

    /**
     * Whether this is an UPDATE statement.
     *
     * @return bool whether this is an UPDATE statement
     */
    public function isUpdate() {
      return !empty($this->update);
    }

    /**
     * Whether this is a DELETE statement.
     *
     * @return bool whether this is a DELETE statement
     */
    public function isDelete() {
      return !empty($this->delete);
    }

    /**
     * Merge this Miner into the given Miner.
     *
     * @param  Miner $Miner to merge into
     * @param  bool $overrideLimit optional override limit, default true
     * @return Miner
     */
    public function mergeInto(Miner $Miner, $overrideLimit = true) {
      if ($this->isSelect()) {
        $this->mergeSelectInto($Miner);
        $this->mergeFromInto($Miner);
        $this->mergeJoinInto($Miner);
        $this->mergeWhereInto($Miner);
        $this->mergeGroupByInto($Miner);
        $this->mergeHavingInto($Miner);
        $this->mergeOrderByInto($Miner);

        if ($overrideLimit) {
          $this->mergeLimitInto($Miner);
        }
      }
      elseif ($this->isInsert()) {
        $this->mergeInsertInto($Miner);
        $this->mergeSetInto($Miner);
      }
      elseif ($this->isReplace()) {
        $this->mergeReplaceInto($Miner);
        $this->mergeSetInto($Miner);
      }
      elseif ($this->isUpdate()) {
        $this->mergeUpdateInto($Miner);
        $this->mergeJoinInto($Miner);
        $this->mergeSetInto($Miner);
        $this->mergeWhereInto($Miner);

        // ORDER BY and LIMIT are only applicable when updating a single table.
        if (!$this->join) {
          $this->mergeOrderByInto($Miner);

          if ($overrideLimit) {
            $this->mergeLimitInto($Miner);
          }
        }
      }
      elseif ($this->isDelete()) {
        $this->mergeDeleteInto($Miner);
        $this->mergeFromInto($Miner);
        $this->mergeJoinInto($Miner);
        $this->mergeWhereInto($Miner);

        // ORDER BY and LIMIT are only applicable when deleting from a single
        // table.
        if ($this->isDeleteTableFrom()) {
          $this->mergeOrderByInto($Miner);

          if ($overrideLimit) {
            $this->mergeLimitInto($Miner);
          }
        }
      }

      return $Miner;
    }

    /**
     * Get the full SELECT statement.
     *
     * @param  bool $usePlaceholders optional use ? placeholders, default true
     * @return string full SELECT statement
     */
    protected function getSelectStatement($usePlaceholders = true) {
      $statement = "";

      if (!$this->isSelect()) {
        return $statement;
      }

      $statement .= $this->getSelectString();

      if ($this->from) {
        $statement .= " " . $this->getFromString();
      }

      if ($this->where) {
        $statement .= " " . $this->getWhereString($usePlaceholders);
      }

      if ($this->groupBy) {
        $statement .= " " . $this->getGroupByString();
      }

      if ($this->having) {
        $statement .= " " . $this->getHavingString($usePlaceholders);
      }

      if ($this->orderBy) {
        $statement .= " " . $this->getOrderByString();
      }

      if ($this->limit) {
        $statement .= " " . $this->getLimitString();
      }

      if($this->end_option) {
	$statement .= " " . $this->getEndOptionsString();
      }

      return $statement;
    }

    /**
     * Get the full INSERT statement.
     *
     * @param  bool $usePlaceholders optional use ? placeholders, default true
     * @return string full INSERT statement
     */
    protected function getInsertStatement($usePlaceholders = true) {
      $statement = "";

      if (!$this->isInsert()) {
        return $statement;
      }

      $statement .= $this->getInsertString();

      if ($this->set) {
        $statement .= " " . $this->getSetString($usePlaceholders);
      }

	  if($this->onDuplicateKeyValue){
		 $statement .= " ON DUPLICATE KEY UPDATE " . $this->onDuplicateKeyValue->getSetString($usePlaceholders, false);
	  }

      if($this->end_option) {
	$statement .= " " . $this->getEndOptionsString();
      }

      return $statement;
    }

    /**
     * Get the full REPLACE statement.
     *
     * @param  bool $usePlaceholders optional use ? placeholders, default true
     * @return string full REPLACE statement
     */
    protected function getReplaceStatement($usePlaceholders = true) {
      $statement = "";

      if (!$this->isReplace()) {
        return $statement;
      }

      $statement .= $this->getReplaceString();

      if ($this->set) {
        $statement .= " " . $this->getSetString($usePlaceholders);
      }

      if($this->end_option) {
	$statement .= " " . $this->getEndOptionsString();
      }

      return $statement;
    }

    /**
     * Get the full UPDATE statement.
     *
     * @param  bool $usePlaceholders optional use ? placeholders, default true
     * @return string full UPDATE statement
     */
    protected function getUpdateStatement($usePlaceholders = true) {
      $statement = "";

      if (!$this->isUpdate()) {
        return $statement;
      }

      $statement .= $this->getUpdateString();

      if ($this->set) {
        $statement .= " " . $this->getSetString($usePlaceholders);
      }

      if ($this->where) {
        $statement .= " " . $this->getWhereString($usePlaceholders);
      }

      // ORDER BY and LIMIT are only applicable when updating a single table.
      if (!$this->join) {
        if ($this->orderBy) {
          $statement .= " " . $this->getOrderByString();
        }

        if ($this->limit) {
          $statement .= " " . $this->getLimitString();
        }
      }

      if($this->end_option) {
	$statement .= " " . $this->getEndOptionsString();
      }

      return $statement;
    }

    /**
     * Get the full DELETE statement.
     *
     * @param  bool $usePlaceholders optional use ? placeholders, default true
     * @return string full DELETE statement
     */
    protected function getDeleteStatement($usePlaceholders = true) {
      $statement = "";

      if (!$this->isDelete()) {
        return $statement;
      }

      $statement .= $this->getDeleteString();

      if ($this->from) {
        $statement .= " " . $this->getFromString();
      }

      if ($this->where) {
        $statement .= " " . $this->getWhereString($usePlaceholders);
      }

      // ORDER BY and LIMIT are only applicable when deleting from a single
      // table.
      if ($this->isDeleteTableFrom()) {
        if ($this->orderBy) {
          $statement .= " " . $this->getOrderByString();
        }

        if ($this->limit) {
          $statement .= " " . $this->getLimitString();
        }
      }

      if($this->end_option) {
	$statement .= " " . $this->getEndOptionsString();
      }

      return $statement;
    }

    /**
     * Get the full SQL statement.
     *
     * @param  bool $usePlaceholders optional use ? placeholders, default true
     * @return string full SQL statement
     */
    public function getStatement($usePlaceholders = true) {
      $statement = "";

      if ($this->isSelect()) {
        $statement = $this->getSelectStatement($usePlaceholders);
      }
      elseif ($this->isInsert()) {
        $statement = $this->getInsertStatement($usePlaceholders);
      }
      elseif ($this->isReplace()) {
        $statement = $this->getReplaceStatement($usePlaceholders);
      }
      elseif ($this->isUpdate()) {
        $statement = $this->getUpdateStatement($usePlaceholders);
      }
      elseif ($this->isDelete()) {
        $statement = $this->getDeleteStatement($usePlaceholders);
      }

      return $statement;
    }

    /**
     * Get all placeholder values (SET, WHERE, and HAVING).
     *
     * @return array all placeholder values
     */
    public function getPlaceholderValues() {
      return array_merge($this->getSetPlaceholderValues(),
                         $this->getWherePlaceholderValues(),
                         $this->getHavingPlaceholderValues(),
						 !empty($this->onDuplicateKeyValue) ? $this->onDuplicateKeyValue->getSetPlaceholderValues() : Array()
			  );
    }

    /**
     * Execute the statement using the PDO database connection.
     *
     * @return PDOStatement|false executed statement or false if failed
     */
    public function execute() {
      $this->_rowsFound = 0;
      if (class_exists('database')) {
        //Check if there is a common Database Class...
        $database = database::getInstance();
        $resultset = $database->query($this->getStatement(), $this->getPlaceholderValues());
        if ($resultset === FALSE || $resultset === NULL) {
            if ($database->lastError) {
                $lastError = $database->lastError;
                $retry = FALSE;
                if (!is_string($lastError))
                    $lastError = print_r($lastError, true);
                if ((strpos($lastError, '1054') !== FALSE) &&
                    (strpos($lastError, 'syscreat') !== FALSE) ) {
                    $database->query("ALTER TABLE {$this->insert} ADD COLUMN `syscreator` INT(11) default NULL", array() );
                    $database->query("ALTER TABLE {$this->insert} ADD COLUMN `syscreated` TIMESTAMP NOT NULL  default '0000-00-00 00:00:00'", array());
                    $retry = TRUE;
                }
                if ((strpos($lastError, '1054') !== FALSE) &&
                    (strpos($lastError, 'sysmodifi') !== FALSE) ) {
                    $database->query("ALTER TABLE {$this->update} ADD COLUMN `sysmodifier` INT(11) default NULL", array());
                    $database->query("ALTER TABLE {$this->update} ADD COLUMN `sysmodified` TIMESTAMP NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP", array());
                    $retry = TRUE;
                }
                if ($retry)
                    $resultset = $database->query($this->getStatement(), $this->getPlaceholderValues());
            }
        }
        if (in_array('SQL_CALC_FOUND_ROWS',$this->option)) {
          try {
            $dummy = $database->query("SELECT FOUND_ROWS() as rowcount", array());
            if (is_array($dummy) && count($dummy))
              $this->_rowsFound = $dummy[0]['rowcount'];
          }
          catch(Exception $err){}
        }
        if (!$this->_rowsFound || $this->_rowsFound == 1)
          $this->_rowsFound = count($resultset);
        return $resultset;
      }
      elseif (class_exists('PDO') && defined('PDO::ATTR_DEFAULT_FETCH_MODE')) {
        //Else try to execute this by PDO; only if PDO is compiled i PHP.
        $PdoConnection = $this->getPdoConnection();

        // Without a PDO database connection, the statement cannot be executed.
        if (!$PdoConnection) {
          return false;
        }

        $statement = $this->getStatement();

        // Only execute if a statement is set.
        if ($statement) {
          $PdoStatement = $PdoConnection->prepare($statement);
          $result = $PdoStatement->execute($this->getPlaceholderValues());
          if (!$result) {
            if ($PdoStatement->errorInfo) {
                $lastError = $PdoStatement->errorInfo;
                $retry = FALSE;
                if (!is_string($lastError))
                    $lastError = print_r($lastError, true);
                if ((strpos($lastError, 'syscreat') !== FALSE) ) {
                    $tmpStmt = $PdoConnection->prepare("ALTER TABLE {$this->insert} ADD COLUMN `syscreator` INT(11) default NULL");
                    $tmpStmt->execute();
                    $tmpStmt = $PdoConnection->prepare("ALTER TABLE {$this->insert} ADD COLUMN `syscreated` TIMESTAMP NOT NULL  default '0000-00-00 00:00:00'");
                    $tmpStmt->execute();
                    $retry = TRUE;
                }
                if ((strpos($lastError, 'sysmodifi') !== FALSE) ) {
                    $tmpStmt = $PdoConnection->prepare("ALTER TABLE {$this->update} ADD COLUMN `sysmodifier` INT(11) default NULL");
                    $tmpStmt->execute();
                    $tmpStmt = $PdoConnection->prepare("ALTER TABLE {$this->update} ADD COLUMN `sysmodified` TIMESTAMP NOT NULL  default '0000-00-00 00:00:00'");
                    $tmpStmt->execute();
                    $retry = TRUE;
                }
                if ($retry)
                    $resultset = $PdoStatement->execute($this->getPlaceholderValues());
            }
          }

          if (in_array('SQL_CALC_FOUND_ROWS',$this->option)) {
            try {
              $PdoFoundRows = $PdoConnection->prepare("SELECT FOUND_ROWS() AS rowcount");
              $PdoFoundRows->execute();
              $this->_rowsFound = intval($PdoFoundRows->fetchColumn());
              if (!is_numeric($this->_rowsFound) || $this->_rowsFound <= 1) {
                $this->_rowsFound = $PdoStatement->rowCount();
              }
            }
            catch(Exception $err){}
          }
          return $PdoStatement;
        }
        else {
          return false;
        }
      }
      else {
        return false;
      }
    }

    /**
     * Executes the query, but only returns the row count
     *
     * @return int|false count of rows or false if no connection, or not a select query
     */
    public function queryGetRowCount(){
      if($this->getPdoConnection()){

        // Save the existing select, order and limit arrays
        $old_select = $this->select;
        $old_order = $this->orderBy;
        $old_limit = $this->limit;

        // Reset the values
        $this->select = $this->orderBy = $this->limit = Array();

        // Add the new count select
        $this->select("COUNT(*)");

        // Run the query
        $result = $this->query();

        // Restore the values
        $this->select   = $old_select;
        $this->orderBy  = $old_order;
        $this->limit    = $old_limit;

        // Fetch the count from the query result
        if($result){
          $c = $result->fetchColumn();
          $result->closeCursor();
          return $c;
        }
      }
      return false;
    }


    public function rowsFound(){
      return $this->_rowsFound;
    }


    /**
     * Get the full SQL statement without value placeholders.
     *
     * @return string full SQL statement
     */
    public function __toString() {
      return $this->getStatement(false);
    }

  }

?>
