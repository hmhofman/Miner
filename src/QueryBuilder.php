<?php
require_once(__DIR__ . "/Miner.php");
class QueryBuilder extends Miner{
	

	public function getQueryString($usePlaceholders = true){
		return $this->getStatement($usePlaceholders);
	}

	/**
	* Adds a Lock in Share Mode to the query
	* @return QueryBuilder
	*/
	public function lockInShareMode(){
		$this->option('LOCK IN SHARE MODE');
		return $this;
	}

	/**
	* Add Lock for Update to the query
	* @return QueryBuilder
	*/
	public function lockForUpdate(){
		$this->option('FOR UPDATE');
		return $this;
	}    


}

?>