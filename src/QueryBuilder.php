<?php
require_once(__DIR__ . "/Miner.php");
class QueryBuilder extends Miner{
	

	public function getQueryString($usePlaceholders = true){
		return $this->getStatement($usePlaceholders);
	}
	
	public function query(){
		return $this->execute();
	}

	/**
	* Adds a Lock in Share Mode to the query
	* @return QueryBuilder
	*/
	public function lockInShareMode(){
		$this->endOption('LOCK IN SHARE MODE');
		return $this;
	}

	/**
	* Add Lock for Update to the query
	* @return QueryBuilder
	*/
	public function lockForUpdate(){
		$this->endOption('FOR UPDATE');
		return $this;
	}    


}

?>