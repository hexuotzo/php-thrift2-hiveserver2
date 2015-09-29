<?php

$GLOBALS['THRIFT_ROOT'] = dirname(__FILE__).'/Thrift';
require_once $GLOBALS['THRIFT_ROOT'] . '/packages/hive_service/TCLIService.php';
require_once $GLOBALS['THRIFT_ROOT'] . '/transport/TSocket.php';
require_once $GLOBALS['THRIFT_ROOT'] . '/transport/TSaslClientTransport.php';
require_once $GLOBALS['THRIFT_ROOT'] . '/protocol/TBinaryProtocol.php';

class HiveServer2
{
	public $maxRows = 10000;
	protected $_hasMoreRows = true;
	
	public function connect() {
		$this->_socket = new TSocket('localhost', 10000);
		$this->_socket->setSendTimeout(10000);
		$this->_socket->setRecvTimeout(10000);
		$this->_transport = new TSaslClientTransport($this->_socket);
		$this->_protocol  = new TBinaryProtocol($this->_transport);
		$this->_client = new TCLIServiceClient($this->_protocol);
		$this->_transport->open();
		$openSessionReq = new TOpenSessionReq(array(
			'username' => 'username',
			'password' => 'password',
			'configuration' => null
		));
		$this->_openSessionResp = $this->_client->OpenSession($openSessionReq);
		$this->maxRows = $this->maxRows;
	}

	public function execute($sql) {
		$query = new TExecuteStatementReq(array(
				"sessionHandle" => $this->_openSessionResp->sessionHandle,
				"statement" 	=> $sql,
				"confOverlay" 	=> null
		));
		$this->_operationHandle = $this->_client->ExecuteStatement($query);
	}
	
	public function fetch() {
		$rows = array();
		while ($this->_hasMoreRows) {
			$rows += $this->fetchSet();
		}
		return $rows;
	}
	
	protected function fetchSet() {
		$rows = array();
		$fetchReq = new TFetchResultsReq (array(
			'operationHandle' => $this->_operationHandle->operationHandle,
			'orientation' => TFetchOrientation::FETCH_NEXT,
			'maxRows' => $this->maxRows,
		));
        $metaReq = new TGetResultSetMetadataReq (array(
                'operationHandle' => $this->_operationHandle->operationHandle,
                'orientation' => TFetchOrientation::FETCH_NEXT,
                'maxRows' => $this->maxRows,
        ));
		return $this->_fetch($fetchReq, $metaReq);
	} 
	
	protected function _fetch($fetchReq, $metaReq) {
		$resultsRes = $this->_client->FetchResults($fetchReq);
        $metaRes = $this->_client->GetResultSetMetadata($metaReq);
		$rowData = array();
		foreach ($resultsRes->results->rows as $key => $row) {
			$rows = array();
			foreach ($row->colVals as $n => $colValue) {
			    $k = $metaRes->schema->columns[$n]->columnName;
                            $rows[$k] =  trim($this->_getValue($colValue));
			}
			$rowData[] = $rows;
		}
		if (0 == count($resultsRes->results->rows)) {
			$this->_hasMoreRows = false;
		}
		return $rowData;
	}
	
	protected function _getValue($colValue) {
		if ($colValue->boolVal)
			return $colValue->boolVal->value;
		elseif ($colValue->byteVal)
			return $colValue->byteVal->value;
		elseif ($colValue->i16Val)
			return $colValue->i16Val->value;
		elseif ($colValue->i32Val)
			return $colValue->i32Val->value;
		elseif ($colValue->i64Val)
			return $colValue->i64Val->value;
		elseif ($colValue->doubleVal)
			return $colValue->doubleVal->value;
		elseif ($colValue->stringVal)
			return $colValue->stringVal->value;
	}
	
	function __destruct() {
		if ($this->_operationHandle) {
			$req = new TCloseOperationReq(array(
				'operationHandle' => $this->_operationHandle->operationHandle
			));
			$this->_client->CloseOperation($req);
		}
	}
}


#example
$hive = new HiveServer2();
$hive->connect();
$hive->execute('select * from tablename limit 5');
#$hive->execute('desc tablename');
$b = $hive->fetch();
print_r($b);
