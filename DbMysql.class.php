<?php
/**
 * Mysql数据库操作类 v2.1
 * 2017.10.12 by Aboc QQ:9986584
 * 增加文件缓存
 *
 */
class DbMysql {
	/**
	 * 在数据库操作中,只对数据库操作有影响的字符做转义
	 * 当此类正常后,所有数据操作 @
	 */
	/*
	 * 数据库连接句柄
	 */
	private $_Db = NULL;
	/*
	 * 是否持续连接 0.1
	 */
	private $_pconnect = 0;
	/*
	 * 编码
	 */
	private $_charset = 'utf8';


	/*
	 *最后一次插入的ID
	 */
	private $_lastId = 0;
	/*
	 * 默认数据库配置
	 */
	private $_config = array ('dbhost' => 'localhost', 'dbuser' => 'root', 'dbpass' => 'root', 'dbname' => 'test');


	/**
	 * sql语句
	 *
	 * @var unknown_type
	 */
	private $_sql = '';
	
	private $_sqls = array();

	private $_error_sql = '';

	/* 开启事务 */
	var $_commit        = false;
	/* 事务状态，发现一个错的就将它修改为false */
	var $_commit_hd     = true;
	/**
	 *
	 * @var type 备份用
	 */
	private $_backupsql = '';
	/**
	 * 初始连接数据库
	 */
	function __construct($config,$pconnect=0) {
		if (empty($config)) $config = array();
		$this->checkConfig ( $config );
		$this->_pconnect = $pconnect;
		$this->connect ();
		$this->query ( 'set names ' . $this->_charset ); //设置编码
	}
	/**
	 * 判断config变量
	 *
	 * @param unknown_type $config
	 */
	private function checkConfig($config) {
		foreach ( $config as $key => $value ) {
			$this->_config [$key] = empty ( $value ) ? $this->_config [$key] : $value;
		}
		//return $this->_config;
	}
	/*
	 * 连接数据库
	 */
	private function connect() {
//		print_r($this->_config);
		if ($this->_pconnect) {
			$this->_Db = mysqli_pconnect ( $this->_config ['dbhost'], $this->_config ['dbuser'], $this->_config ['dbpass'] ) or die ( '数据库连接失败' . mysqli_errno ($this->_Db) );
		} else {
			$this->_Db = mysqli_connect ( $this->_config ['dbhost'], $this->_config ['dbuser'], $this->_config ['dbpass'] ) or die ( '数据库连接失败' . mysqli_errno ($this->_Db) );
		}
		if ($this->_Db != NULL) {
			mysqli_select_db ($this->_Db, $this->_config ['dbname']) or die ( '数据库' . $this->_config ['dbname'] . '不存在' );
		}
	}
	/**
	 * 将变量的单引号或双引号转义
	 *
	 * @param unknown_type $string
	 */
	private function strtag($string1) {
		if (is_array ( $string1 )) {
			$stringnew = array();
			foreach ( $string1 as $key => $value ) {
				$stringnew [$this->strtag ( $key )] = $this->strtag ( $value );
			}
		} else {
			$stringnew = mysqli_real_escape_string ($this->_Db, $string1 );
		}
		return $stringnew;
	}
	/**
	 * 将数组转化为SQL接受的条件样式
	 *
	 * @param unknown_type $array
	 */
	private function _changeArray($array) {
		if(is_string($array)){
			return $array;
		}
		$array = $this->strtag ( $array ); //转义
		$str = '';
		foreach ( $array as $key => $value ) {
			$str .= empty ( $str ) ? "`" . $key . "`='" . $value."'" : ", `" . $key . "`='" . $value."'";
		}
		return $str;
	}
	/**
	 * 执行查询语句
	 * @return bool
	 */
	public function query($sql) {
		$this->_sql = trim($sql);
		$this->_sqls[] = $sql;
		if (! $result = mysqli_query ( $this->_Db,$this->_sql)) {
			if($this->_commit && $this->_sql != "ROLLBACK"){
				//开启事务的状态下执行这个
				$this->_error_sql = $this->_sql;
				$this->_commit_hd = false;
				$this->commit();
			}
			if(IS_DEBUG){
				$str = $this->_sql.'<br>'.mysqli_error($this->_Db).'<br>';
				//$this->createErrorLog($sql);
				die($str);
			} else {
				$date = date("Y-m-d H:i:s");
				$thisurl = str::getThisUrl();
				$error = mysqli_error($this->_Db);
				$msg = "$date $thisurl $error $this->_sql\r\n";
				//log_write($msg,"mysql.log");
				die("系统出现一个错误");
			}
		} else {
			return $result;
		}
	}
	/**
	 * 插入记录
	 *
	 */
	public function insert($table, $array) {
		if(!is_array($array))return false;
		$array = $this->strtag ( $array ); //转义
		$str = '';
		$val = '';
		foreach ($array as $key=>$value){
			$str .= ($str != '')?",`$key`":"`$key`";
			$val .= ($val != '')?",'$value'":"'$value'";
		}
		$sql = 'insert into '.$this->_deal_table($table).' ('.$str. ') values('.$val.')';
		if ($this->query ( $sql )) {
			$this->lastId();
			return $this->_lastId?$this->_lastId:true;
		} else {
			return false;
		}
	}


	/**
	 * 替换并插入
	 * @param unknown_type $table
	 * @param unknown_type $array
	 */
	public function replaceInsert($table, $array) {
		if(!is_array($array))return false;
		$array = $this->strtag ( $array ); //转义
		$str = '';
		$val = '';
		foreach ($array as $key=>$value){
			$str .= ($str != '')?",`$key`":"`$key`";
			$val .= ($val != '')?",'$value'":"'$value'";
		}
		$sql = 'replace into '.$this->_deal_table($table).' ('.$str. ') values('.$val.')';
		if ($this->query ( $sql )) {
			$this->lastId();
			return $this->_lastId?$this->_lastId:true;
		} else {
			return false;
		}
	}

	/**
	 * 批量插入记录
	 *
	 * @param $table 表名
	 * @param $batchArray 批量数据 ,二维数组,健名必需相同,否则不能插入
	 */
	public function insertBatch($table,$batchArray){
		if(!is_array($batchArray))return false;
		$str = '';
		$val = '';
		$vals = array();
		foreach ($batchArray as $keys=>$row){
			if(!is_array($row))return false;
			foreach ($row as $key=>$value){
				if($keys == 0)$str .= ($str != '')?",`$key`":"`$key`";
				$val .= ($val != '')?",'$value'":"'$value'";
			}
			$vals[$keys] = '('.$val.')';
			$val = '';
		}
		if(!$vals){
			return false;
		}
		$vals = implode(',',$vals);
		$sql = 'insert into '.$this->_deal_table($table).' ('.$str. ') values '.$vals;
		if ($this->query ( $sql )) {
			$this->lastId();
			return $this->_lastId?$this->_lastId:true;
		} else {
			return false;
		}

	}
	/**
	 * 更新记录
	 *
	 */
	public function update($table, $array, $where = NULL) {
		if ($where == NULL) {
			$sql = 'update '.$this->_deal_table($table).' set ' . $this->_changeArray ( $array );
		} else {
			$sql = 'update '.$this->_deal_table($table).' set ' . $this->_changeArray ( $array ) . ' where ' . $where;
		}
		if ($res = $this->query ( $sql )) {
			return mysqli_affected_rows($this->_Db);
		} else {
			return false;
		}
	}
	/**
	 * 删除记录
	 *
	 */
	public function delete($table, $where = NULL) {
		if ($where == NULL) {
			$sql = 'delete from '.$this->_deal_table($table);
		} else {
			$sql = 'delete from '.$this->_deal_table($table).' where ' . $where;
		}
		if ($this->query ( $sql )) {
			return mysqli_affected_rows($this->_Db);
		} else {
			return false;
		}
	}

	/**
	 * 处理table
	 * @param $table
	 *
	 * @return string
	 */
	private function _deal_table($table){
		if(stripos($table,'.')!== false){
			return $table;
		} else {
			return "`$table`";
		}
	}
	/**
	 * 获取一条记录
	 *
	 */
	public function fetchRow($sql,$cacheTime=0,$cacheId='') {
		if($content = $this->checkCache($sql,$cacheTime,$cacheId)){
			return $content;
		} else{
			$reult = $this->query ( $sql );
			$row = mysqli_fetch_assoc ( $reult );
			if(!empty($row)){
				foreach ($row as $key=>$value){
					$row[$key] = stripslashes($value);
				}
			}
			if($cacheTime)$this->createCache($sql,$row,$cacheId,$cacheTime);
			return $row;
		}
	}
	/**
	 * 获取所有记录/用的mysql_fetch_assoc循环
	 *
	 */
	public function fetchAll($sql,$cacheTime=0,$cacheId='',$index = false) {
		if($content = $this->checkCache($sql,$cacheTime,$cacheId)){
			return $content;
		} else{
			$result = $this->query ( $sql );
			if ($result !== false) {
				$arr = array ();
				while ( $row = mysqli_fetch_assoc ( $result ) ) {
					if(!empty($row)){
						foreach ($row as $key=>$value){
							$row[$key] = stripslashes($value);
						}
					}
					if($index && isset($row[$index])){
						$arr [$row[$index]] = $row;
					} else {
						$arr [] = $row;
					}
				}
				if($cacheTime)$this->createCache($sql,$arr,$cacheId,$cacheTime);
				return $arr;
			} else {
				return array();
			}
		}
	}

	/**
	 * 获取最后一次影响的Id
	 *
	 */
	public function lastId() {
		$this->_lastId = mysqli_insert_id ( $this->_Db );
		return $this->_lastId;
	}
	/**
	 * 获取符合条件的记录数
	 *
	 */
	public function fetchNum($sql) {
		$reult = $this->query ( $sql );
		$num = mysqli_num_rows ( $reult );
		return $num;
	}

	/**
	 * 通过数组获取where字符串条件
	 *
	 * @param $data
	 */
	public function getWhere($data){
		$str = '';
		foreach ( $data as $key => $value ) {
			$value = addslashes($value);
			$str .= empty ( $str ) ? "`" . $key . "`='" . $value."'" : " AND `" . $key . "`='" . $value."'";
		}
		return $str;
	}

	/**
	 * 获取第一列
	 *
	 * @param        $sql
	 * @param int    $cacheTime
	 * @param string $cacheId
	 *
	 * @return bool|mixed
	 */
	public function fetchCol($sql,$cacheTime=0,$cacheId=''){
		if($content = $this->checkCache($sql,$cacheTime,$cacheId)){
			return $content;
		} else {
			$result = $this->query( $sql );
			$row = mysqli_fetch_array($result, MYSQLI_NUM);
			$data = $row[0];
			if($cacheTime)$this->createCache($sql,$data,$cacheId,$cacheTime);
			return $data;
		}
	}

	/**
	 * 输出适合的where语句
	 */
	public function quoteInto($string,$value ) {
		$value = $this->strtag($value);
		if(is_numeric($value)){
			$string = str_replace('?',$value,$string);
		}else{
			$string = str_replace('?',"'".$value."'",$string);
		}
		return $string;
	}

	/**
	 * 数据数据库所用大小
	 *
	 * @param unknown_type $dbname
	 * @return unknown
	 */
	public function getSqlSize($dbname){
		$sql = "SHOW TABLE STATUS from $dbname";
		$rows = $this->fetchAll($sql);
		$total = 0;
		foreach ($rows as $row){
			$total +=  $row['Data_length'];
			$total +=  $row['Index_length'];
		}
		return round($total/(1024*1024),2);
	}

	/**
	 * 判断缓存文件是否有效,如果有效，则返回缓存内容
	 */
	private function checkCache($sql,$cacheTime = 0,$cacheId=''){
		if($cacheTime == 0){
			return false;
		} else {
			$tmp = $this->createFilename($sql,$cacheId);
			return eihoo::cache()->get($tmp);
		}

	}

	/**
	 * 生成缓存
	 */
	private function createCache($sql,$data,$cacheId='',$cacheTime=3600){
		$tmp = $this->createFilename($sql,$cacheId);
		return eihoo::cache()->set($tmp,$data,$cacheTime);
	}

	/**
	 * 根据sql语句生成文件名及路径
	 */
	private function createFilename($sql,$cacheId=''){
		if(!empty($cacheId))$sql = $cacheId;
		return md5($sql);
	}

	/**
	 * 清除缓存
	 *
	 * 条件为空则清除所有缓存
	 *
	 * @return DbMysql
	 */
	public function clearCache($sql='',$cacheId=''){
		if(!empty($sql) || !empty($cacheId)){
			$tmp = $this->createFilename($sql,$cacheId);
			eihoo::cache()->delete($tmp);
		}
		//清除所有缓存
		else{
			eihoo::cache()->clear();
		}
		return true;
	}


	/**
	 * 获取最后一次执行的sql语句
	 *
	 */
	public function getLastSql(){
		return $this->_sql;
	}

	/**
	 * 开启事务
	 *
	 * @return type
	 */
	function begin(){
		if($this->_commit){
			die("出错啦，多次使用事务");
		}
		$this->_commit = true;
		$this->query("SET AUTOCOMMIT=0");
		$this->query("BEGIN");
		return;
	}

	/**
	 * 提交事务
	 * @return boolean
	 */
	function commit(){
		if(!$this->_commit){
			return false;
		}
		if($this->_commit_hd){
			$this->query("COMMIT");
			$this->query("SET AUTOCOMMIT=1");
			$this->_commit = false;
			$this->_commit_hd = false;
			return true;
		}else{
			$error_sql = $this->_error_sql;
			$this->query("ROLLBACK");
			die("ROLLBACK SQL Error!".(IS_DEBUG?$error_sql:''));
			return false;
		}
	}

	/**
	 * 判断是否为update/insert/delete操作
	 *
	 * @param $sql
	 *
	 * @return bool
	 */
	private function _check_write_sql($sql){
		if(strtoupper(substr($sql, 0,7)) == 'UPDATE ' ||
		   strtoupper(substr($sql, 0,7)) == 'INSERT ' ||
		   strtoupper(substr($sql, 0,7)) == 'DELETE '){
			return true;
		}
		return false;
	}
	
	/**
	 * 获取最后一次影响的记录数
	 * @return type
	 */
	function fetchChangeRow() {
		return mysqli_affected_rows($this->_Db);
	}

    /**
     * 锁定单表
     *
     * @param $table
     * @param string $type
     */
	function lockTable($table,$type="write"){
	    $this->query("LOCK TABLES $table $type");
    }

    /**
     * 锁定多表
     * lockTables("table1","write","table2","read")
     *
     * @param table type table type
     */
    function lockTables(){
        $args = func_get_args();
        $num = count($args);
        if($num == 0){
            return;
        }
        if($num%2 !=0){
            die("锁定表参数有误");
        }
        $tables = array();
        for($i = 0; $i < $num; $i = $i+ 2){
            $tables[] = $args[$i]." ".$args[$i+1];
        }
        $this->query("LOCK TABLES ".join(",", $tables));
    }

    /**
     * 解锁表
     */
    function unlockTable(){
	    $this->query("UNLOCK TABLES");
    }



	/**
	 * 释放查询结果
	 */
	private function free() {
		mysqli_free_result($this->_Db);
	}
	/**
	 *
	 */
	function __destruct() {
//		$this->free();
	}
}
