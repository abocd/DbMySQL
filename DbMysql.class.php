<?php
/**
 * Mysql数据库操作类 v2.0
 * 2015.5.6 by Aboc QQ:9986584
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
	private $_charset = 'gbk';
	
	
	/*
	 *最后一次插入的ID
	 */
	 private $_lastId = 0;

	/*
	 * 默认数据库配置
	 */
	private $_config = array ('dbhost' => 'localhost', 'dbuser' => 'root', 'dbpass' => 'root', 'dbname' => 'test');
	
	/**
	 * 缓存路径
	 */
	private $_cachePath = '';
	
	/**
	 * sql语句
	 *
	 * @var unknown_type
	 */
	private $_sql = '';
        
        /**
         *
         * @var type 备份用
         */
        private $_backupsql = '';

	/**
	 * 初始连接数据库
	 */
	function __construct($config,$pconnect=0,$cachepath='cache') {
		if (empty($config)) $config = array();
		$this->checkConfig ( $config );
		$this->_pconnect = $pconnect;
		$this->connect ();
		$this->query ( 'set names ' . $this->_charset ); //设置编码
		$this->_cachePath = $cachepath;
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
			$this->_Db = mysql_pconnect ( $this->_config ['dbhost'], $this->_config ['dbuser'], $this->_config ['dbpass'] ) or die ( '数据库连接失败' . mysql_errno () );
		} else {
			$this->_Db = mysql_connect ( $this->_config ['dbhost'], $this->_config ['dbuser'], $this->_config ['dbpass'] ) or die ( '数据库连接失败' . mysql_errno () );
		}
		if ($this->_Db != NULL) {
			mysql_select_db ( $this->_config ['dbname'], $this->_Db ) or die ( '数据库' . $this->_config ['dbname'] . '不存在' );
		}
	}

	/**
	 * 将变量的单引号或双引号转义
	 *
	 * @param unknown_type $string
	 */
	private function strtag($string1) {
			if (is_array ( $string1 )) {
				foreach ( $string1 as $key => $value ) {
					$stringnew [$this->strtag ( $key )] = $this->strtag ( $value );
				}
			} else {
				//在此做转义,对单引号
				//TODO 好像 %也要转义吧?
				//$string = iconv("gbk","gbk",$string);
				$stringnew = mysql_real_escape_string ( $string1 );
//				$stringnew = get_magic_quotes_gpc()?$string:addslashes ( $string1 );
//				$stringnew=str_replace(array("'",'"'),array("\'",'\"'),$string1);
			}
		return $stringnew;
	}

	/**
	 * 将数组转化为SQL接受的条件样式
	 *
	 * @param unknown_type $array
	 */
	private function chageArray($array) {
		//MYSQL支持insert into joincart set session_id = 'dddd',product_id='44',number='7',jointime='456465'
		//所以更新和插入可以使用同一组数据
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
		//echo $sql.'<br>';
        $this->_sql = $sql;
		if (! $result = mysql_query ( $sql, $this->_Db)) {
			if(UC_DBUSER == 'root'){
			    echo $sql.'<br>'.mysql_error().'<br>';
			    //$this->createErrorLog($sql);
			    die ( '数据库出错' );
			} else {
				 $subject = date("Y-m-d H:i:s")."数据库查询出错";
				 $thisurl = str::getThisUrl();
				 $error = mysql_error();
    			 $content = <<<EOT
数据库查询出错,详细如下:<br />
$thisurl <br />
$error <br />
$sql
EOT;
                 $username = '风子';
                 if (function_exists('sendMail')){
                 	sendMail('9986584@qq.com',$username,$subject,$content);
                    die(':(&nbsp;数据库查询出错,已经通知管理员,请稍后重试');
                 }
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
		$sql = 'insert into `' . $table . '` ('.$str. ') values('.$val.')';
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
		$sql = 'replace into `' . $table . '` ('.$str. ') values('.$val.')';
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
		$vals = implode(',',$vals);
		$sql = 'insert into `' . $table . '` ('.$str. ') values '.$vals;
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
			$sql = 'update `' . $table . '` set ' . $this->chageArray ( $array );
		} else {
			$sql = 'update `' . $table . '` set ' . $this->chageArray ( $array ) . ' where ' . $where;
		}
		if ($res = $this->query ( $sql )) {
			return $res;
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
			$sql = 'delete from `' . $table . '`';
		} else {
			$sql = 'delete from `' . $table . '` where ' . $where;
		}
		if ($this->query ( $sql )) {
			return true;
		} else {
			return false;
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
			$row = mysql_fetch_assoc ( $reult );
			if(!empty($row)){
				foreach ($row as $key=>$value){
					$row[$key] = stripslashes($value);
				}
			}
		if($cacheTime)$this->createCache($sql,$row,$cacheId);
		return $row;
		}
	}

	/**
	 * 获取所有记录/用的mysql_fetch_assoc循环
	 *
	 */
	public function fetchAll($sql,$cacheTime=0,$cacheId='') {
		if($content = $this->checkCache($sql,$cacheTime,$cacheId)){
			return $content;
		} else{
			$result = $this->query ( $sql );
			if ($result !== false) {
				$arr = array ();
				while ( $row = mysql_fetch_assoc ( $result ) ) {
					if(!empty($row)){
						foreach ($row as $key=>$value){
							$row[$key] = stripslashes($value);
						}
					}
					$arr [] = $row;
				}
				if($cacheTime)$this->createCache($sql,$arr,$cacheId);
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
		$this->_lastId = mysql_insert_id ( $this->_Db );
		return $this->_lastId;
	}

	/**
	 * 获取符合条件的记录数
	 *
	 */
	public function fetchNum($sql) {
		$reult = $this->query ( $sql );
		$num = mysql_num_rows ( $reult );
		return $num;
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
		//不缓存，直接返回
		return false;
		if($cacheTime == 0){
			return false;
		} else {
			$tmp = $this->createFilename($sql,$cacheId);
			if(file_exists($tmp['path'].$tmp['filename'])&&(filemtime($tmp['path'].$tmp['filename'])+$cacheTime)>time()){
				$content = file_get_contents($tmp['path'].$tmp['filename']);
				return !empty($content)?unserialize($content):array();
			} else{
				return false;
			}
		}
		
	}
	
	/**
	 * 生成缓存
	 */
	private function createCache($sql,$data,$cacheId=''){
		return;
		$tmp = $this->createFilename($sql,$cacheId);
		if(!is_dir($tmp['path']))@mkdir($tmp['path'],0777,true);
		@file_put_contents($tmp['path'].$tmp['filename'],serialize($data));
	}
	
	/**
	 * 根据sql语句生成文件名及路径
	 */
	private function createFilename($sql,$cacheId=''){
		if(!empty($cacheId))$sql = $cacheId;
		$data = array(
					'path'    => $this->_cachePath.'sql/',
					'filename'=> ''
				);
		if(empty($sql)) return $data;
		$tmpName = md5($sql);
		$data = array(
					'path'    => $this->_cachePath.'sql/'.substr($tmpName,0,2).'/'.substr($tmpName,2,2).'/',
					'filename'=> substr($tmpName,3).'.tmp'
				);
		return $data;
	}
	
	/**
	 * 清除缓存
	 * 
	 * 条件为空则清除所有缓存
	 * 
	 * @return DbMysql
	 */
	public function clearCache($sql='',$cacheId=''){
		$data = $this->createFilename($sql,$cacheId);
		$times = time();
		if(!empty($sql) || !empty($cacheId)){
			if(!empty($data['filename'])){
				$path1= $data['path'].$data['filename'];
				if(file_exists($path1) && filemtime($path1)<$times)@unlink($path1);
			}			
		}
		//清除所有缓存
		else{
			$this->clearFile($this->_cachePath,$times);
		}
		return true;
	}

	/**
	 * 遍历删除文件及目录
	 */
	private function clearFile($cachePath,$times){
		$list = scandir($cachePath);
		foreach ($list as $key1=>$row1){
			if($key1<=1)continue;
			$path1 = $cachePath.'/'.$row1;
			if(is_dir($path1)){
				$this->clearFile($path1,$times);			
				//rmdir($path1);				
			} else {
				if(file_exists($path1) && filemtime($path1)<$times)@unlink($path1);
			}		
		}	
	}

	/**
	 * 写错误日志
	 *
	 * @param unknown_type $log
	 */
	private function createErrorLog($sql){
		$log = array(
					date("Y-m-d H:i:s"),
					str::getThisUrl(),
					$sql,
					mysql_error ()
				);				
		$log = implode(' - ',$log)."\r\n";
		$filename = $this->_cachePath.'error/'.date("Y-m").'.txt';
		if(!$fp = fopen($filename,'a+')){
			echo '错误日志打开失败,请联络QQ:9986584';
		}
		if( fwrite($fp,$log) === FALSE ){
			echo '错误日志写入失败,请联络QQ:9986584';
		}
		fclose($fp);		
	}
	
	/**
	 * 获取最后一次执行的sql语句
	 *
	 */
	public function getLastSql(){
		return $this->_sql;
	}
        
    /**
     * 获取数据库中所有的表
     * @param type $dbname
     * @return type 
     */
    function fetchAllTable($dbname='') {
        $dbname = !empty($dbname) ? $dbname : $this->_config['dbname'];
        $list = array();
        $result = mysql_list_tables($dbname);
        if ($result) {
            while ($row = mysql_fetch_row($result)) {
                $list[] = $row[0];
            }
            return $list;
        }
        else
            return array();
    }

    /**
     * 获取最后一次影响的记录数
     * @return type 
     */
    function fetchChangeRow() {
        return mysql_affected_rows();
    }
	
            //以下为数据库备份功能
    
    /**
     * 备份数据库
     * @param type $tableName
     * @param type $localhost
     * @param type $path
     * @param string $fileName
     * @return type 
     */
    function backupDatabase($tableName='',$localhost=true,$path='',$fileName='backup.sql'){
        @set_time_limit(600);
        $tableList = $this->fetchAllTable ();
        if(!empty($tableName)){
            if(in_array($tableName, $tableList))
            $tableList = array($tableName);
            else
                return -1;
        }
        $nowtime = date("Y-m-d H:i:s");
        $this->_backupsql = <<<EOT
/*
Author:aboc
QQ:9986584
Date:$nowtime
*/

EOT;
        foreach ($tableList as $table) {
            $create = $this->fetchRow("show create table $table");
            if(isset ($create['Create Table'])){
                $this->_backupsql .= "DROP TABLE IF EXISTS `$table`;";
                $this->_backupsql .= $create['Create Table'].";\n\r";
                //以下是导出数据
                $rows = $this->fetchAll("select * from ".$create['Table']);
                foreach ($rows as $row) {
                    $this->_backupsql .= "insert into `$table` values(";
                    foreach ($row as $key=>$value) {
                        $row[$key] = "'".addcslashes($value,"'")."'";
                    }
                    $this->_backupsql .= join(',', $row).");\r\n";                    
                }
                $this->_backupsql .= "\r\n\r\n";
            }
        }
        $this->_backupsql = iconv('gbk', 'utf-8', $this->_backupsql);
        if($localhost){
            //本地
            $fileName = time().rand(111111,999999).$fileName;
            if(file_put_contents($fileName, $this->_backupsql))
                    return 1;
            else
                return -2;
            
        }
        else{
            header("Content-type:application/txt");
            header("Content-Disposition:attachment;filename=\"$fileName\"");
            echo $this->_backupsql;
        }
        //return $this->_backupsql;
    }
    
    
    
    
    
    

	/**
	 * 释放查询结果
	 */
	private function free() {
		mysql_free_result($this->_Db);
	}

	/**
	 *
	 */
	function __destruct() {
//		$this->free();
	}
}

?>
