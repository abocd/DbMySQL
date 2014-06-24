<?php
/**
 * Mysql���ݿ������ v2.0
 * 2009.1.22 by Aboc QQ:9986584
 * �����ļ�����
 *
 */
class DbMysql {
	/**
	 * �����ݿ������,ֻ�����ݿ������Ӱ����ַ���ת��
	 * ������������,�������ݲ��� @
	 */

	/*
	 * ���ݿ����Ӿ��
	 */
	private $_Db = NULL;

	/*
	 * �Ƿ�������� 0.1
	 */
	private $_pconnect = 0;

	/*
	 * ����
	 */
	private $_charset = 'gbk';
	
	
	/*
	 *���һ�β����ID
	 */
	 private $_lastId = 0;

	/*
	 * Ĭ�����ݿ�����
	 */
	private $_config = array ('dbhost' => 'localhost', 'dbuser' => 'root', 'dbpass' => 'root', 'dbname' => 'test');
	
	/**
	 * ����·��
	 */
	private $_cachePath = '';
	
	/**
	 * sql���
	 *
	 * @var unknown_type
	 */
	private $_sql = '';
        
        /**
         *
         * @var type ������
         */
        private $_backupsql = '';

	/**
	 * ��ʼ�������ݿ�
	 */
	function __construct($config,$pconnect=0,$cachepath='cache') {
		if (empty($config)) $config = array();
		$this->checkConfig ( $config );
		$this->_pconnect = $pconnect;
		$this->connect ();
		$this->query ( 'set names ' . $this->_charset ); //���ñ���
		$this->_cachePath = $cachepath;
	}

	/**
	 * �ж�config����
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
	 * �������ݿ�
	 */
	private function connect() {
//		print_r($this->_config);
		if ($this->_pconnect) {
			$this->_Db = mysql_pconnect ( $this->_config ['dbhost'], $this->_config ['dbuser'], $this->_config ['dbpass'] ) or die ( '���ݿ�����ʧ��' . mysql_errno () );
		} else {
			$this->_Db = mysql_connect ( $this->_config ['dbhost'], $this->_config ['dbuser'], $this->_config ['dbpass'] ) or die ( '���ݿ�����ʧ��' . mysql_errno () );
		}
		if ($this->_Db != NULL) {
			mysql_select_db ( $this->_config ['dbname'], $this->_Db ) or die ( '���ݿ�' . $this->_config ['dbname'] . '������' );
		}
	}

	/**
	 * �������ĵ����Ż�˫����ת��
	 *
	 * @param unknown_type $string
	 */
	private function strtag($string1) {
			if (is_array ( $string1 )) {
				foreach ( $string1 as $key => $value ) {
					$stringnew [$this->strtag ( $key )] = $this->strtag ( $value );
				}
			} else {
				//�ڴ���ת��,�Ե�����
				//TODO ���� %ҲҪת���?
				//$string = iconv("gbk","gbk",$string);
				$stringnew = mysql_real_escape_string ( $string1 );
//				$stringnew = get_magic_quotes_gpc()?$string:addslashes ( $string1 );
//				$stringnew=str_replace(array("'",'"'),array("\'",'\"'),$string1);
			}
		return $stringnew;
	}

	/**
	 * ������ת��ΪSQL���ܵ�������ʽ
	 *
	 * @param unknown_type $array
	 */
	private function chageArray($array) {
		//MYSQL֧��insert into joincart set session_id = 'dddd',product_id='44',number='7',jointime='456465'
		//���Ը��ºͲ������ʹ��ͬһ������
		$array = $this->strtag ( $array ); //ת��
		$str = '';
		foreach ( $array as $key => $value ) {
			$str .= empty ( $str ) ? "`" . $key . "`='" . $value."'" : ", `" . $key . "`='" . $value."'";
		}
		return $str;
	}

	/**
	 * ִ�в�ѯ���
	 * @return bool
	 */
	public function query($sql) {
		//echo $sql.'<br>';
        $this->_sql = $sql;
		if (! $result = mysql_query ( $sql, $this->_Db)) {
			if(UC_DBUSER == 'root'){
			    echo $sql.'<br>'.mysql_error().'<br>';
			    //$this->createErrorLog($sql);
			    die ( '���ݿ����' );
			} else {
				 $subject = date("Y-m-d H:i:s")."���ݿ��ѯ����";
				 $thisurl = str::getThisUrl();
				 $error = mysql_error();
    			 $content = <<<EOT
���ݿ��ѯ����,��ϸ����:<br />
$thisurl <br />
$error <br />
$sql
EOT;
                 $username = '����';
                 if (function_exists('sendMail')){
                 	sendMail('9986584@qq.com',$username,$subject,$content);
                    die(':(&nbsp;���ݿ��ѯ����,�Ѿ�֪ͨ����Ա,���Ժ�����');
                 }
			}
		} else {
			return $result;
		}
	}


	/**
	 * �����¼
	 *
	 */
	public function insert($table, $array) {
		if(!is_array($array))return false;
		$array = $this->strtag ( $array ); //ת��
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
	 * �滻������
	 * @param unknown_type $table
	 * @param unknown_type $array
	 */
	public function replaceInsert($table, $array) {
		if(!is_array($array))return false;
		$array = $this->strtag ( $array ); //ת��
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
	 * ���������¼
	 *
	 * @param $table ����  
	 * @param $batchArray �������� ,��ά����,����������ͬ,�����ܲ���
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
	 * ���¼�¼
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
	 * ɾ����¼
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
	 * ��ȡһ����¼
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
	 * ��ȡ���м�¼/�õ�mysql_fetch_assocѭ��
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
	 * ��ȡ���һ��Ӱ���Id
	 *
	 */
	public function lastId() {
		$this->_lastId = mysql_insert_id ( $this->_Db );
		return $this->_lastId;
	}

	/**
	 * ��ȡ���������ļ�¼��
	 *
	 */
	public function fetchNum($sql) {
		$reult = $this->query ( $sql );
		$num = mysql_num_rows ( $reult );
		return $num;
	}

	/**
	 * ����ʺϵ�where���
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
	 * �������ݿ����ô�С
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
	 * �жϻ����ļ��Ƿ���Ч,�����Ч���򷵻ػ�������
	 */
	private function checkCache($sql,$cacheTime = 0,$cacheId=''){
		//�����棬ֱ�ӷ���
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
	 * ���ɻ���
	 */
	private function createCache($sql,$data,$cacheId=''){
		return;
		$tmp = $this->createFilename($sql,$cacheId);
		if(!is_dir($tmp['path']))@mkdir($tmp['path'],0777,true);
		@file_put_contents($tmp['path'].$tmp['filename'],serialize($data));
	}
	
	/**
	 * ����sql��������ļ�����·��
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
	 * �������
	 * 
	 * ����Ϊ����������л���
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
		//������л���
		else{
			$this->clearFile($this->_cachePath,$times);
		}
		return true;
	}

	/**
	 * ����ɾ���ļ���Ŀ¼
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
	 * д������־
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
			echo '������־��ʧ��,������QQ:9986584';
		}
		if( fwrite($fp,$log) === FALSE ){
			echo '������־д��ʧ��,������QQ:9986584';
		}
		fclose($fp);		
	}
	
	/**
	 * ��ȡ���һ��ִ�е�sql���
	 *
	 */
	public function getLastSql(){
		return $this->_sql;
	}
        
    /**
     * ��ȡ���ݿ������еı�
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
     * ��ȡ���һ��Ӱ��ļ�¼��
     * @return type 
     */
    function fetchChangeRow() {
        return mysql_affected_rows();
    }
	
            //����Ϊ���ݿⱸ�ݹ���
    
    /**
     * �������ݿ�
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
                //�����ǵ�������
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
            //����
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
	 * �ͷŲ�ѯ���
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
