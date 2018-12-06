<?php

require_once('RiakException.php');

class Riak {

    private $logEnabled = true;
    private $logfile = '/var/log/riak.log';
    private $syslogEnabled = true;

	function Riak( $options ) {
		$this->force_no_decode = false;	
		$this->debug = false;	

		foreach($options as $key => $value) $this->$key = $value; 
		$auth = array();

	}

	function log( $logthis ) {
        $message = "{$_SERVER['REMOTE_ADDR']} - ".$logthis;
		if( $this->debug ) {
			echo $logthis."\n";
		}
        if($this->logEnabled) {
            file_put_contents($this->logfile, date("[Y-m-d H:i:s] ").$message."\n", FILE_APPEND);
        }
        if($this->syslogEnabled) {
            openlog('RiakDB.PHP', LOG_ODELAY, LOG_LOCAL1);
            syslog(LOG_INFO, $message);
        }
	}
	
	function send( $method, $url, $post_data = NULL, $type = 'application/json' ) {

		$bldred=chr(0x1B).'[1;31m'; $bldgrn=chr(0x1B).'[1;32m'; $bldylw=chr(0x1B).'[1;33m'; $bldblu=chr(0x1B).'[1;34m'; $bldpur=chr(0x1B).'[1;35m'; $bldcyn=chr(0x1B).'[1;36m'; $bldwht=chr(0x1B).'[1;37m'; $txtrst=chr(0x1B).'[0m'; 


		$mstart = microtime(true);
		$s = fsockopen($this->host, $this->port, $errno, $errstr);
		if(!$s) {
			echo "$errno: $errstr\n";
			throw new RiakException($errstr, $errno);
		}

		//$request = "$method $url HTTP/1.1\r\nHost: $this->host:$this->port\r\n";
		$request = "$method $url HTTP/1.0\r\nHost: $this->host\r\n";
		if (isset($this->user)) $request .= "Authorization: Basic ".base64_encode("$this->user:$this->pass")."\r\n";


		$request .= "Content-Type: $type\r\n";
		$request .= "Accept: application/json, application/octet-stream, audio/*\r\n";
		//if( isset($this->xauth) ) $request .= "X-Auth-Token: {$this->xauth}\r\n";
		$request .= "X-Riak-ClientId: Vortex\r\n";

		if($post_data) {
			//$request .= "Content-Type: application/json\r\n";
			$request .= "Content-Length: ".strlen($post_data)."\r\n\r\n";
			$request .= "$post_data\r\n";
		} else {
			$request .= "\r\n";
		}


		fwrite($s, $request);
		$response = "";

		while(!feof($s)) { $response .= fgets($s); }
		fclose($s);

		$mend = microtime(true);

		//if( $this->profile ) printf("{$bldblu}URL:{$bldylw}$url {$bldblu}µT:{$bldylw}".( $mend - $mstart ).$txtrst."\n");
		$this->log( "$url µT:".( $mend - $mstart ));

		list($this->headers, $this->body) = explode("\r\n\r\n", $response);

		if( $method == "DELETE" ) {
			if( stristr($this->headers,"204 No Content") ) {
				return( array('status' => 'success'));
			}

		}

		if( !stristr($this->headers,"200 OK") &&
			!stristr($this->headers,"201 Created") &&
			!stristr($this->headers,"204 No Content")) {
			$this->log("{$bldpur}>>>>: $method $url HTTP/1.0 ($type) POST_LENGTH:".strlen($post_data)."$txtrst \n");
			if( $post_data && $type == 'application/json' ) $this->log("{$bldpur}POST_DATA: ".trim($post_data)."\n");
			$this->log("{$bldylw}<<<<: µT=".( $mend - $mstart )."{$txtrst}\n");
			$this->log("{$bldylw}<<<<: H:".$this->headers."{$txtrst}");
			$this->log("{$bldylw}<<<<: B:".$this->body."{$txtrst}");

			// 404 is an accepted response, but others should be an exception
			if (!stristr($this->headers, '404 Object Not Found')) {
				if(preg_match('/HTTP\/\d\.\d (\d+) (.*)/', explode("\r\n", $this->headers)[0], $matches)) {
					throw new RiakException($matches[2], $matches[1]);
				}

				throw new RiakException($this->headers, 500);
			}
		}
		
		if( $this->force_no_decode ) {

			return $this->body;
		} 

		return json_decode($this->body,true);
		

	}



}



?>
