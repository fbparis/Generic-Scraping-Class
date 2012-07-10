<?php

/*
	Generic scraping class by fbparis@gmail.com
	Should be ultra fast, permanently running N simultaneous connections

	-Implement your own code to discover new urls to scrap in method exec_conn()
	
		Tip: Be smart and prevent $this->todo to grow exponentially...
	
	-Implement your own code to scrap content in method extract_datas()
	
		Tip: You can use simple_html_dom class to handle complex scraps
		Can be found here: http://sourceforge.net/projects/simplehtmldom/
*/

set_time_limit(0);

class Scraper {
	/* Public options */
	public $user_agent = '';
	public $max_conns = 3; // number of simultaneous connections
	public $timeout = 30; // in seconds
	public $debug_level = 0; // 0 = all ; 1 = notices ; 2 = errors
	public $max_retry = 5; // max retries on 5xx responses
	
	public $output_file = ''; // 1 json encoded object per line will be written
	public $input_file = ''; // 1 URL to scrap per line, optionnal
	public $todo = array(); // Associative array of urls to scrap: URL => 0 
	
	/* Private internal stuff */
	protected $conns = 0;
	protected $fp_out = null;
	protected $fp_in = null;
	protected $timer = 0;
		
	function __construct() {
	}
	
	protected function debug($msg,$level=0) {
		if ($level >= $this->debug_level) printf("%s\n",$msg);
	}
	
	/* Call this method to start scraping */
	public function run() {
		$this->timer = microtime(true);
		if (is_readable($this->input_file)) $this->fp_in = @fopen($this->input_file,'r');
		$this->fp_out = fopen($this->output_file,'w');
		$mh = curl_multi_init();
		while (true) {
			while ($this->conns < $this->max_conns) if (!$this->assign_conn($mh)) {
				if (!$active && ($this->conns <= 0)) {
					$this->debug('Exiting');
					break(2);
				}
				break;
			}
			$status = curl_multi_exec($mh,$active);
			while ($info = curl_multi_info_read($mh)) $this->exec_conn($mh,$info['handle']);
			usleep(50);			
		}
		curl_multi_close($mh);
		fclose($this->fp_out);
		if (is_resource($this->fp_in)) @fclose($this->fp_in);
		$this->debug(sprintf('Done in %d seconds',microtime(true)-$this->timer),1);
	}
	
	protected function assign_conn(&$mh) {
		/* First, look in $this->todo for a ready to scrap url */
		foreach ($this->todo as $url=>$status) if ($status <= 0) return $this->add_conn($mh,$url,$status);
		/* If nothing found in $this->todo, try to get one from the input file */
		if ($url = trim(@fgets($this->fp_in))) return $this->add_conn($mh,$url,0);
		/* Nothing to scrap, return false */
		return false;
	}
	
	protected function add_conn(&$mh,$url,$status=0) {
		$ch = curl_init($url);
		curl_setopt($ch,CURLOPT_USERAGENT,$this->user_agent);
		curl_setopt($ch,CURLOPT_RETURNTRANSFER,true);
		curl_setopt($ch,CURLOPT_TIMEOUT,$this->timeout);
		curl_setopt($ch,CURLOPT_SSL_VERIFYPEER,false);
		curl_setopt($ch,CURLOPT_FOLLOWLOCATION,false);
		$ret = curl_multi_add_handle($mh,$ch);
		if (0 === $ret) {
			$this->todo[$url] = 1 - $status;
			$this->conns++;
			$this->debug(">>> $url");
			return true;
		} else {
			$this->todo[$url] = $status;
			$this->debug("Curl error $ret while adding new handle",2);
			curl_close($ch);
			return false;
		}
	}
	
	protected function exec_conn(&$mh,&$ch) {
		$info = curl_getinfo($ch);
		$html = curl_multi_getcontent($ch);
		curl_multi_remove_handle($mh,$ch);
		curl_close($ch);
		$this->conns--;
		$this->debug(sprintf("<<< %s (%d)",$info['url'],$info['http_code']));
		$status = $this->todo[$info['url']];
		unset($this->todo[$info['url']]);
		switch ($info['http_code']) {
			case 200:
				
				/* Add code to add other url(s) in $this->todo here */
				
				if ($results = $this->extract_datas($html)) {
					foreach ($results as $i=>$result) {
						fputs($this->fp_out,json_encode($results[$i]) . "\n");
					}
				}
				break;
			default:
				$this->debug(sprintf("Error parsing %s (%d)",$info['url'],$info['http_code']),2);
				if ((($info['http_code'] >= 500) || ($info['http_code'] == 0)) && ($status < $this->max_retry)) $this->todo[$info['url']] = -$status;
				return false;
		}
		return true;
	}
	
	protected function extract_datas(&$html) {
		$results = array();
		
		/* Add code to extract datas here */
		
		return count($results) ? $results : false;
	}
}

?>
						