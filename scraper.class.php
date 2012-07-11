<?php

/*
	Generic scraping class by fbparis@gmail.com
	Should be ultra fast, permanently running N simultaneous connections

	-Implement your own code to discover new urls to scrap in method exec_conn()
	
		Tip: Be smart and prevent $todo to grow exponentially...
		Tip: Please avoid endless loops: do not add the same url more than once
	
	-Implement your own code to scrap content in method extract_datas()
	
		Tip: You can use simple_html_dom class to handle complex scraps
		Can be found here: http://sourceforge.net/projects/simplehtmldom/
*/

set_time_limit(0);

class Scraper {
	/* Public options */
	public $default_user_agent = ''; // default user_agent on an interface, can be an array of strings for more random ;)
	public $default_max_conns = 1; // default number of simultaneous connections on an interface
	public $default_auto_adjust_speed = true; // default behaviour of an interface (true/false)
	public $default_max_sleep_delay = 120; // in seconds, used only if auto_adjust_speed is true
	public $default_timeout = 30; // in seconds
	public $debug_level = 0; // 0 = all ; 1 = notices ; 2 = errors
	public $max_retry = 5; // max retries on 0 and 5xx responses
	
	public $output_file = ''; // Optional, 1 json encoded object per line will be written
	public $input_file = ''; // Optional, 1 URL to scrap per line
	public $errors_file = ''; // Optional, URLS that have not been scraped ; each line is formated as: URL HTTP_CODE
	
	/* Private internal stuff */
	protected $todo = array();  
	protected $done = false;
	protected $conns = array();
	protected $interfaces = array();
	protected $fp_out = null;
	protected $fp_in = null;
	protected $fp_errors = null;
	protected $timer = 0;		
		
	/* Special stuff for recovering mode */
	public $recovery_mode = false;
	protected $fp_in_offset = 0;
	protected $recovery_file = null;
		
	function __construct($recovery_file=null) {
		$this->recovery_file = $recovery_file ? $recovery_file : __FILE__ . '.recover.inc';
		if (file_exists($this->recovery_file)) {
			$this->debug('Running in recovery mode',1);
			if ($recovery = @unserialize(file_get_contents($this->recover_file))) {
				foreach ($recovery as $k=>$v) $this->$k = $v;
				@unlink($this->recovery_file);
			} else {
				$this->debug('Unable to recover datas, exiting',2);
				$this->done = true; // prevent backup
				exit;
			}
		}
		register_shutdown_function(array($this, '__destruct'));
		if (function_exists('pcntl_signal')) {
			$this->debug('System interruptions will be intercepted!',1);	
			pcntl_signal(SIGINT,array($this,'__destruct'));
			pcntl_signal(SIGKILL,array($this,'__destruct'));
		} 
	}
	
	function __destruct() {
		if (!$this->done) {
			if (is_resource($this->fp_out)) @fclose($this->fp_out);
			if (is_resource($this->fp_errors)) @fclose($this->fp_errors);
			$this->recovery_mode = true;
			foreach ($this->todo as $url=>$status) if ($status > 0) {
				$this->todo[$url] = 1 - $status;
			}
			$this->interfaces = array();
			$this->conns = array();
			$this->timer = microtime(true) - $this->timer;
			if (is_resource($this->fp_in)) {
				$this->fp_in_offset = ftell($this->fp_in);
				@fclose($this->fp_in);
			}
			$this->fp_in = $this->fp_out = $this->fp_errors = null;
			if (!@file_put_contents($this->recovery_file,serialize($this))) printf("%s\n",serialize($this));
			$this->done = true; // prevent multiple executions...
		}
	}
	
	public function add_interface($ip=0,$user_agent=null,$max_conns=null,$auto_adjust_speed=null,$max_sleep_delay=null,$timeout=null) {
		if (array_key_exists($ip,$this->conns)) return false;
		if ($user_agent === null) $user_agent = $this->default_user_agent;
		if ($max_conns === null) $max_conns = $this->default_max_conns;
		if ($auto_adjust_speed === null) $auto_adjust_speed = $this->default_auto_adjust_speed;
		if ($max_sleep_delay === null) $max_sleep_delay = $this->default_max_sleep_delay;
		if ($timeout === null) $timeout = $this->default_timeout;
		if ($this->interfaces[$ip] = new ScraperInterface($ip,$user_agent,$max_conns,$auto_adjust_speed,$max_sleep_delay,$timeout)) return true;
		return false;
	}
	
	public function add_url($url) {
		if ($this->recovery_mode) return true;
		if (array_key_exists($url,$this->todo)) return false;
		$this->todo[$url] = 0;
		return true;
	}
	
	/* Call this method to start scraping */
	public function run() {
		if (!count($this->interfaces)) $this->add_interface();
		$this->timer = $this->recovery_mode ? microtime(true) - $this->timer : microtime(true);
		if (is_readable($this->input_file)) {
			$this->fp_in = @fopen($this->input_file,'r');
			@fseek($this->fp_in,$this->fp_in_offset);
		}
		if ($this->output_file) $this->fp_out = @fopen($this->output_file,$this->recovery_mode ? 'a' : 'w');
		if ($this->errors_file) $this->fp_errors = @fopen($this->errors_file,$this->recovery_mode ? 'a' : 'w');
		$mh = curl_multi_init();
		$this->done = count($this->interfaces) == 0;
		while (!$this->done) {
			while ($this->assign_conn($mh)); 
			$status = curl_multi_exec($mh,$active);
			while ($info = curl_multi_info_read($mh)) $this->exec_conn($mh,$info['handle']);
			usleep(50);			
		}
		curl_multi_close($mh);
		if (is_resource($this->fp_errors)) @fclose($this->fp_errors);
		if (is_resource($this->fp_out)) @fclose($this->fp_out);
		if (is_resource($this->fp_in)) @fclose($this->fp_in);
		$this->debug(sprintf('Done in %.3f seconds',microtime(true)-$this->timer),1);
	}
	
	protected function debug($msg,$level=0) {
		if ($level >= $this->debug_level) printf("%s\n",$msg);
	}
	
	protected function assign_conn(&$mh) {
		/* First, look in $todo for a ready to scrap url */
		foreach ($this->todo as $url=>$status) if ($status <= 0) return $this->add_conn($mh,$url,$status);
		/* If nothing found in $todo, try to get one from the input file */
		if (is_resource($this->fp_in) && ($url = trim(@fgets($this->fp_in)))) return $this->add_conn($mh,$url,0);
		/* Nothing to scrap, check if done and return false */
		if (!count($this->todo) && !count($this->conns)) $this->done = true;
		return false;
	}
	
	protected function add_conn(&$mh,$url,$status=0) {
		foreach ($this->interfaces as $k=>$interface) if ($interface->ready()) {
			// Last used interface is moved to the bottom of the pile 
			unset($this->interfaces[$k]);
			$this->interfaces[$k] = $interface;
			$interface = &$this->interfaces[$k];
			// Get a connection
			$ch = $interface->get_conn($url);
			$ret = curl_multi_add_handle($mh,$ch);
			if (0 === $ret) {
				$this->todo[$url] = 1 - $status;
				$this->conns[$url] = $k;
				$this->debug(">>> $url");
				return true;
			} else {
				$this->debug("Curl error $ret while adding new handle",2);
				$interface->close_conn($ch);
				break;
			}
		}
		/* no available connection */
		$this->todo[$url] = $status;
		return false;
	}
	
	protected function get_status($http_code) {
		if ($http_code == 200) return 'success';
		if ($http_code == 0) return 'fail';
		if ($http_code >= 500) return 'fail';
		return null;
	}
	
	protected function exec_conn(&$mh,&$ch) {
		$info = curl_getinfo($ch);
		$html = curl_multi_getcontent($ch);
		curl_multi_remove_handle($mh,$ch);
		$this->interfaces[$this->conns[$info['url']]]->close_conn($ch,$this->get_status($info['http_code']));
		unset($this->conns[$info['url']]);
		$this->debug(sprintf("<<< %s (%d)",$info['url'],$info['http_code']));
		$status = $this->todo[$info['url']];
		$ret = true;
		switch ($info['http_code']) {
			case 200:
				
				/* Add code to add other url(s) in $todo here */
				
				if ($results = $this->extract_datas($html)) {
					foreach ($results as $i=>$result) {
						if (is_resource($this->fp_out)) @fputs($this->fp_out,json_encode($results[$i]) . "\n");
						else printf("%s\n",trim(print_r($result,true)));
					}
				}
				break;
			default:
				$ret = false;
				$this->debug(sprintf("Error parsing %s (%d)",$info['url'],$info['http_code']),2);
				if ($this->get_status($info['http_code']) == 'fail') {
					if ($status < $this->max_retry) $this->todo[$info['url']] = -$status;
					else if (is_resource($this->fp_errors)) @fputs($this->fp_errors,sprintf("%s %d\n",$info['url'],$info['http_code']));
				} 
		}
		unset($this->todo[$info['url']]);
		return $ret;
	}
	
	protected function extract_datas(&$html) {
		$results = array();
		
		/* Add code to extract datas here */
		
		return count($results) ? $results : false;
	}
}

class ScraperInterface {
	protected $ip = 0;
	protected $user_agent = '';
	protected $max_conns = 1;
	protected $auto_adjust_speed = true;
	protected $max_sleep_delay = 300;
	protected $timeout = 30;
	
	protected $conns = 0;
	protected $current_max_conns = 1;
	protected $sleep_delay = 0;
	protected $last_conn = 0;
	protected $failed = false;
	
	function __construct($ip,$user_agent,$max_conns,$auto_adjust_speed,$max_sleep_delay,$timeout) {
		$this->ip = $ip;
		$this->user_agent = $user_agent;
		$this->max_conns = 1;
		$this->auto_adjust_speed = $auto_adjust_speed;
		$this->max_sleep_delay = $max_sleep_delay;
		$this->timeout = $timeout;
		
		$this->current_max_conns = $this->auto_adjust_speed ? 1 : $this->max_conns;
	}
	
	public function ready() {
		return ($this->conns < $this->current_max_conns) && (($this->sleep_delay == 0) || (((microtime(true) - $this->last_conn)) >= $this->sleep_delay));
	}
	
	public function get_conn($url) {
		$ch = curl_init($url);
		if ($this->ip) curl_setopt($ch,CURLOPT_INTERFACE,$this->ip);
		$ua = is_array($this->user_agent) && count($this->user_agent) ? $this->user_agent[mt_rand(0,count($this->user_agent) - 1)] : $this->user_agent;
		if (is_string($ua)) curl_setopt($ch,CURLOPT_USERAGENT,$ua);
		curl_setopt($ch,CURLOPT_RETURNTRANSFER,true);
		curl_setopt($ch,CURLOPT_TIMEOUT,$this->timeout);
		curl_setopt($ch,CURLOPT_SSL_VERIFYPEER,false);
		curl_setopt($ch,CURLOPT_FOLLOWLOCATION,false);
		$this->conns++;
		if ($this->auto_adjust_speed && ($this->current_max_conns == 1)) $this->last_conn = microtime(true);
		return $ch;
	}
	
	public function close_conn(&$ch,$status=null) {
		$this->conns--;
		@curl_close($ch);
		if (!$this->auto_adjust_speed) return true;
		switch ($status) {
			case 'fail':
				return $this->trigger_fail();
			case 'success':
				return $this->trigger_success();
			default:
				return true;
		}
	}
	
	protected function trigger_fail() {
		if (!$this->failed) {
			$this->failed = true;
			return true;
		}
		if ($this->current_max_conns > 1) {
			$this->current_max_conns--;
		} else {
			if ($this->sleep_delay == 0) $this->sleep_delay = 1;
			else $this->sleep_delay = min($this->max_sleep_delay,$sleep_delay * 2);
		}
		return true;
	}
	
	protected function trigger_success() {
		$this->failed = false;
		if ($this->sleep_delay > 0) {
			$this->sleep_delay = round($this->sleep_delay / 3);
		} else if ($this->current_max_conns < $this->max_conns) {
			$this->current_max_conns++;
		}
		return true;
	}
}

?>
						