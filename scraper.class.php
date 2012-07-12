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
	public $max_memory_usage_ratio = 0.8; // if $todo_file is set, new urls will be written in file if memory usage exceeds this ratio
	
	/* Public advanced stuff */
	public $success_codes = array(200);
	public $fail_codes = array(0,118,204,300,310,500,501,502,503,504,505,507,509);
	public $redirect_codes = array(301,302,303,307);
	
	/* Private internal stuff */
	protected $input_file = '';
	protected $output_file = '';
	protected $todo_file = '';
	protected $recovery_file = '';
	protected $errors_file = '';
	protected $todo = array();  
	protected $headers = array();
	protected $done = true;
	protected $conns = array();
	protected $interfaces = array();
	protected $fp_out = null;
	protected $fp_in = null;
	protected $fp_errors = null;
	protected $fp_todo = null;
	protected $timer = 0;
	protected $memory_limit = 0;
	protected $todo_swaps = 0;
	protected $success_function = null;
	protected $redirect_function = null;
		
	/* Special stuff for recovering mode */
	public $recovery_mode = false;
	protected $fp_in_offset = 0;
		
	function __construct() {
		$this->recovery_file = __FILE__ . '.recover.inc';
		if (file_exists($this->recovery_file)) {
			$this->debug('Running in recovery mode',1);
			if ($recovery = @unserialize(file_get_contents($this->recovery_file))) {
				if (!@unlink($this->recovery_file)) $this->debug('Unable to delete recovery file!',2);
				foreach ($recovery as $k=>$v) $this->$k = $v;
			} else {
				$this->debug('Unable to recover datas, exiting',2);
				$this->done = true; // prevent backup
				exit;
			}
		}
		$this->memory_limit = intval(ini_get('memory_limit')) * 1024 * 1024;
		$this->input_file = __FILE__ . '.input';
		$this->output_file = __FILE__ . '.output';
		$this->todo_file = __FILE__ . '.todo';
		$this->errors_file = __FILE__ . '.errors';
		register_shutdown_function(array($this, '_destruct'));
		if (function_exists('pcntl_signal')) {
			$this->debug('System interruptions will be intercepted!',1);	
			pcntl_signal(SIGINT,array($this,'_destruct'));
			pcntl_signal(SIGKILL,array($this,'_destruct'));
		} 
	}
	
	function _destruct() {
		if (!$this->done) {
			if (is_resource($this->fp_out)) @fclose($this->fp_out);
			if (is_resource($this->fp_errors)) @fclose($this->fp_errors);
			if (is_resource($this->fp_todo)) @fclose($this->fp_todo);
			$this->recovery_mode = true;
			$this->success_function = null;
			$this->redirect_function = null;
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
			$this->fp_in = $this->fp_out = $this->fp_errors = $this->fp_todo = null;
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
	
	public function add_url($url,$headers=null) {
		if ($this->recovery_mode) return true;
		if (array_key_exists($url,$this->todo)) return false;
		$headers = is_array($headers) ? $headers : null;
		if (is_resource($this->fp_todo) && (memory_get_peak_usage(true) / $this->memory_limit > $this->max_memory_usage_ratio)) {
			if (@fputs($this->fp_todo,sprintf("%s %s\n",$url,serialize($headers)))) return true;
		}
		$this->todo[$url] = 0;
		$this->headers[$url] = $headers;
		return true;
	}
	
	public function remove_url($url) {
		if (array_key_exists($url,$this->todo)) {
			unset($this->todo[$url]);
			unset($this->headers[$url]);
		}
	}
	
	public function register_success_function($function) {
		if (function_exists($function)) {
			$this->success_function = $function;
			return true;
		}
		$this->debug("Success function $function not found!",2);
		return false;
	}

	public function register_redirect_function($function) {
		if (function_exists($function)) {
			$this->redirect_function = $function;
			return true;
		}
		$this->debug("Redirect function $function not found!",2);
		return false;
	}
	
	/* Call this method to start scraping */
	public function run() {
		if (!count($this->interfaces)) $this->add_interface();
		$this->timer = $this->recovery_mode ? microtime(true) - $this->timer : microtime(true);
		if ($this->fp_in = @fopen($this->input_file,'rb')) {
			@fseek($this->fp_in,$this->fp_in_offset);
		} else {
			if (file_exists($this->input_file)) {
				$this->debug(sprintf('Input file "%s" not readable! Exiting',$this->input_file),2);
				exit;
			}
		}
		if (!$this->fp_out = @fopen($this->output_file,$this->recovery_mode ? 'ab' : 'wb')) {
			$this->debug(sprintf('Output file "%s" not writable! Exiting',$this->output_file),2);
			exit;
		}
		if (!$this->fp_errors = @fopen($this->errors_file,$this->recovery_mode ? 'ab' : 'wb')) {
			$this->debug(sprintf('Errors file "%s" not writable! Exiting',$this->errors_file),2);
			exit;
		}
		if (!$this->fp_todo = @fopen($this->todo_file,$this->recovery_mode ? 'ab' : 'wb')) {
			$this->debug(sprintf('Todo file "%s" not writable! Exiting',$this->todo_file),2);
			exit;
		}
		$mh = curl_multi_init();
		$this->done = count($this->interfaces) == 0;
		$this->recovery_mode = false;
		while (!$this->done) {
			while ($this->assign_conn($mh)); 
			$status = curl_multi_exec($mh,$active);
			while ($info = curl_multi_info_read($mh)) $this->exec_conn($mh,$info['handle']);
			usleep(50);			
		}
		curl_multi_close($mh);
		if (is_resource($this->fp_todo)) @fclose($this->fp_todo);
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
		foreach ($this->todo as $url=>$status) if ($status <= 0) return $this->add_conn($mh,$url,$this->headers[$url],$status);
		/* If nothing found in $todo, try to get one from the input file */
		if (is_resource($this->fp_in) && ($url = trim(@fgets($this->fp_in)))) {
			list($url,$headers) = split(' ',$url,2);
			$headers = @unserialize($headers);
			return $this->add_conn($mh,$url,$headers,0);
		}
		/* If stuffs in todo_file, switch it with input_file */
		if (is_resource($this->fp_todo)) {
			clearstatcache();
			if (filesize($this->todo_file) > 0) {
				$this->debug('Swapping todo file with input file...');
				@fclose($this->fp_todo);
				$this->fp_todo = null;
				if (is_resource($this->fp_in)) {
					@fclose($this->fp_in);
					$this->fp_in = null;
					if (!rename($this->input_file,$this->input_file . '.' . $this->todo_swaps)) {
						$this->debug('Unable to rename input file! Exiting',2);
						exit;
					}
					$this->todo_swaps++;
				}
				if (!rename($this->todo_file,$this->input_file)) {
					$this->debug('Unable to rename todo file! Exiting',2);
					exit;
				}
				$this->fp_in = @fopen($this->input_file,'rb');
				$this->fp_todo = @fopen($this->todo_file,'wb');
				return $this->assign_conn($mh);
			}
		}
		/* Nothing to scrap, check if done and return false */
		if (!count($this->todo) && !count($this->conns)) $this->done = true;
		return false;
	}
	
	protected function add_conn(&$mh,$url,$headers=null,$status=0) {
		foreach ($this->interfaces as $k=>$interface) if ($interface->ready()) {
			// Last used interface is moved to the bottom of the pile 
			unset($this->interfaces[$k]);
			$this->interfaces[$k] = $interface;
			$interface = &$this->interfaces[$k];
			// Get a connection
			$ch = $interface->get_conn($url,$this->headers[$url]);
			$ret = curl_multi_add_handle($mh,$ch);
			if (0 === $ret) {
				$this->todo[$url] = 1 - $status;
				$this->headers[$url] = $headers;
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
		$this->headers[$url] = $headers;
		return false;
	}
	
	protected function get_status($http_code) {
		if (in_array($http_code,$this->success_codes)) return 'success';
		if (in_array($http_code,$this->fail_codes)) return 'fail';
		if (in_array($http_code,$this->redirect_codes)) return 'redirect';
		return null;
	}
	
	protected function exec_conn(&$mh,&$ch) {
		$info = curl_getinfo($ch);
		list($html,$headers) = split("\r\n\r\n",curl_multi_getcontent($ch),2);
		curl_multi_remove_handle($mh,$ch);
		$response_status = $this->get_status($info['http_code']);
		$this->interfaces[$this->conns[$info['url']]]->close_conn($ch,$response_status);
		unset($this->conns[$info['url']]);
		$this->debug(sprintf("<<< %s (%d)",$info['url'],$info['http_code']));
		$status = $this->todo[$info['url']];
		switch ($response_status) {
			case 'success':
				$results = $this->success_function ? @call_user_func($this->success_function,&$this,&$html,&$headers,&$info) : null;
				if (is_array($results)) {
					foreach ($results as $i=>$result) {
						if (is_resource($this->fp_out)) @fputs($this->fp_out,json_encode($results[$i]) . "\n");
						else printf("%s\n",trim(print_r($result,true)));
					}
				}
				$this->remove_url($url);
				break;
			case 'redirect':
				if ($this->redirect_function) @call_user_func($this->redirect_function,&$this,&$headers,&$info);
				$this->remove_url($url);
				break;
			case 'fail':
				if ($status < $this->max_retry) $this->todo[$info['url']] = -$status;
				else {
					if (is_resource($this->fp_errors)) @fputs($this->fp_errors,sprintf("%s %d\n",$info['url'],$info['http_code']));
					$this->remove_url($url);
				}
				break;
			default:
		}
		if (!$response_status == 'success') return true;
		else {
			$this->debug(sprintf("*** %s has returned a %d http code",$info['url'],$info['http_code']),1);
			return false;
		}
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
	
	public function get_conn($url,$headers=null) {
		$ch = curl_init($url);
		if ($this->ip) curl_setopt($ch,CURLOPT_INTERFACE,$this->ip);
		$ua = is_array($this->user_agent) && count($this->user_agent) ? $this->user_agent[mt_rand(0,count($this->user_agent) - 1)] : $this->user_agent;
		if (is_string($ua)) curl_setopt($ch,CURLOPT_USERAGENT,$ua);
		if (is_array($headers) && count($headers)) curl_setopt($ch,CURLOPT_HTTPHEADER,$headers);
		curl_setopt($ch,CURLOPT_HEADER,true);
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
				if (!$this->failed) {
					$this->failed = true;
					break;
				}
				if ($this->current_max_conns > 1) {
					$this->current_max_conns--;
				} else {
					if ($this->sleep_delay == 0) $this->sleep_delay = 1;
					else $this->sleep_delay = min($this->max_sleep_delay,$sleep_delay * 2);
				}
				break;
			case 'success':
				$this->failed = false;
				if ($this->sleep_delay > 0) {
					$this->sleep_delay = round($this->sleep_delay / 3);
				} else if ($this->current_max_conns < $this->max_conns) {
					$this->current_max_conns++;
				}
				break;
		}
	}
}

?>
						