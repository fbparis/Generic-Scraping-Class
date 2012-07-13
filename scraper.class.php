<?php

/*
	Generic scraping class by fbparis@gmail.com
	Check the latest version here: https://github.com/fbparis/Generic-Scraping-Class
	-Should be ultra fast, permanently running N simultaneous connections on N' interfaces
	-An "Auto Adjust Speed" mode is available to automaticaly set the speed according to errors/success responses
	-If for any reason your script crashs, you won't loose anything and can restart from the last url scraped!
	
	Nothing to change, just call it, optionaly register your success_function and/or redirect_function and run!
	
	If {SCRIPTNAME} is your script name (where you include this class and call it), so you can create a file named
	{SCRIPTNAME}.input with one URL per line, or one URL followed by a space and a serialized array of headers per line to use it
	as input file.
	
	Also, these files may be created by the class:
	-{SCRIPTNAME}.errors: one URL followed by space and http response code for each urls that have been failed more than $max_retry times
	-{SCRIPTNAME}.output: one json_encoded object per line (results of the scrap)
	-{SCRIPTNAME}.todo: used by the class to replace $todo when memory usage become to high
	-{SCRIPTNAME}.recover.inc: used by the class to backup its internal state on error/interruption
	
	Optionaly, a {SCRIPTNAME}.input.orig can be create during the process to backup original input file when a todo file has to replace it.
	
	Empty files will be removed after the scraping is done.
	
	Basic example:
	
	$S = new Scraper;
	$S->add_url('http://example.com/');	
	$S->run();
	
	In this example, one url will be scraped and the resulting html will be json encoded in a file {SCRIPTNAME}.output
	
	NOTE ON SAVED / RETRIED ERRORS
		Only URLS returning a HTTP response code in $fail_codes will be retried / saved in {SCRIPTNAME}.errors
		Maybe you want to retry / save other ones, to do so, simply add some error codes in $fail_codes before calling method run().
		For example:
		
			$S->fail_codes[] = 404;
			
	
	NOTE ON SCRIPT INTERRUPTION
		If pcntl extension is not activated in your PHP configuration, stopping the script with CTRL-C won't be intercepted and
		recovery mode won't be activated.
		So there's a simple way to stop a running script using this Class:
		Just crate a .STOP file in the directory where the script is running (touch .STOP) and it will exit by saving its internal state
		
		
	NOTE ON AUTO_ADJUST_SPEED MODE
		If $auto_adjust_speed is set to true for an interface, the class will start scraping with 1 connexion on this interface.
		Connexion count will increase if no errors are encoutered, otherwise it will decrease, or if only one connexion,
		it will slow down (by increasing a delay beetween connexions).
		A basic behaviour has been coded, you can take a look on it in the close_conn() method of the ScraperInterface class.
		
		If $auto_adjust_speed is set to false, the class will directly and always scrap with $max_conns simultaneous connexions.

	
	See comments for the rest of the documentation ;-)
*/

set_time_limit(0);

class Scraper {
	/* Public options */
	/* These options can be overwritten when you call add_interface() method: */
	public $default_user_agent = ''; // default user_agent on an interface, can be an array of strings for more random ;)
	public $default_max_conns = 1; // default number of simultaneous connections on an interface
	public $default_auto_adjust_speed = true; // default behaviour of an interface (true/false)
	public $default_max_sleep_delay = 120; // in seconds, used only if auto_adjust_speed is true
	public $default_timeout = 30; // in seconds
	
	/* These are global options for the class */
	public $debug_level = 0; // 0 = all ; 1 = notices ; 2 = errors
	public $max_retry = 5; // max retries on error, when max has been reached for an url, it's added to the error file
	public $max_memory_usage_ratio = 0.8; // new urls to scrap will be added to a file when memory usage exceeds this ratio
	
	/* Public advanced stuff */
	public $success_codes = array(200); // on which http response codes we have a successful scrap
	public $fail_codes = array(0,118,204,300,310,500,501,502,503,504,505,507,509); // on which http response codes we want to retry later
	public $redirect_codes = array(301,302,303,307); // on which http response code we have an url redirection
	
	/* Private internal stuff */
	protected $basefile = '';
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
	protected $run_done = false;
		
	/* Special stuff for recovering mode */
	public $recovery_mode = false;
	protected $fp_in_offset = 0;
			
	/* PUBLIC API */
	
	/* 
		Add an interface 
		
		If you only have one IP and are ok with the default parameters (see public properties of the class above),
		you don't need to call this method because method run() will call it for you if no interfaces have been set.
		
		Even if you have one IP you may want to use this method to quickly override default options, or if you prefer using a proxy.
		
		add_interface() accepts these parameters:
		-$ip can be 0 for the current IP or any value accepted by curl_setopt for CURLOPT_INTERFACE
		-$user_agent can be a user agent string, or an array of user agent strings (then one will be chosen randomly each time)
		-$max_conns is the maximum number of simultaneous connexions allowed on this interface
		-$auto_adjust_speed can be true or false, see above for a description of this feature
		-$max_sleep_delay, in seconds, will be used only if $auto_adjust_speed is true
		-$timeout, in seconds
		-$proxy is a proxy ip or host, any value accepted by curl_setopt for CURLOPT_PROXY
		-$proxy_userpwd is a formated string "username:password" (see curl_setopt for CURLOPT_PROXYUSERPWD)
		
		You can add only one interface per (IP-PROXY)
	*/
	public function add_interface($ip=0,$user_agent=null,$max_conns=null,$auto_adjust_speed=null,$max_sleep_delay=null,$timeout=null,$proxy=null,$proxy_userpwd=null) {
		$key = $proxy ? "$ip-$proxy" : $ip;
		if (array_key_exists($key,$this->conns)) return false;
		if ($user_agent === null) $user_agent = $this->default_user_agent;
		if ($max_conns === null) $max_conns = $this->default_max_conns;
		if ($auto_adjust_speed === null) $auto_adjust_speed = $this->default_auto_adjust_speed;
		if ($max_sleep_delay === null) $max_sleep_delay = $this->default_max_sleep_delay;
		if ($timeout === null) $timeout = $this->default_timeout;
		if ($this->interfaces[$key] = new ScraperInterface($ip,$user_agent,$max_conns,$auto_adjust_speed,$max_sleep_delay,$timeout,$proxy,$proxy_userpwd)) return true;
		return false;
	}
	
	/* 
		Add an URL to scrap 
		
		You can add an URl to the todo list by calling this method.
		Optionaly you can pass an array of headers which will be used to retrieve this URL.
		
		Examples: 
		
			add_url('http://www.example.com');
			add_url('http://www.example.com/somepage.html',array('Referer: http://www.google.com/','Cookie: test=1'));
			
		Note that in recovery mode, every calls to this method until run() is called again will be ignored,
		so you don't have to rewrite your script when restarting it...
	*/
	public function add_url($url,$headers=null) {
		if ($this->recovery_mode) return true;
		if (array_key_exists($url,$this->todo)) return false;
		$headers = is_array($headers) ? $headers : null;
		if (is_resource($this->fp_todo) && ((memory_get_peak_usage(true) / $this->memory_limit) > $this->max_memory_usage_ratio)) {
			if (@fputs($this->fp_todo,sprintf("%s %s\n",$url,serialize($headers)))) return true;
		}
		$this->todo[$url] = 0;
		$this->headers[$url] = $headers;
		return true;
	}
	
	/* 
		Register your custom function to be called on success
		
		This function must take 4 parameters:
		-$S is a reference "to this class", $this
		-$html is the content of the document retrieved
		-$headers is the headers received when loading document
		-$info is the result of curl_get_info()
		
		...and return a mixed value:
		-if a single value, it will be json_encoded and saved in {SCRIPT_PATHNAME}.output (one line)
		-if an array, each element of the array will be json_encoded and saved in {SCRIPT_PATHNAME}.output
		
		Warning: if the element you want to record is an array, make it an object or insert it in another array...
		
		Exemple function:
		
		function success_function($S,$html,$headers,$info) {
			// find new urls to scrap and add them to the todo list
			...
			$S->add_url($url);
			...
			// extract data from $html
			...
			foreach ($matches as $data) $results[] = $data;
			...
			return $results;
		}
		
		Note that if you don't register any success function, the default behaviour will be to record all the html for each url scraped.
	*/
	public function register_success_function($function) {
		if (function_exists($function)) {
			$this->success_function = $function;
			return true;
		}
		$this->debug("Success function $function not found!",2);
		return false;
	}

	/* 
		Register your custom function to be called on redirect
		
		This function must take 3 parameters:
		-$S is a reference "to this class", $this
		-$headers is the headers received when loading document
		-$info is the result of curl_get_info()
		
		...and nothing.
		
		Exemple function:
		
		function redirect_function($S,$headers,$info) {
			// You may want to add the redirect url to the todo list:
			...
			if ($url = $info['redirect_url']) $S->add_url($url);
		}
	*/
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
		/* This method can only be called once in a script due to recovery mode limitations */
		if ($this->run_done) {
			$this->debug('Method run() can only be called once in a script!',2);
			exit;
		}
		/* If no interfaces have been added, add a default one with the current IP */
		if (!count($this->interfaces)) $this->add_interface();
		/* Recording the total time is fun */
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
		$mh = curl_multi_init();
		/* $done has to be false now */
		$this->done = count($this->interfaces) == 0;
		/* No more need for $recovery_mode at this point */
		$this->recovery_mode = false;
		while (!$this->done) {
			while ($this->assign_conn($mh)); 
			$status = curl_multi_exec($mh,$active);
			while ($info = curl_multi_info_read($mh)) $this->exec_conn($mh,$info['handle']);
			usleep(50);	// save your CPU
		}
		curl_multi_close($mh);
		clearstatcache();
		if (is_resource($this->fp_todo)) @fclose($this->fp_todo);
		if (filesize($this->todo_file) == 0) unlink($this->todo_file); // cleaning a little
		if (is_resource($this->fp_errors)) @fclose($this->fp_errors);
		if (filesize($this->errors_file) == 0) unlink($this->errors_file); // cleaning a little
		if (is_resource($this->fp_out)) @fclose($this->fp_out);
		if (filesize($this->output_file) == 0) {
			$this->debug('No output generated',1);
			unlink($this->output_file); // cleaning a little
		}
		if (is_resource($this->fp_in)) @fclose($this->fp_in);
		/* Cleaning again and restore input file if needed */
		if ($this->todo_swaps > 0) {
			unlink($this->input_file);
			if (file_exists($this->input_file . '.orig')) rename($this->input_file . '.orig',$this->input_file);
		}
		$this->run_done = true;
		$this->debug(sprintf('Done in %.3f seconds',microtime(true)-$this->timer),1);
	}
	
	/* Just print some text */
	public function debug($msg,$level=0) {
		if ($level >= $this->debug_level) printf("%s\n",$msg);
	}
	
	/* END OF PUBLIC PART */
	
	/* Constructor: no params */
	public function __construct() {
		/*
			Calling script path and name is the prefix to several files used by the class: 
			- recovery file: {SCRIPT_PATHNAME}.recover.inc
			- input file: {SCRIPT_PATHNAME}.input
			- output file: {SCRIPT_PATHNAME}.output
			- errors file: {SCRIPT_PATHNAME}.errors
			- todo file: {SCRIPT_PATHNAME}.todo
		*/
		$this->recovery_file = $_SERVER['PATH_TRANSLATED'] . '.recover.inc';
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
		$this->basefile = $_SERVER['PATH_TRANSLATED'];
		$this->memory_limit = intval(ini_get('memory_limit'));
		if (strpos(ini_get('memory_limit'),'K') !== false) $this->memory_limit *= 1024; 
		elseif (strpos(ini_get('memory_limit'),'M') !== false) $this->memory_limit *= 1024 * 1024; 
		elseif (strpos(ini_get('memory_limit'),'G') !== false) $this->memory_limit *= 1024 * 1024 * 1024; 
		$this->input_file = $this->basefile . '.input';
		$this->output_file = $this->basefile . '.output';
		$this->todo_file = $this->basefile . '.todo';
		$this->errors_file = $this->basefile . '.errors';
		if (!$this->fp_todo = @fopen($this->todo_file,$this->recovery_mode ? 'ab' : 'wb')) {
			$this->debug(sprintf('Todo file "%s" not writable! Exiting',$this->todo_file),2);
			exit;
		}
		register_shutdown_function(array($this, '_destruct'));
		if (function_exists('pcntl_signal')) {
			$this->debug('System interruptions will be intercepted!',1);	
			pcntl_signal(SIGINT,array($this,'_destruct'));
			pcntl_signal(SIGKILL,array($this,'_destruct'));
		} 
	}
	
	/* Pseudo-Destructor: called on shutdown (normal termination or fatal error as well) */
	public function _destruct() {
		if (!$this->done) {
			$this->debug('Backing up internal state...',1);
			if (is_resource($this->fp_out)) @fclose($this->fp_out);
			if (is_resource($this->fp_errors)) @fclose($this->fp_errors);
			if (is_resource($this->fp_todo)) @fclose($this->fp_todo);
			$this->recovery_mode = true;
			$this->success_function = null;
			$this->redirect_function = null;
			/* No time to properly close all running connections so we mark them as ready */
			foreach ($this->todo as $url=>$status) if ($status > 0) {
				$this->todo[$url] = 1 - $status;
			}
			$this->interfaces = array();
			$this->conns = array();
			/* We'll keep the timer running on relaunch */
			$this->timer = microtime(true) - $this->timer;
			/* Also keep trace of the pointer position in the input file */
			if (is_resource($this->fp_in)) {
				$this->fp_in_offset = ftell($this->fp_in);
				@fclose($this->fp_in);
			}
			$this->fp_in = $this->fp_out = $this->fp_errors = $this->fp_todo = null;
			if (!@file_put_contents($this->recovery_file,serialize($this))) printf("%s\n",serialize($this));
			$this->done = true; // prevent multiple executions...
		}
	}

	/* Remove an URL from the todo list */
	protected function remove_url($url) {
		if (array_key_exists($url,$this->todo)) {
			unset($this->todo[$url]);
			unset($this->headers[$url]);
		}
	}
	
	/* Find an URL to scrap */
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
					if ($this->todo_swaps == 0) {
						if (!rename($this->input_file,$this->input_file . '.orig')) {
							$this->debug('Unable to rename input file! Exiting',2);
							exit;
						}
					} else unlink($this->input_file);
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
		/* Useful to stop the script if pcntl extension is not loaded */
		if (!$this->done && file_exists('.STOP')) {
			$this->debug('STOP signal received, exiting',1);
			print_r($this->todo);
			print_r($this->conns);
			exit;
		}
		return false;
	}
	
	/* Find an available connexion to scrap an URL */
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
	
	/* Get a response status according to a http response code */
	protected function get_status($http_code) {
		if (in_array($http_code,$this->success_codes)) return 'success';
		if (in_array($http_code,$this->fail_codes)) return 'fail';
		if (in_array($http_code,$this->redirect_codes)) return 'redirect';
		return null;
	}
	
	/* Process a http response */
	protected function exec_conn(&$mh,&$ch) {
		$info = curl_getinfo($ch);
		$url = $info['url'];
		$http_code = $info['http_code'];
		list($headers,$html) = split("\r\n\r\n",curl_multi_getcontent($ch),2);
		curl_multi_remove_handle($mh,$ch);
		$response_status = $this->get_status($http_code);
		$this->interfaces[$this->conns[$url]]->close_conn($ch,$response_status);
		unset($this->conns[$url]);
		$this->debug(sprintf("<<< %s (%d)",$url,$http_code));
		$status = $this->todo[$url];
		switch ($response_status) {
			case 'success':
				if ($results = $this->success_function ? call_user_func($this->success_function,&$this,&$html,&$headers,&$info) : $html) {
					if (!is_array($results)) $results = array($results);
					foreach ($results as $i=>$result) {
						if (is_resource($this->fp_out)) @fputs($this->fp_out,json_encode($results[$i]) . "\n");
						else printf("%s\n",trim(print_r($result,true)));
					}
				}
				$this->remove_url($url);
				break;
			case 'redirect':
				if ($this->redirect_function) call_user_func($this->redirect_function,&$this,&$headers,&$info);
				$this->remove_url($url);
				break;
			case 'fail':
				if ($status < $this->max_retry) $this->todo[$url] = -$status;
				else {
					if (is_resource($this->fp_errors)) @fputs($this->fp_errors,sprintf("%s %d\n",$url,$http_code));
					$this->remove_url($url);
				}
				break;
			default:
				$this->remove_url($url);
		}
		/* Useful to stop the script if pcntl extension is not loaded */
		if (file_exists('.STOP')) {
			$this->debug('STOP signal received, exiting',1);
			exit;
		}
		if ($response_status == 'success') return true;
		$this->debug(sprintf("*** %s has returned a %d http code",$url,$http_code),1);
		return false;
	}
}

/* Class used internaly to reprensent and manage interfaces */
class ScraperInterface {
	protected $ip = 0;
	protected $user_agent = '';
	protected $max_conns = 1;
	protected $auto_adjust_speed = true;
	protected $max_sleep_delay = 300;
	protected $timeout = 30;
	protected $proxy = null;
	protected $proxy_userpwd = null;
	
	protected $conns = 0;
	protected $current_max_conns = 1;
	protected $sleep_delay = 0;
	protected $last_conn = 0;
	protected $failed = false;
	
	function __construct($ip,$user_agent,$max_conns,$auto_adjust_speed,$max_sleep_delay,$timeout,$proxy=null,$proxy_userpwd=null) {
		$this->ip = $ip;
		$this->user_agent = $user_agent;
		$this->max_conns = $max_conns;
		$this->auto_adjust_speed = $auto_adjust_speed;
		$this->max_sleep_delay = $max_sleep_delay;
		$this->timeout = $timeout;
		$this->proxy = $proxy;
		$this->proxy_userpwd = $proxy_userpwd;
		
		$this->current_max_conns = $this->auto_adjust_speed ? 1 : $this->max_conns;
	}
	
	/* Return true if interface is ready to accept new connexions, false otherwise */
	public function ready() {
		return ($this->conns < $this->current_max_conns) && (($this->sleep_delay == 0) || (((microtime(true) - $this->last_conn)) >= $this->sleep_delay));
	}
	
	/* Create a new connexion with curl and return it */
	public function get_conn($url,$headers=null) {
		$ch = curl_init($url);
		if ($this->ip) curl_setopt($ch,CURLOPT_INTERFACE,$this->ip);
		$ua = is_array($this->user_agent) && count($this->user_agent) ? $this->user_agent[mt_rand(0,count($this->user_agent) - 1)] : $this->user_agent;
		if (is_string($ua)) curl_setopt($ch,CURLOPT_USERAGENT,$ua);
		if (is_array($headers) && count($headers)) curl_setopt($ch,CURLOPT_HTTPHEADER,$headers);
		if ($this->proxy) curl_setopt($ch,CURLOPT_PROXY,$this->proxy);
		if ($this->proxy && $this->proxy_userpwd) curl_setopt($ch,CURLOPT_PROXYUSRPWD,$this->proxy_userpwd);
		curl_setopt($ch,CURLOPT_HEADER,true);
		curl_setopt($ch,CURLOPT_RETURNTRANSFER,true);
		curl_setopt($ch,CURLOPT_TIMEOUT,$this->timeout);
		curl_setopt($ch,CURLOPT_SSL_VERIFYPEER,false);
		curl_setopt($ch,CURLOPT_FOLLOWLOCATION,false);
		$this->conns++;
		if ($this->auto_adjust_speed && ($this->current_max_conns == 1)) $this->last_conn = microtime(true);
		return $ch;
	}
	
	/* Close a connexion and adjust behaviour if $auto_adjust_speed is true */
	public function close_conn(&$ch,$status=null) {
		$this->conns--;
		@curl_close($ch);
		if (!$this->auto_adjust_speed) return true;
		switch ($status) {
			case 'fail':
				/* Do something only on consecutive fails */
				if (!$this->failed) {
					$this->failed = true;
					break;
				}
				if ($this->current_max_conns > 1) {
					/* Reduce $max_conns by 1 if several simultaneous conns */
					$this->current_max_conns--;
				} else {
					/* Or add/increase delay beetween connexions if only 1 connexion is active */
					if ($this->sleep_delay == 0) $this->sleep_delay = 1;
					else $this->sleep_delay = min($this->max_sleep_delay,$sleep_delay * 2);
				}
				break;
			case 'success':
				if ($this->sleep_delay > 0) {
					/* Decrease delay beetween connexions, if some */
					$this->sleep_delay = round($this->sleep_delay / 3);
				} else if (!$this->failed && ($this->current_max_conns < $this->max_conns)) {
					/* Or, on consecutive successes only, increase the number of simultaneous conns */
					$this->current_max_conns++;
				}
				$this->failed = false;
				break;
		}
	}
}

?>
						