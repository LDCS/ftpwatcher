// ftpwatcher is an FTP Utility with download checking and post processing.
//
package main

import (
    "github.com/LDCS/genutil"
    "github.com/LDCS/qcfg"
    "fmt"
    "os"
    "os/signal"
    "os/exec"
    "log"
    "time"
    "strings"
    "errors"
    "strconv"
    "github.com/LDCS/goftp"
    "path"
    "regexp"
    "io/ioutil"
    "io"
    "github.com/LDCS/compresstype"
    "github.com/LDCS/sflag"
    "encoding/json"
    "github.com/LDCS/cim"
)

var(
    _DISTRIBUTION_CHOICES = []string{"dated-dirs", "dscope"}
	_CONFIG_BLOCK_NAME = "ftp-watcher-param"
	_CONFIG_PARAM_ROW = "ftp-watcher"
	_CONFIG_DISTRIBUTION_ROW = "distribution"
	_CONFIG_DOWNLOAD_CHECK_ROW = "download-check"
	_CONFIG_POST_DOWNLOAD_ROW = "post-download"
	_CONFIG_PROXY_ROW = "proxy"
	_CONFIG_SCHEDULER_ROW = "scheduler"
	_CONFIG_ALERT_ROW = "warn-alert"
	_CONFIG_LMIRROR_ROW = "lmirror"
	_MODE_CHOICES = []string{"mirror", "archive"}
	_SKIP_PATTERNS_SEP = ","
	_SKIP_PATTERNS_LITERAL_SEP = "_COMMA_"
	thread_no = 10
	thread_id = 1
)

var opt = struct{
	Usage         string       "FTP download utility"
	Config        string       "Config file name"
	Inst          string       "Instance number"
	Logbasedir    string       "Log files base directory|/data0/logs/ftpwatcher"
	Alertcmd      string       "Alert command|NOCMD"
}{}

func parseArgs() {

	sflag.Parse(&opt)
    if opt.Config == "" || opt.Inst == "" {
		fmt.Println("Usage: ftpwatcher --Config /path/to/ftpwatcher.cfg --Inst <two digit instance number>")
		panic("Config file path or instance number not specified")
    }
}

// worker can't be a method so it accepts FTPWatcher objectptr
type worker func (*FTPWatcher, map[string]interface{}, int, ...interface{})

type work struct {

    worker_func worker
    fw *FTPWatcher
    watch_data map[string]interface{} // Maps are always passed by ref
    opt []interface{}
    //opt2 interface{}
}

type lmirror_plugin_function func (*FTPWatcher, map[string]interface{}, string, string, ...interface{}) (string, error)

type WatchInfo struct {
    ReportTstamp string
    Numfiles int
    Bytes string // Not to lose precision
    LastDownloadedFilename string
    DownloadDirectory string
    Logfile string
}

type FtpWatcherInfo struct {
    RunStart string
    LastRun string
}

type FTPWatcher struct {
    __proxy_hostname string
    __skip_patterns []string
    _max_retry_attempts int
    _MAX_THREADS int
    _dscope_filename_dates_rec *regexp.Regexp
    __log_dir string
    __log_filename string
    __log_err_filename string
    __json_file string
    __pid_filename string
    __tmp_dir string
    __stats_filename string
    __default_mode string
    // How long to wait in seconds before looping, used in daemon mode
    __loop_wait_time int
    // Command separator used in cfg file instead of ';' which interferes with cfg parsing
    _command_separator string
    _DELETED_SUFFIX string
    _default_permission os.FileMode
    daemon bool
    watchers []map[string]interface{}
    tids []int
    lmirror_plugins map[string]lmirror_plugin_function
    jsondata []map[string]interface{}
    bytes_per_hour map[string][]int64
    total_bytes map[string]int64
}

func newFTPWatcher(watchlist []map[string]interface{}, start_daemon bool) (fw *FTPWatcher) {
    fw = new(FTPWatcher)
    fw.__proxy_hostname = "foo.bar.net"
    fw.__skip_patterns = []string{".", ".."}
    fw._max_retry_attempts = 5
    fw._MAX_THREADS = 10
    fw._dscope_filename_dates_rec, _ = regexp.Compile("(?P<month>\\d{2})(?P<day>\\d{2})\\.")
    fw.__log_dir = opt.Logbasedir + "/%s/"
    fw.__json_file = fmt.Sprintf("%s/ftpwatcher-%s.json", opt.Logbasedir, opt.Inst)
    fw.__log_filename = fmt.Sprintf("ftpwatcher-%s-%s.log", opt.Inst, time.Now().Format("20060102"))
    fw.__log_err_filename = fmt.Sprintf("ftpwatcher-%s.err", opt.Inst)
    fw.__pid_filename = fmt.Sprintf("ftpwatcher-%s.pid", opt.Inst)
    fw.__stats_filename = fmt.Sprintf("ftpwatcher-stats-%s.json", opt.Inst)
    fw.__tmp_dir = fmt.Sprintf("/tmp/ftpwatcher-%s.d", opt.Inst)
    fw.__default_mode = "mirror"
    fw.__loop_wait_time = 300
    fw._command_separator = "^"
    fw._DELETED_SUFFIX = ".DELETED"
    fw._default_permission = 0755
    fw.daemon = start_daemon
    fw.watchers = watchlist
    fw.lmirror_plugins = make(map[string]lmirror_plugin_function)
    fw.jsondata = make([]map[string]interface{}, 1)
    fw.jsondata[0] = make(map[string]interface{})
    fw.bytes_per_hour = make(map[string][]int64)
    fw.total_bytes = make(map[string]int64)
    genutil.EnsureDirOrDie("/tmp", fmt.Sprintf("ftpwatcher-%s.d", opt.Inst))
    if fw.makedir(fmt.Sprintf(fw.__log_dir, ""), fw._default_permission, fw.watchers[0]) == false {
		os.Stdout.WriteString("Could not make ftpwatcher log directory\n")
		os.Exit(1)
    }
    fw._setup_error_logging()
    if fw._check_watch_data(fw.watchers) == false {
		os.Stderr.WriteString("Configuration error in one or more watchers, exiting\n")
		os.Exit(1)
    }
    fw._setup_signal_handling()
    fw._write_pid_file()
    //fw._init_stats()
    fw._start_warn_scheduler(fw.watchers)
    fw.register_lmirror_func("transpath", lmirror_plugin_transpath)
    fw.register_lmirror_func("adaptive-transpath", lmirror_plugin_adaptive_transpath)
    fw.register_lmirror_func("transzip", lmirror_plugin_transzip)
    fw.register_lmirror_func("split", lmirror_plugin_split)
    fw.check_lmirror_cfg_parms()
    go start_cim_server(fw)
    //go gen_stats(fw)
    return
}

func gen_stats(fw *FTPWatcher) {
    prev_total := make(map[string]int64)
    for block, tot := range fw.total_bytes {
		prev_total[block] = tot
    }
    for {
		time.Sleep(time.Minute*10)
		for block, tot := range fw.total_bytes {
			fw.bytes_per_hour[block] = append(fw.bytes_per_hour[block], tot - prev_total[block])
			prev_total[block] = tot
		}
		fw.write_stats_file()
    }
}

func (fw *FTPWatcher) _init_stats() {
    stats_file := fmt.Sprintf(fw.__log_dir, "") + "/" + fw.__stats_filename
    _, err := os.Stat(stats_file)
    if err == nil {
		buf, _ := ioutil.ReadFile(stats_file)
		json.Unmarshal(buf, &fw.bytes_per_hour)
    } else {
		for _, watch_data := range fw.watchers {
			fw.bytes_per_hour[watch_data["blockname"].(string)] = make([]int64, 0)
		}
		fw.write_stats_file()
    }
    for _, watch_data := range fw.watchers {
		fw.total_bytes[watch_data["blockname"].(string)] = 0
    }

}

func (fw *FTPWatcher) write_stats_file() {
    stats_file := fmt.Sprintf(fw.__log_dir, "") + "/" + fw.__stats_filename
    fp, _ := os.OpenFile(stats_file, os.O_CREATE | os.O_WRONLY | os.O_TRUNC, 0666)
    out, _ := json.Marshal(fw.bytes_per_hour)
    fp.Write(out)
    fp.Close()
}

func show_info(data interface{}, cmd string, args ...string) string {
    fw := data.(*FTPWatcher)
    if cmd != "info" { return "" }
    if len(args) < 1 { return "No dotted path provided!" }
    path := strings.Split(args[0], ".")
    if len(path) == 1 {
		if path[0] == "main" {
			out, _ := json.MarshalIndent(fw.jsondata[0], "", "    ")
			return string(out)
		}
    } else if len(path) == 2 {
		if path[0] == "main" {
			content, ok := fw.jsondata[0][path[1]]
			if ok == false { return "Information about " + path[1] + " does not exist" }
			out, _ := json.MarshalIndent(content, "", "    ")
			return string(out)
		}
    }
    return "No information about the path: " + args[0]
}

func start_cim_server(fw *FTPWatcher) {
    cn := new(cim.CimNode)
    cn.IsLeaf = true
    cn.Name = "/"
    cn.Path = "/"
    cn.Children = []*cim.CimNode{}
    cn.Callbacks = make(map[string]cim.CBfunc)
    cn.Callbacks["info"] = show_info
    hostname,_ := os.Hostname()
    cs, err := cim.NewCimServer(hostname, "ftpwatcher" + opt.Inst, cn, fw)
    if err != nil {
		fmt.Println("Cim server failed to start")
        fmt.Println(err)
        return
    }
    cs.Start()

}


func CopyFile(dst, src string) (int64, error) {
    sf, err := os.Open(src)
    if err != nil {
		return 0, err
    }
    defer sf.Close()
    df, err := os.Create(dst)
    if err != nil {
		return 0, err
    }
    defer df.Close()
    return io.Copy(df, sf)
}

func lmirror_plugin_split(fw *FTPWatcher, watch_data map[string]interface{}, destfile, lmirror_file string, opts ...interface{}) (string,error) {
    fi, _ := os.Lstat(destfile)
    modtime := fi.ModTime()
    if lmirror_file == "" {
		// First do transpath
		lmirror_file, _ = lmirror_plugin_transpath(fw, watch_data, destfile, lmirror_file)
    }
    //ok, _ := fw._parse_run_cmd(lmirror_file, watch_data["lmirror_split_cmd"].(string), watch_data)
    meta := destfile + ".meta"
    base, file := path.Split(lmirror_file)
    cmd := watch_data["lmirror_split_cmd"].(string)
    cmd += fmt.Sprintf(" --Inbase %s --Infile %s --Metafile %s --Outbase %s --Tempbase %s", base, file, meta, base, fw.__tmp_dir)
    buf := genutil.BashExecOrDie(false, cmd, fw.__tmp_dir);
    watch_data["logger"].(*log.Logger).Println(watch_data["lmirror_split_cmd"].(string), "output : ", buf)

    err1 := os.Remove(destfile)
    if err1 != nil {
		watch_data["logger"].(*log.Logger).Println(
			"split plugin error : Cannot remove link", destfile)
		return lmirror_file, err1
    }

    // fp, err2 := os.Create(meta)
    // if err2 != nil {
    // 	watch_data["logger"].(*log.Logger).Println(
    // 	    "split plugin error : Cannot create meta file", meta)
    // 	return lmirror_file, err1
    // }
    // // Write tstamp inside the text file
    // //fp.WriteString(modtime.Format(time.RFC822))
    // fp.Close()
    err3 := os.Symlink(meta, destfile)
    if err3 != nil {
		watch_data["logger"].(*log.Logger).Println(
			"transzip plugin error : Cannot create link", destfile, "->", meta)
		return meta, err1
    }
    fw._parse_run_cmd(destfile, "touch -h -t " + modtime.Format("200601021504.05"), watch_data)
    //os.Chtimes(destfile, modtime, modtime)
    return lmirror_file, nil
}

func lmirror_plugin_transzip(fw *FTPWatcher, watch_data map[string]interface{}, destfile, lmirror_file string, opts ...interface{}) (string,error) {
    fi, _ := os.Lstat(destfile)
    modtime := fi.ModTime()
    if lmirror_file == "" {
		// transpath did not run
		newdestfile, err1 := compresstype.Convert(destfile, watch_data["lmirror_zip_fmt"].(string))
		if err1 != nil {
			watch_data["logger"].(*log.Logger).Println(
				"transzip plugin error : Cannot convert type of ", destfile, "to", watch_data["lmirror_zip_fmt"].(string), "Error : ", err1)
			//fmt.Println(err1)
			return destfile, err1
		}
		// err2 := os.Rename(newdestfile, destfile)
		// if err2 != nil {
		//     watch_data["logger"].(*log.Logger).Println("transzip plugin error : Cannot rename", newdestfile, "to", destfile)
		//     return newdestfile, err2
		// }
		// os.Chtimes(destfile, modtime, modtime)
		err2 := os.Symlink(newdestfile, destfile)
		if err2 != nil {
			watch_data["logger"].(*log.Logger).Println("Cannot create symlink ", destfile, "to", newdestfile)
			return newdestfile, err2
		}
		os.Chtimes(newdestfile, modtime, modtime)
		// Can't use Chtimes for changing timestamp of link
		fw._parse_run_cmd(destfile, "touch -h -t " + modtime.Format("200601021504.05"), watch_data)
		return destfile, nil
    }
    newlmfile, err1 := compresstype.Convert(lmirror_file, watch_data["lmirror_zip_fmt"].(string))
    if err1 != nil {
		watch_data["logger"].(*log.Logger).Println(
			"transzip plugin error : Cannot convert type of ", lmirror_file, "to", watch_data["lmirror_zip_fmt"].(string))
		return lmirror_file, err1
    }
    err2 := os.Remove(destfile)
    if err2 != nil {
		watch_data["logger"].(*log.Logger).Println(
			"transzip plugin error : Cannot remove the link", destfile)
		return newlmfile, err1
    }
    err3 := os.Symlink(newlmfile, destfile)
    if err3 != nil {
		watch_data["logger"].(*log.Logger).Println(
			"transzip plugin error : Cannot create link", destfile, "->", newlmfile)
		return newlmfile, err1
    }
    //os.Chtimes(destfile, modtime, modtime)
    fw._parse_run_cmd(destfile, "touch -h -t " + modtime.Format("200601021504.05"), watch_data)
    os.Chtimes(newlmfile, modtime, modtime)
    return newlmfile, nil
}

func lmirror_plugin_transpath(fw *FTPWatcher, watch_data map[string]interface{}, destfile, lmirror_file string, opts ...interface{}) (string,error) {
    fi, _ := os.Stat(destfile)
    modtime := fi.ModTime()
    lm_path := watch_data["lmirror_path_fmt"].(string)
    lm_path = strings.Replace(lm_path, "__CURDATE__", modtime.Format("20060102"), -1)
    lm_path = strings.Replace(lm_path, "__CURDIR__", watch_data["curdir"].(string), -1)
    lm_path = strings.Replace(lm_path, "__SUNDAY__", modtime.AddDate(0, 0, -int(modtime.Weekday())).Format("20060102"), -1)
    r := regexp.MustCompile("[12][90][0-9][0-9][01][0-9][0123][0-9]")
    nominal_date_dir := ""
    if r.MatchString(destfile) == true {
		nominal_date_dir = r.FindString(destfile)
    } else {
		nominal_date_dir = modtime.Format("undated/20060102")
    }
    lm_path = strings.Replace(lm_path, "__NOMINAL_DATE__", nominal_date_dir, -1)

    dst := ""
    if fw.makedir( lm_path, fw._default_permission, watch_data) == false {
		watch_data["logger"].(*log.Logger).Println("transpath plugin error : Cannot make lmirror_dir = ", lm_path)
		return destfile, errors.New("Cannot make lmirror_dir = " + lm_path)
    } else {
		_, fname := path.Split(destfile)
		dst = path.Join(lm_path, fname)
		_, err := os.Stat(dst)
		if err == nil {
			old := dst + ".old"
			os.Remove(old)
			watch_data["logger"].(*log.Logger).Println("transpath: ALERT :", dst, "exists, taking a backup before overwriting.")
			os.Rename(dst, old)
		}
		//os.Remove(dst)
		_, err = CopyFile(dst, destfile)
		if err != nil {
			watch_data["logger"].(*log.Logger).Printf("transpath plugin error : Cannot copy %s to %s\n", destfile, dst)
			return destfile, err
		}
		err = os.Remove(destfile)
		if err != nil {
			watch_data["logger"].(*log.Logger).Printf("transpath plugin error : Cannot remove %s\n", destfile)
			return destfile, err
		}
		err = os.Symlink(dst, destfile)
		if err != nil {
			watch_data["logger"].(*log.Logger).Printf("transpath plugin error : Cannot create symlink %s->%s\n", destfile, dst)
			return dst, err
		}
		//os.Chtimes(destfile, modtime, modtime)
		fw._parse_run_cmd(destfile, "touch -h -t " + modtime.Format("200601021504.05"), watch_data)
		watch_data["logger"].(*log.Logger).Println("transpath : finished running touch -h -t on", destfile, "Now changing timestamp of", dst)
		os.Chtimes(dst, modtime, modtime)
		watch_data["logger"].(*log.Logger).Println("transpath : Finished changing timestamp of", dst)
    }

    return dst, nil
}

func lmirror_plugin_adaptive_transpath(fw *FTPWatcher, watch_data map[string]interface{}, destfile, lmirror_file string, opts ...interface{}) (string,error) {
    fi, _ := os.Stat(destfile)
    modtime := fi.ModTime()
    t1 := opts[0].(time.Time)
    t2 := opts[1].(time.Time)
    lm_path_orig := watch_data["lmirror_path_fmt"].(string)
    lm_path_orig = strings.Replace(lm_path_orig, "__CURDATE__", modtime.Format("20060102"), -1)
    lm_path_orig = strings.Replace(lm_path_orig, "__CURDIR__", watch_data["curdir"].(string), -1)
    lm_path_orig = strings.Replace(lm_path_orig, "__SUNDAY__", modtime.AddDate(0, 0, -int(modtime.Weekday())).Format("20060102"), -1)
    lm_path := ""
    lm_path_old := ""
    lm_path_new := ""
    meta := destfile + ".meta"
    _, err := os.Stat(meta)
    oldperiod := ""
    period := ""
    var ccfg *qcfg.CfgBlock = nil
    if err == nil {
		// Metafile exists
		ccfg = qcfg.NewCfg(meta, meta, false)
		oldperiod = ccfg.Str("ftpwatcher", "file", "periodicity", "")
		watch_data["logger"].(*log.Logger).Println("Old period for ", destfile, "is", oldperiod)
    }    
    if t1.IsZero() {
		// This is the first download
		lm_path = lm_path_orig
    } else {
		lm_path_new_daily := path.Join(lm_path_orig, t2.Format("20060102"))
		lm_path_old_daily := path.Join(lm_path_orig, t1.Format("20060102"))
		lm_path_new_weekly := path.Join(lm_path_orig, t2.AddDate(0, 0, -int(t2.Weekday())).Format("20060102"))
		lm_path_old_weekly := path.Join(lm_path_orig, t1.AddDate(0, 0, -int(t1.Weekday())).Format("20060102"))
		if t2.Weekday() == time.Sunday {
			lm_path_new_weekly = path.Join(lm_path_orig, t2.AddDate(0, 0, -7).Format("20060102"))
		}
		if t1.Weekday() == time.Sunday {
			lm_path_old_weekly = path.Join(lm_path_orig, t1.AddDate(0, 0, -7).Format("20060102"))
		}
		lm_path_new_monthly := path.Join(lm_path_orig, t2.Format("200601"))
		lm_path_old_monthly := path.Join(lm_path_orig, t1.Format("200601"))

		if err != nil {
			// No metafile 
			ccfg = qcfg.NewCfgMem(meta)
		}
		timediff := t2.Sub(t1)
		if timediff < 2*24*time.Hour {
			//Daily update cycle
			lm_path_new = lm_path_new_daily
			lm_path_old = lm_path_old_daily
			period = "daily"
			if oldperiod == "weekly" {
				period = "weekly"
				lm_path_new = lm_path_new_weekly
				lm_path_old = lm_path_old_weekly
				watch_data["logger"].(*log.Logger).Println("adaptive transpath plugin has detected daily update cycle for", destfile, "but using old period weekly")
			} else if oldperiod == "monthly" {
				period = "monthly"
				lm_path_new = lm_path_new_monthly
				lm_path_old = lm_path_old_monthly
				watch_data["logger"].(*log.Logger).Println("adaptive transpath plugin has detected daily update cycle for", destfile, "but using old period monthly")
			} else {
				watch_data["logger"].(*log.Logger).Println("adaptive transpath plugin has detected daily update cycle for", destfile)
			}
		} else if timediff < 8*24*time.Hour {
			// Weekly update cycle
			lm_path_new = lm_path_new_weekly
			lm_path_old = lm_path_old_weekly
			period = "weekly"
            if oldperiod == "monthly" {
				period = "monthly"
				lm_path_new = lm_path_new_monthly
                lm_path_old = lm_path_old_monthly
				watch_data["logger"].(*log.Logger).Println("adaptive transpath plugin has detected weekly update cycle for", destfile, "but using old period monthly")
            } else {
				watch_data["logger"].(*log.Logger).Println("adaptive transpath plugin has detected weekly update cycle for", destfile)
			}
		} else if timediff < 2*31*24*time.Hour {
			// Monthly update cycle
			period = "monthly"
			lm_path_new = lm_path_new_monthly
			lm_path_old = lm_path_old_monthly	    
			watch_data["logger"].(*log.Logger).Println("adaptive transpath plugin has detected monthly update cycle for", destfile)
		}
		ccfg.EditEntry("ftpwatcher", "file", "periodicity", period)
		ccfg.CfgWrite(meta)
		lm_path = lm_path_new
		_, fname := path.Split(destfile)
		_, err1 := os.Stat(path.Join(lm_path_orig, fname))
		_, err2 := os.Stat(path.Join(lm_path_old, fname))
		if err1 == nil && err2 != nil {
			if fw.makedir(lm_path_old, fw._default_permission, watch_data) == false {
				watch_data["logger"].(*log.Logger).Println("adaptive-transpath plugin error : Cannot make lm_path_old = ", lm_path_old)
			}
			_, err := CopyFile(path.Join(lm_path_old, fname), path.Join(lm_path_orig, fname))
			if err != nil {
				watch_data["logger"].(*log.Logger).Printf("adaptive-transpath plugin error : Cannot copy %s to %s\n", path.Join(lm_path_orig, fname), path.Join(lm_path_old, fname))
            } else {
				err = os.Remove(path.Join(lm_path_orig, fname))
				if err != nil {
					watch_data["logger"].(*log.Logger).Printf("adaptive-transpath plugin error : Cannot remove %s\n", path.Join(lm_path_orig, fname))
				}
				os.Chtimes(path.Join(lm_path_old, fname), t1, t1)
			}
			
		}
    }
    dst := ""
    if fw.makedir( lm_path, fw._default_permission, watch_data) == false {
		watch_data["logger"].(*log.Logger).Println("adaptive-transpath plugin error : Cannot make lmirror_dir = ", lm_path)
		return destfile, errors.New("Cannot make lmirror_dir = " + lm_path)
    } else {
		_, fname := path.Split(destfile)
		dst = path.Join(lm_path, fname)
		_, err := os.Stat(dst)
		if err == nil {
			old := dst + ".old"
			os.Remove(old)
			watch_data["logger"].(*log.Logger).Println("adaptive-transpath: ALERT :", dst, "exists, taking a backup before overwriting.")
			os.Rename(dst, old)
		}
		_, err = CopyFile(dst, destfile)
		if err != nil {
			watch_data["logger"].(*log.Logger).Printf("adaptive-transpath plugin error : Cannot copy %s to %s\n", destfile, dst)
			return destfile, err
		}
		err = os.Remove(destfile)
		if err != nil {
			watch_data["logger"].(*log.Logger).Printf("adaptive-transpath plugin error : Cannot remove %s\n", destfile)
			return destfile, err
		}
		err = os.Symlink(dst, destfile)
		if err != nil {
			watch_data["logger"].(*log.Logger).Printf("adaptive-transpath plugin error : Cannot create symlink %s->%s\n", destfile, dst)
			return dst, err
		}
		//os.Chtimes(destfile, modtime, modtime)
		fw._parse_run_cmd(destfile, "touch -h -t " + modtime.Format("200601021504.05"), watch_data)
		os.Chtimes(dst, modtime, modtime)
		//fmt.Println(modtime)
    }

    return dst, nil
}

func (fw *FTPWatcher) register_lmirror_func(name string, lmirror_plugin_func lmirror_plugin_function) {
    fw.lmirror_plugins[name] = lmirror_plugin_func
}

func (fw *FTPWatcher) check_lmirror_cfg_parms() {
    // watchData["use_lmirror_plugins"] = lmirror_plugins_to_use
    // watchData["lmirror_path_fmt"] = lmirror_path_fmt
    // watchData["lmirror_zip_fmt"] = lmirrot_zip_fmt
    // watchData["lmirror_split_cmd"] = lmirror_split_cmd
    for _,watch_data := range fw.watchers {
		
		if watch_data["use_lmirror_plugins"] == "" || watch_data["use_lmirror_plugins"] == nil {
			watch_data["use_lmirror_plugins"] = nil
			continue
		}

		use_plugins := make(map[string]bool)
		for _, plugin := range watch_data["use_lmirror_plugins"].([]string) {
			use_plugins[plugin] = false
			if _, exists := fw.lmirror_plugins[plugin]; exists == false {
				watch_data["logger"].(*log.Logger).Printf("Error : No lmirror plugin called %s available.\n", plugin)
			} 
			if plugin == "transpath" || plugin == "split" {
				if watch_data["lmirror_path_fmt"] == "" {
					watch_data["logger"].(*log.Logger).Println(
						"Error: No lmirror_path_fmt option in cfg with plugin", plugin)
				} else {
					if plugin == "split" && watch_data["lmirror_split_cmd"] == "" {
						watch_data["logger"].(*log.Logger).Println(
							"Error: No lmirror_split_cmd option in cfg with plugin", plugin)
					} else {
						use_plugins[plugin] = true
					}
				}
			} else {
				use_plugins[plugin] = true
			}
		}
		watch_data["use_lmirror_plugins"] = use_plugins
    }
}

func (fw *FTPWatcher) write_json_file() {
    fp, _ := os.OpenFile(fw.__json_file, os.O_CREATE | os.O_WRONLY | os.O_TRUNC, 0666)
    out, _ := json.Marshal(fw.jsondata)
    fp.Write(out)
    fp.Close()
}

func (fw *FTPWatcher) _start_warn_scheduler( watchers []map[string]interface{} ) {
    /*
     Start warn scheduler thread for all watchers
     */
    for _, watch_data := range watchers {
		warn_time, exists := watch_data["warn_time"]
		if exists == true && warn_time != "" {
			watch_data["logger"].(*log.Logger).Println("Starting warn scheduler")
			warn_in_q := watch_data["warn_in_q"].(chan work)
			fw._start_threads(warn_in_q, 1)
			fw._add_work_to_chan(watch_data, warn_in_q, warn_scheduler)
		}
    }
}

func (fw *FTPWatcher) _add_work_to_chan(watch_data map[string]interface{}, in_q chan work, worker_func worker, opts...interface{}) {
    work_ptr := new(work)
    work_ptr.worker_func = worker_func
    work_ptr.watch_data = watch_data
    work_ptr.fw = fw
    // if len(opts) == 1 {
    // 	work_ptr.opt1 = opts[0]
    // } else if len(opts) == 2 {
    // 	work_ptr.opt1 = opts[0]
    // 	work_ptr.opt2 = opts[1]
    // }
    work_ptr.opt = opts
    // Send work down the channel
    in_q<-*work_ptr
}

func (fw *FTPWatcher) _split_run_cmd(command string) []string {
    /*
     Splits commands in the case of multiple commands
     */
    return strings.Split(command, fw._command_separator)
}

func (fw *FTPWatcher) _parse_run_cmd(filepath, command string, watch_data map[string]interface{}) (bool, string) {
    /*
     Parses command string and runs command on given filepath
     */
    parts := strings.Fields(command)
    env := []string{}
    cmd := []string{}
    for _, part := range parts {
		if strings.Index(part, "=") != -1 {
			env = append(env, part)
		} else {
			cmd = append(cmd, part)
		}
    }
    cmd = append(cmd, filepath)
    watch_data["logger"].(*log.Logger).Printf("Running cmd %s with ENV args %v\n", command, env)
    c := exec.Command(cmd[0], cmd[1:]...)
    c.Env = env
    out, _ := c.CombinedOutput()
    success := c.ProcessState.Success()
    return success, string(out)
}

func (fw *FTPWatcher) warn_checker(filepath, warn_cmd string, watch_data map[string]interface{}) (bool) {
    /*
     Run warn_cmd on filepath and return True/False
     on command success or failure respectively
     */
    for _, command := range fw._split_run_cmd(warn_cmd) {
		success, stderr := fw._parse_run_cmd(filepath, command, watch_data)

		if success == false {
			watch_data["logger"].(*log.Logger).Printf(
				"Scheduled warn command has exited with failure.\n%s - filepath %s\n",
				stderr, filepath)
			return false
		}
    }
    return true
}

func (fw *FTPWatcher) mailer(recp string, watch_data map[string]interface{}, subject, body, key string) {
    /*
     Sent Email
     */
    watch_data["logger"].(*log.Logger).Printf("Sending warn alert email : %s\n", subject)
    /*
     TODO
     */
}

func warn_scheduler(fw *FTPWatcher, watch_data map[string]interface{}, tid int, opts...interface{}) {
    /*
     Scheduler for running warn_cmd
     */
    for {
		tz, exists := watch_data["tz"]
		now := time.Now()
		if exists && (tz!="") {
			now = fw._convert_local_timezone(time.Now(), watch_data["tz"].(string))
		}
		wt := watch_data["warn_time"].(time.Time)
		warn_time := time.Date(now.Year(), now.Month(), now.Day(), wt.Hour(), wt.Minute(), wt.Second(), 0, now.Location())
		if now.After(warn_time) {
			// We are past today's warn time
			wt1 := warn_time.AddDate(0, 0, 1) // Next warn time
			sleep_duration := wt1.Sub(now)
			watch_data["logger"].(*log.Logger).Printf("Next warn_time is %s. Sleeping for %s\n", wt1, sleep_duration)
			time.Sleep(sleep_duration) // Sleep till then
		} else {
			sleep_duration := warn_time.Sub(now)
			watch_data["logger"].(*log.Logger).Printf("Next warn_time is %s. Sleeping for %s\n", warn_time, sleep_duration)
			time.Sleep(sleep_duration)
		}
		if fw.warn_checker(watch_data["dest"].(string), watch_data["warn_cmd"].(string), watch_data) == false {
			watch_data["logger"].(*log.Logger).Println("Warn cmd(s) ended with failure - running warn alert")
			fw.mailer(watch_data["warn_alert_recp"].(string), watch_data,
				watch_data["warn_alert_subj"].(string),
				watch_data["warn_alert_body"].(string),
				"warn_alert")
		} else {
			watch_data["logger"].(*log.Logger).Println("Warn cmd(s) finished successfully")
		}
    }
}

func (fw *FTPWatcher) _write_pid_file() {
    fl, _ := os.OpenFile(fmt.Sprintf(fw.__log_dir, "") + string(os.PathSeparator) + fw.__pid_filename,
		os.O_WRONLY|os.O_CREATE, fw._default_permission)
    fl.WriteString(strconv.FormatInt(int64(os.Getpid()),10) + "\n")
    fl.Close()
}

func (fw *FTPWatcher) _setup_signal_handling() {
    // capture ctrl+c and log the info
    c := make(chan os.Signal, 1)
    signal.Notify(c, os.Interrupt, os.Kill)
    go func() {
		for sig := range c {
			os.Stdout.WriteString(fmt.Sprintf("Captured %v, stopping ftpwatcher and exiting..\n", sig))
			fw.quit_signal_handler(sig)
			os.Exit(0)
		}
    }()    
}

func (fw *FTPWatcher) quit_signal_handler(sig os.Signal) {
    for _, watch_data := range fw.watchers {
		watch_data["logger"].(*log.Logger).Printf("Got signal %v, exiting" , sig)
    }
    old := fw.jsondata[0]["ftpwatcher"].(FtpWatcherInfo)
    fw.jsondata[0]["ftpwatcher"] = FtpWatcherInfo{old.RunStart, time.Now().Format("20060102 15:04:05")}
    fw.write_json_file()
    fw.write_stats_file()
}

func (fw *FTPWatcher) _convert_local_timezone(local_time time.Time, timezone string) time.Time {
    loc, _ := time.LoadLocation(timezone)
    return local_time.In(loc)
}

func (fw *FTPWatcher) _setup_logger(debug bool, log_dir, blockname string) (lgr *log.Logger){
    fl, err := os.OpenFile(log_dir + string(os.PathSeparator) + fw.__log_filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, fw._default_permission)
    if err != nil {
        fmt.Println("Unable to setup logging: ", err)
    }
    return log.New(fl, "[Block Name : " + blockname + "] ", log.LstdFlags)
}

func (fw *FTPWatcher) _thread_worker(in_q chan work, tid int) {
    for w := range in_q {
		w.worker_func(w.fw, w.watch_data, tid, w.opt...)
		// Can also push the outputs to out_q
    }
}

func (fw *FTPWatcher) _start_threads(in_q chan work, thread_no int) {
    /*
     Spawn as many Go routines as we are allowed
     Go runtime will take care of rest and if we have lots
     of data in input queue they will be executed as soon as there's
     a goroutine free
     */
    for i:=0 ; i < thread_no ; i++ {
		go fw._thread_worker(in_q, thread_id)
		thread_id = thread_id + 1
    }
}

func (fw *FTPWatcher) _check_alert( watch_data map[string]interface{} ) bool {
    /*
     Check warn-alert email settings
     */
    warn_alert_recp, exists1 := watch_data["warn_alert_recp"]
    warn_alert_subj, exists2 := watch_data["warn_alert_subj"]
    warn_alert_body, exists3 := watch_data["warn_alert_body"]
    if !(exists1 && exists2 && exists3 && (warn_alert_body!="") &&
		(warn_alert_subj!="") && (warn_alert_recp!="")) {
		return false
    }
    return true
}

func parseInt(num string) (n int, err error) {
    x, err := strconv.ParseInt(num, 10, 32)
    n = int(x)
    return
}

func (fw *FTPWatcher) _parse_time(timestr string, today time.Time, _logger *log.Logger) (time.Time, error) {
    /*
     Parse timestr and return Time object
     */
    if len(timestr) != 6 {
		_logger.Println(timestr, "does not look like a valid timestamp!")
		return time.Now(), errors.New("Not a valid timestamp")
    }
    hour, err1 := parseInt(timestr[0:2])
    minutes, err2 := parseInt(timestr[2:4])
    seconds, err3 := parseInt(timestr[4:6])
    if err1!=nil || err2!=nil || err3!=nil {
		_logger.Println(timestr, "does not look like a valid timestamp!")
		return time.Now(), errors.New("Not a valid timestamp")
    }
    if !( hour<24 && hour>=0 && minutes<60 && minutes>=0 && seconds<60 && seconds>=0 ) {
		_logger.Println(timestr, "does not look like a valid timestamp!")
		return time.Now(), errors.New("Not a valid timestamp")
    }
    return time.Date(today.Year(), today.Month(), today.Day(), hour, minutes, seconds, 0, today.Location()), nil
}

func (fw *FTPWatcher) _check_scheduler( watch_data map[string]interface{} ) bool {
    /*
     Checks and parses scheduler settings
     returns None on error
     */
    start_time, exist := watch_data["start_time"]
    if exist && (start_time!="") {
		st, err := fw._parse_time(start_time.(string),
			watch_data["today"].(time.Time),
			watch_data["logger"].(*log.Logger))
		if err == nil {
			watch_data["start_time"] = st
		} else {
			return false
		}
    } else {
		return false
    }
    
    end_time, exists := watch_data["end_time"]
    if !(exists && (end_time!="")) {
		watch_data["logger"].(*log.Logger).Println("Scheduler lacking end time setting, exiting.")
		return false
    }
    et, err := fw._parse_time(end_time.(string),
		watch_data["today"].(time.Time),
		watch_data["logger"].(*log.Logger))
    
    if err == nil {
		watch_data["end_time"] = et
    } else {
		return false
    }
    warn_time, exists := watch_data["warn_time"]
    if exists && (warn_time != "") {
		warn_cmd, exists := watch_data["warn_cmd"]
		if !(exists && (warn_cmd != "")) {
			watch_data["logger"].(*log.Logger).Println("Warn time with no warn_cmd setting.")
			return false
		} else if fw._check_alert(watch_data) == false {
			watch_data["logger"].(*log.Logger).Println("Configuration error - Warn time with no warn-alert email settings.")
			return false
		}
		wt, err := fw._parse_time(warn_time.(string),
			watch_data["today"].(time.Time),
			watch_data["logger"].(*log.Logger))
		if err == nil {
			watch_data["warn_time"] = wt
		} else {
			return false
		}
    }
    return true
}

func (fw *FTPWatcher) _check_watch_data( watches []map[string]interface{} ) bool {
    /*
     Goes through all watch data and creates loggers, ftp server objects and
     logs in to each ftp server.
     */
    for _, watch_data := range watches {
		tz, exists := watch_data["tz"]
		if exists && (tz != "") {
			watch_data["today"] = fw._convert_local_timezone(time.Now(), tz.(string))
		} else {
			watch_data["today"] = time.Now()
		}
		server_tz, exists := watch_data["server_tz"]
		if ! (exists && (server_tz != "")) {
			watch_data["server_tz"] = nil
		}

		//Required parameters
		hostname, exists := watch_data["hostname"]
		if ! (exists && hostname != "") {
			os.Stderr.WriteString("No hostname in data !\n")
			return false
		}
		user, exists1 := watch_data["user"]
		passwd, exists2 := watch_data["passwd"]
		if !( exists1 && user != "" && exists2 && passwd != "" ) {
			os.Stderr.WriteString("No username or password in data!\n")
			return false
		}
		block_name, _ := watch_data["block_name"]
		watch_data["log_dir"] = fmt.Sprintf(fw.__log_dir , block_name.(string))
		if fw.makedir(watch_data["log_dir"].(string), fw._default_permission, watch_data) == false {
			os.Stdout.WriteString(fmt.Sprintf("Could not create log directory %s!\n", watch_data["log_dir"].(string)))
			return false
		}
		debug, exist := watch_data["debug"]
		watch_data["debug"] = false
		if exist == true && debug == 1 {
			watch_data["debug"] = true
		}
		watch_data["logger"] = fw._setup_logger(watch_data["debug"].(bool),
			watch_data["log_dir"].(string),
			watch_data["block_name"].(string))

		dest, exist := watch_data["dest"]
		if !(exist && (dest != "")) {
			watch_data["logger"].(*log.Logger).Println("No destination path specified!")
			os.Exit(1)
		}
		mode, exist := watch_data["mode"]
		if !(exist && (mode != "")) {
			watch_data["mode"] = fw.__default_mode
		}
		if fw.makedir(watch_data["dest"].(string), fw._default_permission, watch_data) == false {
			watch_data["logger"].(*log.Logger).Println("Could not make destination path directory, exiting")
			os.Exit(1)
		}
		post_download, exists := watch_data["post_download"]
		lm, exists1 := watch_data["use_lmirror_plugins"]
		if (exists && (post_download != "")) || (exists1 && (lm != "")) {
			if exists == false {
				watch_data["post_download"] = nil
			}
			tn, exists := watch_data["thread_no"]
			if !(exists && tn!=0) {
				thread_no = fw._MAX_THREADS
			} else {
				thread_no = watch_data["thread_no"].(int)
			}
			
			watch_data["post_process_in_queue"] = make(chan work, 10)
			//watch_data['post_process_out_queue'] = make(chan work, 10)
			
			//Pre start threads. They will get input from
			//queue and start executing once queue has data
			fw._start_threads(watch_data["post_process_in_queue"].(chan work),
				//watch_data['post_process_out_queue"],
				thread_no)
		} else {
			watch_data["post_download"] = nil
		}
		download_check, exists := watch_data["download_check"]
		if !(exists && (download_check!="")) {
			watch_data["download_check"] = nil
		}
		newer_than_days, exists := watch_data["newer_than_days"]
		if exists && (newer_than_days != "") {
			watch_data["newer_than"] = watch_data["today"].(time.Time).AddDate(0, 0, -watch_data["newer_than_days"].(int))
		} else {
			watch_data["newer_than"] = nil
		}
		proxy_host, exist := watch_data["proxy_host"]
		if !(exist && (proxy_host!="")) {
			watch_data["proxy_host"] = fw.__proxy_hostname
		}
		skip_patterns, exists := watch_data["skip_patterns"]
		if exists && (len(skip_patterns.([]string)) > 0) {
			watch_data["skip_patterns"] = append(watch_data["skip_patterns"].([]string), fw.__skip_patterns...)
		} else {
			watch_data["skip_patterns"] = fw.__skip_patterns
		}
		if fw._check_scheduler(watch_data) == false {
			watch_data["logger"].(*log.Logger).Println("Configuration error in scheduler, exiting")
			//fmt.Println("check_scheduler == false")
			os.Exit(1)
		} else {
			watch_data["warn_in_q"] = make(chan work, 10)
			//watch_data["warn_out_q"] = make(chan work, 10)
		}
		log_new := watch_data["log_dir"].(string) + string(os.PathSeparator) + fw.__log_filename
		hostn, _ := os.Hostname()
		hostn = strings.SplitN(hostn, ".", 2)[0]
		log_new = strings.Replace(log_new, "/data0/logs", "/data0/nfs/logs/" + hostn, 1)
		fw.jsondata[0][watch_data["blockname"].(string)] = WatchInfo{time.Now().Format("20060102 15:04:05"), 0, "0", "", "", log_new}
    }
    fw.jsondata[0]["ftpwatcher"] = FtpWatcherInfo{time.Now().Format("20060102 15:04:05"), time.Now().Format("20060102 15:04:05")}
    fw.write_json_file()
    return true
}


func (fw *FTPWatcher) _setup_error_logging() {
    // Redirects stderr to file
    err_file_handle, _ := os.OpenFile(fmt.Sprintf(fw.__log_dir, "") + string(os.PathSeparator) + fw.__log_err_filename,
		os.O_APPEND|os.O_CREATE | os.O_WRONLY| os.O_SYNC, fw._default_permission)
    os.Stderr = err_file_handle
    os.Stdout = err_file_handle
    return
}

func (fw *FTPWatcher) makedir( pathname string, mode os.FileMode, watch_data map[string]interface{}) bool {
    /*
     Create a directory if it doesn't exist.  Recursively create the
     parent directory as well if needed.
     */
    if b, _ := path_exists(pathname); b == true {
		return true
    }
    err := os.MkdirAll(pathname, mode)
    if err != nil {
		logger, exists := watch_data["logger"]
		if exists {
			logger.(*log.Logger).Printf("Could not create the dir %s. Error : %s", pathname, err)
			return false
		}
    }
    return true
}

func path_exists(path string) (bool, error) {
    _, err := os.Stat(path)
    if err == nil { return true, nil }
    if os.IsNotExist(err) { return false, nil }
    return false, err
}

func (fw *FTPWatcher) get_schedule(watch_data map[string]interface{}) (start_time, end_time, now time.Time){
	start_time = time.Time{}
	end_time = time.Time{}
	now = time.Time{}
    st, exists1 := watch_data["start_time"]
    et, exists2 := watch_data["end_time"]
    tz, exists3 := watch_data["tz"]
    if !(exists1 && exists2 && (st != nil) && (et != nil)) {
		return
    }
    now = time.Now()
    if exists3 && (tz != "") {
		now = fw._convert_local_timezone(time.Now(), tz.(string))
    }
    start_time = st.(time.Time)
    end_time = et.(time.Time)
    start_time = time.Date(now.Year(), now.Month(), now.Day(),
		start_time.Hour(), start_time.Minute(), start_time.Second(), 0, now.Location())
    end_time = time.Date(now.Year(), now.Month(), now.Day(),
		end_time.Hour(), end_time.Minute(), end_time.Second(), 0, now.Location())
	return
}

func (fw *FTPWatcher) check_schedule(watch_data map[string]interface{}) {
    /*
     Check for schedule, wait if not in run period
     */

	start_time, end_time, now := fw.get_schedule(watch_data)

	if start_time.IsZero() == true && end_time.IsZero() == true {
		return
	}

    if now.After(end_time) {
		// We are past today's end time
		st1 := start_time.AddDate(0, 0, 1) // Next warn time
		sleep_duration := st1.Sub(now)
		watch_data["logger"].(*log.Logger).Printf("Time now %s not in schedule %s-%s, sleeping until %s\n",
			now, st1, end_time.AddDate(0, 0, 1), sleep_duration)
		time.Sleep(sleep_duration) 
    } else if now.Before(start_time) {
		sleep_duration := start_time.Sub(now)
		watch_data["logger"].(*log.Logger).Printf("Time now %s not in schedule %s-%s, sleeping until %s\n",
			now, start_time, end_time, sleep_duration)
		time.Sleep(sleep_duration)
    }

}

func (fw *FTPWatcher) adjust_stale_time(watch_data map[string]interface{}) {
    skip_dirRfile_time_staler_than_days, exists := watch_data["skip_dirRfile_time_staler_than_days"]
    if exists {
		td := time.Now()
		tz, exists3 := watch_data["tz"]
		if exists3 && (tz != "") {
			td = fw._convert_local_timezone(time.Now(), tz.(string))
		}
		watch_data["_skip_dirRfile_time_staler_than_days"] = td.AddDate(0, 0, -skip_dirRfile_time_staler_than_days.(int))
    } else {
		watch_data["_skip_dirRfile_time_staler_than_days"] =  nil
    }
    skip_file_time_staler_than_days, exists := watch_data["skip_file_time_staler_than_days"]
    if exists {
		td := time.Now()
		tz, exists3 := watch_data["tz"]
		if exists3 && (tz != "") {
			td = fw._convert_local_timezone(time.Now(), tz.(string))
		}
		watch_data["_skip_file_time_staler_than_days"] = td.AddDate(0, 0, -skip_file_time_staler_than_days.(int))
		//fmt.Println("_skip_file_time_staler_than_days = ", watch_data["_skip_file_time_staler_than_days"])
    } else {
		watch_data["_skip_file_time_staler_than_days"] =  nil
    }
    skip_dir_name_staler_than_days, exists := watch_data["skip_dir_name_staler_than_days"]
    if exists {
		td := time.Now()
		tz, exists3 := watch_data["tz"]
		if exists3 && (tz != "") {
			td = fw._convert_local_timezone(time.Now(), tz.(string))
		}
		watch_data["_skip_dir_name_staler_than_days"] = td.AddDate(0, 0, -skip_dir_name_staler_than_days.(int))
    } else {
		watch_data["_skip_dir_name_staler_than_days"] =  nil
    }
}

func (fw *FTPWatcher) _connect_login_ftp(watch_data map[string]interface{}) bool {
    use_proxy, exists := watch_data["use_proxy"]
    hostname := watch_data["hostname"].(string)
	list_internal_read_timeout := (time.Second*60)
	if _, ok := watch_data["list_internal_read_timeout"]; ok == true { list_internal_read_timeout = watch_data["list_internal_read_timeout"].(time.Duration) }
    if !(exists && use_proxy != 0) {
		sv, err1 := ftp.Connect(hostname, list_internal_read_timeout)
		watch_data["ftp_server"] = sv
		if err1 != nil {
			watch_data["logger"].(*log.Logger).Printf(
				"Could not establish a connection to host, not continuing for hostname %s\n", hostname)
			watch_data["ftp_server"] = nil
			return false
		}
		err2 := watch_data["ftp_server"].(*ftp.ServerConn).Login(watch_data["user"].(string), watch_data["passwd"].(string))
		if err2 != nil {
			watch_data["logger"].(*log.Logger).Printf(
				"Could not login to %s as %s\n", hostname, watch_data["user"].(string))
			if watch_data["ftp_server"] != nil {
				watch_data["ftp_server"].(*ftp.ServerConn).Quit()
			}
			watch_data["ftp_server"] = nil
			return false
		}
    } else {
		proxy_host := watch_data["proxy_host"].(string)
		srv, err1 := ftp.Connect(proxy_host, list_internal_read_timeout)
		watch_data["ftp_server"] = srv
		if err1 != nil {
			watch_data["logger"].(*log.Logger).Printf(
				"Could not establish a connection to host, not continuing for proxy hostname %s\n", proxy_host)
			watch_data["ftp_server"] = nil
			return false
		}
		err2 := watch_data["ftp_server"].(*ftp.ServerConn).Login(
			watch_data["user"].(string) + "@" + hostname, watch_data["passwd"].(string))
		if err2 != nil {
			watch_data["logger"].(*log.Logger).Printf(
				"Could not login to %s as %s\n", hostname, watch_data["user"].(string))
			if watch_data["ftp_server"] != nil {
				watch_data["ftp_server"].(*ftp.ServerConn).Quit()
			}
			watch_data["ftp_server"] = nil
			return false
		}
		
    }
	watch_data["lastconnectat"] = time.Now()
    return true
}

func (fw *FTPWatcher) _check_directory(dirpath string, watch_data map[string]interface{}) bool {
    /*
     Check if dirpath exists locally,
     try to create if not
     */
    return fw.makedir( dirpath, fw._default_permission, watch_data )
}

func (fw *FTPWatcher) _write_directory_list(listing []*ftp.FTPListData,
    filename string, watch_data map[string]interface{}) {
    /*
     Writes directory listing to filename
     */
    fp, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, fw._default_permission)
    if err != nil {
		watch_data["logger"].(*log.Logger).Printf("Error writing file %s\n", filename)
		return
    }
    for _, line := range listing {
		fp.WriteString(line.RawLine)
    }
    fp.Close()
}

func (fw *FTPWatcher) _skip_pattern(filename string, watch_data map[string]interface{}) bool {
    /*
     Checks if filename is in skip patterns.
     Returns True if so, False otherwise
     */
    
    _skip := false
    for _, pat := range watch_data["skip_patterns"].([]string) {
		matched, _ := path.Match(pat, filename)
		if matched == true {
			watch_data["logger"].(*log.Logger).Printf("Skip pattern %s matches %s\n", pat, filename)
			_skip = true
			break
		}
    }
    return _skip
}

func (fw *FTPWatcher) _adjust_filedate_tz(datestamp time.Time, tz, server_tz string) time.Time {
    /*
     Returns datetime object from file's timestamp and
     timezone settings
     */
    dt := datestamp.UTC()  // IMP : To get time in server's tz we need to call UTC()
    if server_tz != "" {
		loc, _ := time.LoadLocation(server_tz)
		dt = time.Date(dt.Year(), dt.Month(), dt.Day(), dt.Hour(), dt.Minute(), dt.Second(), 0, loc)
    }
    if tz != "" {
		loc, _ := time.LoadLocation(tz)
		dt = dt.In(loc)
    }
    if (server_tz != "") && tz == "" {
		// If server_tz is given but no TZ, use local TZ
		dt = dt.Local()
    }
    return dt
}

func parse_dated_dir(dateddir string) (time.Time, error) {
    loc, _ := time.LoadLocation("UTC")
    if len(dateddir) == 8 {
		year, err1 := parseInt(dateddir[0:4])
		month, err2 := parseInt(dateddir[4:6])
		day, err3 := parseInt(dateddir[6:8])
		if (err1 != nil) && (err2 != nil ) && (err3 != nil ) {
			return time.Date(year,time.Month(month), day, 0, 0, 0, 0, loc), nil
		} else {
			return time.Now(), errors.New("Not a dated dir")
		}
    }
    return time.Now(), errors.New("Not a dated dir")
}

func (fw *FTPWatcher) _check_distribution_method(filename string,
    remotefile_datetime time.Time, localdir string, watch_data map[string]interface{}) string {
    /*
     Checks distribution method and returns directory
     to save file into
     */
    move_to_dir := ""
    if watch_data["distribution"] == nil {
		return move_to_dir
    }
    if watch_data["distribution"] == "dated-dirs" {
		move_to_dir = localdir + string(os.PathSeparator) + remotefile_datetime.Format("20060102")
		if fw._check_directory(move_to_dir, watch_data) == false {
			return ""
		}
    } else if watch_data["distribution"] == "dscope" {
		matches := fw._dscope_filename_dates_rec.FindStringSubmatch(filename)
		if len(matches) == 3 {
			month, _ := parseInt(matches[1])
			day, _ := parseInt(matches[2])
			year := 0
			today := watch_data["today"].(time.Time)
			if month == int(today.Month()) {
				year = today.Year()
			} else if (month > 11) && (int(today.Month()) < 2) {
				year = today.Year() - 1
			} else if (month < 2) && (int(today.Month()) > 11) {
				year = today.Year() + 1
			} else {
				year = today.Year()
			}
			loc, _ := time.LoadLocation("UTC")
			filename_datestamp := time.Date(year, time.Month(month), day, 0, 0, 0, 0, loc)
			move_to_dir = localdir + string(os.PathSeparator) + filename_datestamp.Format("20060102")
		}
    }
    return move_to_dir
}

func (fw *FTPWatcher) _make_symlink(source_file, dest_file, temp_file_name string, watch_data map[string]interface{}) {
    /*
     Creates a symlink on local filesystem
     source_file -> temp_file_name
     */
    watch_data["logger"].(*log.Logger).Printf("Creating symlink %s -> %s\n", source_file, dest_file)
    err := os.Symlink(dest_file, temp_file_name)
    if err != nil {
		watch_data["logger"].(*log.Logger).Printf("Can't create %s: %s\n",
			temp_file_name, err)
    }
}

func (fw *FTPWatcher) get_timestamp_of_link_file(lnkfile string, watch_data map[string]interface{}) time.Time {
	fi, err := os.Lstat(lnkfile)
	if err != nil {
		// No meta/link present, so either no lmirror or file has not been downloaded
		tstamp_str := genutil.ReadableFilenameTimestamp(lnkfile)
		if tstamp_str == "" {
			watch_data["logger"].(*log.Logger).Println("genutil.ReadableFilenameTimestamp returned blank for", lnkfile)
			return time.Time{}
		}
		tstamp, _ := time.Parse("Mon 20060102 15:04:05 MST", tstamp_str)
		return tstamp
	}
    symlink_tstamp := fi.ModTime()
	if dest_file_check, ok := watch_data["dest_file_check"].(bool); ok && dest_file_check {
		tstamp_cache, ok := watch_data["tstamp_cache"].(map[string]time.Time)
		if !ok { return symlink_tstamp }
		targ_tstamp, ok := tstamp_cache[lnkfile]
		if targ_tstamp.IsZero() || (!ok) {
			// This will find out the timestamp of the destination file
			targ_tstamp = getTimestamp(lnkfile)
			tstamp_cache[lnkfile] = targ_tstamp
		}
		return targ_tstamp
	}
    return symlink_tstamp
}

func getTimestamp(lnkfile string) time.Time {
	fi, err := os.Lstat(lnkfile)
    if err != nil {
        return time.Time{}
    }
	if (fi.Mode() & os.ModeSymlink) == 0 {
		// The file is not a symlink
		return time.Time{}
	}
	// the file is a symlink
	// obtain the target
	target, err := os.Readlink(lnkfile)
	if err != nil { return time.Time{} }
	if strings.HasSuffix(target, ".meta") == true {
		return getTimestampFromMetafile(target)
	}
	
	tstamp, _ := time.Parse("Mon 20060102 15:04:05 MST", genutil.ReadableFilenameTimestamp(target))  // on parse error the time is Zero time
	return tstamp
}

func getTimestampFromMetafile(metafile string) time.Time {
	fi, err := os.Stat(metafile)
	if err != nil { return time.Time{} }
	tstamp := fi.ModTime()
	cfg := qcfg.NewCfg(metafile + "." + time.Now().Format("20060102T150405.000"), metafile, false)
	blocks := cfg.GetBlocks()
	if len(blocks) == 0 { return time.Time{} }
	outdir := cfg.Str(blocks[0], "file", "outdir", "NODIR")
	files := strings.Split(cfg.Str(blocks[0], "file", "outfiles", ""), ",")
	if len(files) == 0 { // No component files, so check the target file
		basename := path.Base(metafile)
		fname := outdir + "/" + strings.TrimSuffix(basename, ".meta")
		if retstr := genutil.ReadableFilenameTimestamp(fname); retstr == "" {
			fmt.Println("getTimestampFromMetafile : Cannot find target file =", fname, "using genutil.ReadableFilenameTimestamp")
			return time.Time{}
		}
		return tstamp
	}
	for _, fl := range files {
		fl = outdir + "/" + fl
		if retstr := genutil.ReadableFilenameTimestamp(fl); retstr == "" {
            return time.Time{}
        }
	}
	return tstamp
}

func (fw *FTPWatcher) check_filename_timestamp(file_last_modified, timestamp time.Time, watch_data map[string]interface{}, localfname string) bool {
    /*
     Checks if a local filename's last modified timestamp
     matches the given timestamp. Timestamp argument
     is a datetime object
     */

    // If the local file is newer than ftp file, there is no need to download
    if (file_last_modified.After(timestamp) == true) || (file_last_modified.Equal(timestamp) == true) {
		return true
    }
	if dest_file_check, ok := watch_data["dest_file_check"].(bool); ok && dest_file_check {
		tstamp_cache, ok := watch_data["tstamp_cache"].(map[string]time.Time)
		if ok {
			tstamp_cache[localfname] = time.Time{}  // This causes to compute tstamp afresh next time because we are going to download now
		}
	}
    return false
}

func (fw *FTPWatcher) _open_file(filename string, watch_data map[string]interface{}) (fp *os.File, err error) {
    /*
     Opens filename and returns file handle, handles IO errors
     */

	watch_data["logger"].(*log.Logger).Printf("Opening %s to write data\n", filename)
    fp, err = os.OpenFile(filename, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, fw._default_permission)
    if err != nil {
		watch_data["logger"].(*log.Logger).Printf("Can't create %s: %s\n", 
			filename, err)
		return
    }
	watch_data["logger"].(*log.Logger).Printf("Opened %s to write data\n", filename)
    return
}

func (fw *FTPWatcher) ftp_get_file(filename string, fp *os.File, size uint64, watch_data map[string]interface{}) (bts int64, success bool) {
    // make a buffer to keep chunks that are read
    buf := make([]byte, 4096)
    success = false
    bts = 0
    if fw._reconnect_if_required(watch_data) == false {
		
		watch_data["logger"].(*log.Logger).Printf("Cannot reconnect. Giving up on the file", filename)
		return bts, false
    }
    sc := watch_data["ftp_server"].(*ftp.ServerConn)
	t0 := time.Now()
	sc.SetReadTimeoutFlag()
    for retry_attempts:=1 ; ((retry_attempts <= fw._max_retry_attempts) && (success == false)) ; retry_attempts++ {
		rfp, err := sc.Retr(filename)
		if err!=nil {
			watch_data["logger"].(*log.Logger).Printf(
				"Cannot RETR %s : %s\nRetry attempt %d\n", filename, err, retry_attempts)
			continue
		}
		bts = 0
		fp.Seek(0,0)
		// make a read buffer
		//r := bufio.NewReader(rfp)
		// make a write buffer
		//w := bufio.NewWriter(fp)
		for {
			// read a chunk
			n, err := rfp.Read(buf)
			if err != nil && err != io.EOF {
				watch_data["logger"].(*log.Logger).Printf(
					"Cannot Read %s : %s\nRetry attempt %d\n", filename, err, retry_attempts)
				break
			}
			if n != 0 && err == io.EOF {
				watch_data["logger"].(*log.Logger).Println("While reading file n =", n, "and err == EOF")
			}
			if n == 0 {
				if bts != int64(size) {
					watch_data["logger"].(*log.Logger).Println("Error : Downloaded size =", bts, " does not match filesize =", int64(size), "Retry attempt = ", retry_attempts)
					break
				}
				success = true
				break
			}
			// write a chunk
			if _, err := fp.Write(buf[:n]); err != nil {
				watch_data["logger"].(*log.Logger).Printf(
					"Cannot Write to %s : %s\nRetry attempt %d\n", fp.Name(), err, retry_attempts)
				break
			}
			bts = bts + int64(n)
			if time.Now().Sub(t0) >= 10*time.Minute {
				watch_data["logger"].(*log.Logger).Println("       Downloaded %d bytes.", bts)
				t0 = time.Now()
			}
			
		}

		//if err = w.Flush(); err != nil {
		//	watch_data["logger"].(*log.Logger).Printf(
		//		"Cannot Flush %s : %s\nRetry attempt %d\n", fp.Name(), err, retry_attempts)
		//	success = false
		//}
		watch_data["logger"].(*log.Logger).Println("Closing rfp")
		rfp.Close()
		watch_data["logger"].(*log.Logger).Println("Closed rfp, Closing fp")
		fp.Close()
		watch_data["logger"].(*log.Logger).Println("Closed fp")
		
    }
	sc.UnsetReadTimeoutFlag()
    if success == false {
		watch_data["logger"].(*log.Logger).Printf("Have re-tried file %s %d times, giving up.\n",
			filename, fw._max_retry_attempts)
    }

    return
}

func (fw *FTPWatcher) del_file(fullname string, watch_data map[string]interface{}) bool {
    err := os.RemoveAll(fullname)
    if err != nil {
		watch_data["logger"].(*log.Logger).Printf("Error deleting path %s : %s\n", fullname, err)
		return false
    }
    return true
}


func (fw *FTPWatcher) _rename_file(orig_filename, new_filename string, watch_data map[string]interface{}) bool {
    err := os.Rename(orig_filename, new_filename)
    if err != nil {
		watch_data["logger"].(*log.Logger).Printf("Error renaming %s as %s : %s\n", orig_filename, new_filename, err)
		return false
    }
    return true
}

func (fw *FTPWatcher) _chmod(filename string, watch_data map[string]interface{}) bool {
    err := os.Chmod(filename, fw._default_permission)
    if err != nil {
		watch_data["logger"].(*log.Logger).Printf("Error doing chmod %s : %s\n", filename, err)
		return false
    }
    return true

}

func (fw *FTPWatcher) _set_file_utime(filename string, file_datetime time.Time, watch_data map[string]interface{}) {
    /*
     Sets the given filenames' utime to one generated
     from the given datetime object
     */
    watch_data["logger"].(*log.Logger).Printf("Setting %s timestamp to %s\n", filename, file_datetime)
    os.Chtimes(filename, file_datetime, file_datetime)
}

func (fw *FTPWatcher) download_checker(filepath string, checker_cmd string, watch_data map[string]interface{}) bool {
    
    for _, command := range fw._split_run_cmd(checker_cmd) {
		success, stderr := fw._parse_run_cmd(filepath, command, watch_data)

		if success == false {
			watch_data["logger"].(*log.Logger).Printf(
				"Post download check exited with error! - %s. Deleting file %s\n",
				stderr, filepath)
			return false
		}
    }
    return true
}

func (fw *FTPWatcher) _check_remote_local_files(remote_files_list, subdir_list []string, local_dir string,
    watch_data map[string]interface{}, mode string) {
    /*
     Checks remote directory listing vs local directory listing
     and deletes local files that don't exist on remote side
     */
    if mode != "mirror" {
		return
    }
    file_list := make([]string, 0)
    fl, err := ioutil.ReadDir(local_dir)
    for _, f := range fl {
		file_list = append(file_list, f.Name())
    }
    if err != nil {
		file_list = make([]string, 0)
    }
    fw._delete_files_from_dir(local_dir, file_list, remote_files_list, subdir_list, watch_data)
    
}

func (fw *FTPWatcher) _delete_files_from_dir(local_dir string, files_list,
    remote_files_list,
    remote_subdir_list []string,
    watch_data map[string]interface{}) {
    /*
     Removes files or dirs from local_dir that don't exist in remote_files_list or remote_subdir_list
     */

    temp := make([]string, 0)
    for _, name := range files_list {
		in_remote_file_list := false
		in_remote_subdir_list := false
		for _, remfile := range remote_files_list {
			if name == remfile {
				in_remote_file_list = true
				break
			}
		}
		for _, remdir := range remote_subdir_list {
			if name == remdir {
				in_remote_subdir_list = true
				break
			}
		}
		if in_remote_file_list == false && in_remote_subdir_list == false {
			temp = append(temp, name)
		}
    }
    files_list = temp
    for _, name := range files_list {
		delete_file := true
		if name[0] == '.' {
			continue
		}
		for _, pattern := range fw.__skip_patterns {
			matched, _ := path.Match(pattern, name)
			if matched == true {
				watch_data["logger"].(*log.Logger).Printf("Skip pattern %s matches %s\n", pattern, name)
				delete_file = false
				break
			}
		}
		if delete_file == true {
			fullname := local_dir + string(os.PathSeparator) + name
			watch_data["logger"].(*log.Logger).Printf("Removing local file/dir %s\n", fullname)
			fw.del_file(fullname, watch_data)
		}
    }
}

func download_process(fw *FTPWatcher, watch_data map[string]interface{}, tid int, opts...interface{}) {
    /*
     Does post processing to the downloaded file
     This function is the target for each post process thread
     */
    if len(opts) != 4 {
		return
    }
    fullname := opts[0].(string)
    cmd := opts[1].(string)
    t1 := opts[2].(time.Time)
    t2 := opts[3].(time.Time)
    if watch_data["post_download"] != "" && cmd != ""  && watch_data["post_download"] != nil {
		for _, command := range fw._split_run_cmd(cmd) {
			success, stderr := fw._parse_run_cmd(fullname, command, watch_data)
			
			if success == false {
				watch_data["logger"].(*log.Logger).Printf(
					"Post download processing exited with error! - %s.\n",
					stderr)
				return
			} else {
				watch_data["logger"].(*log.Logger).Printf("Did post processing on filepath %s - output - %s\n",
					fullname, stderr)
			}
		}
    }
    if watch_data["use_lmirror_plugins"] != nil {
		// Call LMirror plugin functions described in cfg file
		plugins := watch_data["use_lmirror_plugins"].(map[string]bool)
		destfile := fullname
		lmirror_file := ""
		if plugins["transpath"] == true {
			lmirror_file, _ = fw.lmirror_plugins["transpath"](fw, watch_data, destfile, "")
		} else if plugins["adaptive-transpath"] == true {
			lmirror_file, _ = fw.lmirror_plugins["adaptive-transpath"](fw, watch_data, destfile, "", t1, t2)
		}
		if plugins["transzip"] == true {
			lmirror_file, _ = fw.lmirror_plugins["transzip"](fw, watch_data, destfile, lmirror_file)
		}
		if plugins["split"] == true {
			_, _ = fw.lmirror_plugins["split"](fw, watch_data, destfile, lmirror_file)
		}
    }
    return
}

func (fw *FTPWatcher) _isLoggedIn(watch_data map[string]interface{}) bool {
    fs := watch_data["ftp_server"]
    if fs == nil {
		return false
    }
    fserver := fs.(*ftp.ServerConn)
    _, err := fserver.CurrentDir()
    if err != nil {
		return false
    }
    return true
}

func (fw *FTPWatcher) _reconnect_if_required(watch_data map[string]interface{}) bool {

	watch_data["logger"].(*log.Logger).Println("Entered _reconnect_if_required()")
	defer watch_data["logger"].(*log.Logger).Println("Leaving  _reconnect_if_required()")
	if fw._isLoggedIn(watch_data) == false {
		if watch_data["ftp_server"] != nil {
			watch_data["ftp_server"].(*ftp.ServerConn).Quit()
		}
		watch_data["logger"].(*log.Logger).Println("Connection Lost. Reconnecting...")
		if fw._connect_login_ftp(watch_data) == false {
			watch_data["logger"].(*log.Logger).Println("Login Failed!")
			return false
		}
		watch_data["lastconnectat"] = time.Now()
		fserver := watch_data["ftp_server"].(*ftp.ServerConn)
		curdir := watch_data["curdir"].(string)
		watch_data["logger"].(*log.Logger).Println("Restoring the wd to", curdir)
		if fserver.ChangeDir(curdir) != nil {
			watch_data["logger"].(*log.Logger).Printf("CWD to %s Failed!\n", curdir)
			return false
		}
		return fw._isLoggedIn(watch_data)
		
	}

	if tmp, ok := watch_data["lastconnectat"]; ok == true {

		if time.Now().Sub(tmp.(time.Time)) > (time.Minute) {
			watch_data["logger"].(*log.Logger).Println("The connection is older than 1 minute, reconnecting...")
			watch_data["ftp_server"].(*ftp.ServerConn).Quit()
			if fw._connect_login_ftp(watch_data) == false {
				watch_data["logger"].(*log.Logger).Println("Login Failed!")
				return false
			}
			watch_data["logger"].(*log.Logger).Println("Reconnection successfull")
			fserver := watch_data["ftp_server"].(*ftp.ServerConn)
			curdir := watch_data["curdir"].(string)
			watch_data["logger"].(*log.Logger).Println("Restoring the wd to", curdir)
			if fserver.ChangeDir(curdir) != nil {
				watch_data["logger"].(*log.Logger).Printf("CWD to %s Failed!\n", curdir)
				return false
			}
			return fw._isLoggedIn(watch_data)
		}
		
	}
	return true

}

func (fw *FTPWatcher) mirrorsubdir(localdir string, watch_data map[string]interface{}, goroutine_start_time_local time.Time) {
    /*
     Recursively mirrors ftp location and stores in localdir
     start_directory is optional directory of ftp server to start in.
     newer_than is datetime object to compare remote file against. Files older
     than given datetime will not be downloaded
     */
    var bytes_downloaded int64 = 0
    var numfiles_downloaded int = 0
    var last_downloaded_filename string = ""
    var download_dir = ""
	if goroutine_start_time_local.Before(watch_data["goroutine_start_time"].(time.Time)) == true {
		// A new goroutine has been issued. so stop this one.
		watch_data["logger"].(*log.Logger).Println("mirrorsubdir : goroutine_start_time_local =", goroutine_start_time_local, "goroutine_start_time_current =", watch_data["goroutine_start_time"].(time.Time),
			"Exiting this goroutine")
		return
	}

    newer_than, _ := watch_data["newer_than"]
    //start_directory, exists2 := watch_data["start_dir"]
/*
    ftp_server, exists3 := watch_data["ftp_server"]
    if !(exists3 && ftp_server!=nil && fw._isLoggedIn(watch_data) == true) {
		if fw._connect_login_ftp(watch_data) == false {
			watch_data["logger"].(*log.Logger).Println("Login Failed!")
			return
		}
    }

*/
	if fw._reconnect_if_required(watch_data) == false {
		watch_data["logger"].(*log.Logger).Println("Could not reconnect. Giving up")
		return
	}
    fserver := watch_data["ftp_server"].(*ftp.ServerConn)
	
    curdir, err := fserver.CurrentDir()
    if err != nil {
		watch_data["logger"].(*log.Logger).Println("PWD failed!")
		return
    }

	if path.Clean(curdir) != path.Clean(watch_data["curdir"].(string)) {
		watch_data["logger"].(*log.Logger).Println("In wrong remote directory =", curdir, "Expected =", watch_data["curdir"].(string), "Exiting mirrorsubdir()")
		return
	}
	if defaultdir, okdef := watch_data["defaultdir"].(string); okdef && (defaultdir != ""){
		dest := watch_data["dest"].(string)
		lastpart := strings.Replace(localdir, dest, "", 1)
		if path.Clean(dest + "/" + defaultdir + "/" + lastpart) != path.Clean(dest + "/" + curdir) {
			watch_data["logger"].(*log.Logger).Println("Mismatch between adjusted local dir =", dest + "/(defaultdir=" + defaultdir + ")/" + lastpart, "and dest/curdir =", dest + "/" + curdir,
				"Exiting mirrorsubdir()")
			return
		}
	} else if path.Clean(localdir) != path.Clean(watch_data["dest"].(string) + "/" + curdir) {
		watch_data["logger"].(*log.Logger).Println("Mismatch between localdir =", localdir, "and dest/curdir =", watch_data["dest"].(string) + "/" + curdir, "Exiting mirrorsubdir()")
		return
	}

    //if exists2 && start_directory != "" {
    // if strings.Index(curdir, start_directory.(string)) ==  -1 {
    // 	if watch_data["ftp_server"] == nil {
    // 		return
    // 	}
    // 	if fserver.ChangeDir(start_directory.(string)) != nil {
    // 		watch_data["logger"].(*log.Logger).Println("CWD Failed!")
    // 		return
    // 	} else {
    // 		curdir, err = fserver.CurrentDir()
    // 		if err != nil {
    // 			watch_data["logger"].(*log.Logger).Println("PWD failed!")
    // 			return
    // 		}
    // 	}
    // }
    //}
    //watch_data["curdir"] = curdir
    if fw._check_directory(localdir, watch_data) == false {
		return
    }
    subdirs := make([]string, 0)
    listing := make([]*ftp.FTPListData, 0)
	time.Sleep(2*time.Second)
    watch_data["logger"].(*log.Logger).Printf("Listing remote directory %s...\n", curdir)
    if listing, err = fserver.List(curdir); err != nil {
		watch_data["logger"].(*log.Logger).Printf("Could not get remote directory listing for %s : err = %s\n", curdir, err.Error())
		return
    }
	watch_data["logger"].(*log.Logger).Printf("Listing completed for remote directory %s...\n", curdir)
    download_dir = curdir
    hour := watch_data["today"].(time.Time).Hour()
    minute := watch_data["today"].(time.Time).Minute()
    fw._write_directory_list(listing, fmt.Sprintf(watch_data["log_dir"].(string) + string(os.PathSeparator) + "directory_listing-%d:%d",
		hour, minute), watch_data)
    filesfound := make([]string, 0)
    dated_dirs := make([]string, 0)
    fullname := ""
    tempname := ""
    bytes_ := 0.0
    t1 := time.Now()
    t0 := time.Now()
    for _, list_out := range listing {
		// if fw._reconnect_if_required(watch_data) == false {
		// 	watch_data["logger"].(*log.Logger).Println("Could not reconnect. Giving up")
		// 	return
		// }

		if goroutine_start_time_local.Before(watch_data["goroutine_start_time"].(time.Time)) == true {
			// A new goroutine has been issued. so stop this one.
			watch_data["logger"].(*log.Logger).Println("mirrorsubdir : goroutine_start_time_local =", 
				goroutine_start_time_local, "goroutine_start_time_current =", watch_data["goroutine_start_time"].(time.Time),
				"Exiting this goroutine")
			return
		}


		if list_out.Name == "" {
			watch_data["logger"].(*log.Logger).Printf("Could not parse list output. Output:\n%s\n", list_out.RawLine )
			continue
		}
		if fw._skip_pattern(list_out.Name, watch_data) == true {
			continue
		}
		remotefile_datetime := list_out.Mtime
		remotefile_datetime = fw._adjust_filedate_tz(remotefile_datetime,
			watch_data["tz"].(string),
			watch_data["server_tz"].(string))
		watch_data["logger"].(*log.Logger).Println("Time stamp of remote file", list_out.Name, "is", remotefile_datetime)
		/*	opt := watch_data["_skip_dirRfile_time_staler_than_days"]
	if opt != nil {
	    ok_to_process_time := opt.(time.Time)
	    if remotefile_datetime.Before(ok_to_process_time) {
		watch_data["logger"].(*log.Logger).Printf("Remote filename %s is older than %s - not downloading 1\n",
		    list_out.Name,ok_to_process_time)
		continue
	    }
	}
*/
		opt := watch_data["_skip_file_time_staler_than_days"]
		if (opt != nil) && (list_out.TryRetr==true) {
			ok_to_process_time := opt.(time.Time)
			if remotefile_datetime.Before(ok_to_process_time) {
				watch_data["logger"].(*log.Logger).Printf("Remote filename %s is older than %s - not downloading 2\n",
					list_out.Name,ok_to_process_time)
				continue
			}
		}
		/*
	dir_name, err := parse_dated_dir(list_out.Name)
	dir_name = fw._adjust_filedate_tz(dir_name,
	    watch_data["tz"].(string),
	    watch_data["server_tz"].(string))
	if err == nil {
	    opt = watch_data["_skip_dir_name_staler_than_days"]
	    if (opt != nil) && (list_out.TryCwd == true) {
		ok_to_process_time := opt.(time.Time)
		if dir_name.Before(ok_to_process_time) {
		    watch_data["logger"].(*log.Logger).Printf("Remote filename %s is older than %s - not downloading 4\n",
			list_out.Name,ok_to_process_time)
		    continue
		}
	    }
	}
*/

		if list_out.TryCwd == true && list_out.TryRetr == false {
			// Indicates a directory
			watch_data["logger"].(*log.Logger).Printf("Remembering subdirectory %s\n", list_out.Name)
			subdirs = append(subdirs, list_out.Name)
			continue
		}
		filesfound = append(filesfound, list_out.Name)
		// move_to_dir := fw._check_distribution_method(list_out.Name,
		// 	remotefile_datetime,
		// 	localdir,
		// 	watch_data)
		move_to_dir := ""
		to := time.Time{}
		tn := remotefile_datetime
		if move_to_dir != "" {
			if fw._check_directory(move_to_dir, watch_data) == false {
				return
			}
			dated_dirs = append(dated_dirs, move_to_dir)
			watch_data["logger"].(*log.Logger).Printf("Made dated directory %s for file %s\n",
				move_to_dir, list_out.Name)
			fullname = move_to_dir + string(os.PathSeparator) + list_out.Name
			tempname = move_to_dir + string(os.PathSeparator)+ "@" + list_out.Name
			
		} else {

			fullname = localdir + string(os.PathSeparator) + list_out.Name
			tempname = localdir + string(os.PathSeparator) + "@" + list_out.Name
		}
		if list_out.TryRetr==true && list_out.TryCwd==true {
			fw._make_symlink(list_out.Name, list_out.LinkDest,
				tempname, watch_data)
			continue
		} else {
			to = fw.get_timestamp_of_link_file(fullname, watch_data)
			if fw.check_filename_timestamp(to, remotefile_datetime, watch_data, fullname) {
				watch_data["logger"].(*log.Logger).Printf("Remote and local timestamps match, not downloading %s\n",
					list_out.Name)
				continue
			}
			watch_data["logger"].(*log.Logger).Println("fw.get_timestamp_of_link_file returned", to, "for", fullname)
			if newer_than != nil {
				if remotefile_datetime.Before(newer_than.(time.Time)) {
					watch_data["logger"].(*log.Logger).Printf("Remote filename is older than %s - not downloading %s 3\n",
						newer_than, fullname)
					continue
				}
			}
			watch_data["logger"].(*log.Logger).Printf("Retrieving %s from %s as %s...\n",
				list_out.Name, curdir, fullname)
			fp, err := fw._open_file(tempname, watch_data)
			if err!= nil {
				continue
			}
			t0 = time.Now()
			bts, success := fw.ftp_get_file(list_out.Name, fp, list_out.Size, watch_data)
			last_downloaded_filename = fullname
			bytes_ = float64(bts)
			if  success == false {
				fp.Close()
				watch_data["logger"].(*log.Logger).Printf("Download for %s unsuccessful, deleting temporary file..\n",
					fullname)
				fw.del_file(tempname, watch_data)
				continue
			} else {
				fp.Close()
				if watch_data["download_check"] != nil {
					if fw.download_checker(tempname, watch_data["download_check"].(string), watch_data) == false {
						fw.del_file(tempname, watch_data)
						// Check failed, file has been deleted
						watch_data["logger"].(*log.Logger).Println("download check of", tempname, "failed, sending alert...")
						hostn, _ := os.Hostname()
						hostn = strings.SplitN(hostn, ".", 2)[0]
						kvpl := fmt.Sprintf("subtab=ftpwatcher;level=critical;subject=%s download check failed for %s/%s;escalate=ops;escalate-minutes1=5;escalate-minutes2=15", hostn, curdir, list_out.Name)
						doAlert(kvpl)
						continue
					}
					watch_data["logger"].(*log.Logger).Println("download check of", tempname, "sucessful")
				}
			}
			t1 = time.Now()
		}
		if fw._rename_file(tempname, fullname, watch_data) == false {
			continue
		}
		if fw._chmod(fullname, watch_data) == false {
			continue
		}
		fw._set_file_utime(fullname, remotefile_datetime, watch_data)
		dt := float64(t1.Sub(t0))/float64(time.Second)
		kbytes := float64(bytes_ / 1024.0)
		bytes_downloaded += int64(bytes_)
		numfiles_downloaded += 1
		watch_data["logger"].(*log.Logger).Printf("%s - %d KBytes in %d seconds - ~%d KB/s\n",
			list_out.Name, int(kbytes+0.5), int(dt+0.5), int((kbytes/dt)+0.5))
		fw.total_bytes[watch_data["blockname"].(string)] += int64(bytes_)
		oldwatchinfo := fw.jsondata[0][watch_data["blockname"].(string)].(WatchInfo)
		nfd := numfiles_downloaded + oldwatchinfo.Numfiles
		old_bytes, _ := strconv.ParseInt(oldwatchinfo.Bytes, 10, 64)
		total_bytes_downloaded := bytes_downloaded + old_bytes
		fw.jsondata[0][watch_data["blockname"].(string)] = WatchInfo{time.Now().Format("20060102 15:04:05"), nfd, strconv.FormatInt(total_bytes_downloaded, 10), 
			last_downloaded_filename, download_dir, oldwatchinfo.Logfile}
		fw.write_json_file()
		
		if (watch_data["post_download"] != nil) || (watch_data["use_lmirror_plugins"]) != nil {
			fw._add_work_to_chan(watch_data, watch_data["post_process_in_queue"].(chan work),
				download_process, fullname, watch_data["post_download"].(string), to, tn)
		}
		
    }
	/*    if len(dated_dirs) > 0 {
	//fw._check_remote_local_files(filesfound, subdirs, dated_dirs,
	//	watch_data, watch_data["mode"].(string))
    } else {
	fw._check_remote_local_files(filesfound, subdirs, localdir,
	    watch_data, watch_data["mode"].(string))
    }
  */  
    //Recursively mirror subdirectories
    for _, subdir := range subdirs {
		watch_data["logger"].(*log.Logger).Printf("Processing subdirectory %s\n", subdir)
		localsubdir := localdir + string(os.PathSeparator) + subdir
		if watch_data["ftp_server"] == nil {
			return
		}
		fserver := watch_data["ftp_server"].(*ftp.ServerConn)
		curdir, err := fserver.CurrentDir()
		if err != nil {
			watch_data["logger"].(*log.Logger).Println("PWD failed!")
			return
		}
		watch_data["logger"].(*log.Logger).Printf("Remote directory now: %s\n" , curdir)
		watch_data["logger"].(*log.Logger).Printf("Remote cwd %s\n", subdir)
		
		if fserver.ChangeDir(subdir) != nil {
			watch_data["logger"].(*log.Logger).Println("CWD Failed!")
			continue
		}
		watch_data["logger"].(*log.Logger).Printf("Mirroring subdir %s as %s\n", subdir, localsubdir )
		//watch_data["start_dir"] = ""
		watch_data["curdir"] = curdir + "/" + subdir
		fw.mirrorsubdir(localsubdir, watch_data, goroutine_start_time_local)
		if watch_data["ftp_server"] == nil {
			watch_data["logger"].(*log.Logger).Println("Bad connection. Quitting this iteration")
			return
		}
		fserver = watch_data["ftp_server"].(*ftp.ServerConn)
		watch_data["logger"].(*log.Logger).Println("Remote cwd ..")
		if fserver.ChangeDirToParent() != nil {
			watch_data["logger"].(*log.Logger).Println("CWD to parent Failed!")
			return
		}
		watch_data["curdir"] = curdir
		newcurdir, err := fserver.CurrentDir()
		if err != nil {
			watch_data["logger"].(*log.Logger).Println("PWD failed!")
			return
		}
		if newcurdir != curdir {
			watch_data["logger"].(*log.Logger).Println("Ended up in wrong directory after cd + cd ..")
			watch_data["logger"].(*log.Logger).Println("Giving up now.")
			break
		} else {
			watch_data["logger"].(*log.Logger).Printf("Finished with %s\n", subdir)
		}
		
    }
}

func doAlert(kvplist string) {
	if opt.Alertcmd == "" || opt.Alertcmd == "NOCMD" { return }
    command := []string{opt.Alertcmd, "--kvplist", kvplist}
    cmd := exec.Command(command[0], command[1:]...)
    cmd.CombinedOutput()
}

func start(fw *FTPWatcher, watch_data map[string]interface{}, tid int, opts...interface{}) {
    /*
     Starts up a single watcher
     */
	goroutine_start_time_local := time.Now()
    pt, exists := watch_data["poll_time"]
    poll_time := 0
    if exists==false {
		poll_time = fw.__loop_wait_time
    } else {
		poll_time = pt.(int)
    }
    watch_data["tid"] = tid
    watch_data["thread_object"] = fmt.Sprintf("Thread id %d", tid)
    watch_data["logger"].(*log.Logger).Println("Thread id", tid, "perpetually running as a daemon")
    for {
		fw.check_schedule(watch_data)
		fw.adjust_stale_time(watch_data)
		if goroutine_start_time_local.Before(watch_data["goroutine_start_time"].(time.Time)) == true {
			// A new goroutine has been issued. so stop this one.
			watch_data["logger"].(*log.Logger).Println("start : goroutine_start_time_local =", goroutine_start_time_local, "goroutine_start_time_current =", watch_data["goroutine_start_time"].(time.Time),
			"Exiting this goroutine")
			return
		}
		// TODO Error check the following. Re submit the job to watcher_in_q if needed
		sd, exists2 := watch_data["start_dir"]
		start_directories := sd.(string)
		if exists2 && start_directories != "" {
			for _, start_dir := range strings.Split(start_directories, ",") {
				watch_data["logger"].(*log.Logger).Println("Attempting to mirror the start directory : ", start_dir)
				watch_data["curdir"] = start_dir
				if fw._reconnect_if_required(watch_data) == false {
					watch_data["logger"].(*log.Logger).Println("Could not reconnect. Giving up")
					continue
				}
				watch_data["logger"].(*log.Logger).Println("Changing current working dir to " + start_dir)
				fserver := watch_data["ftp_server"].(*ftp.ServerConn)
				if fserver.ChangeDir(start_dir) != nil {
                    watch_data["logger"].(*log.Logger).Printf("CWD to %s Failed!\n", start_dir)
                    continue
                }
				fw.mirrorsubdir(watch_data["dest"].(string) + "/"  + start_dir, watch_data, goroutine_start_time_local)
			}
			watch_data["logger"].(*log.Logger).Println("Finished downloading all start directories." )
			
		} else {
			// In order to get the correct default start dir in every iteration, we must reconnect.
			if fw._connect_login_ftp(watch_data) == false {
				watch_data["logger"].(*log.Logger).Println("Login Failed!")
			} else {
				fserver := watch_data["ftp_server"].(*ftp.ServerConn)
				var errdef error = nil
				watch_data["curdir"], errdef = fserver.CurrentDir()
				if errdef != nil {
					watch_data["logger"].(*log.Logger).Println("Cannot get currdir, err =", errdef)
				} else {
					watch_data["defaultdir"] = watch_data["curdir"]
					fw.mirrorsubdir(watch_data["dest"].(string), watch_data, goroutine_start_time_local)
					if ftp_server, okftp := watch_data["ftp_server"].(*ftp.ServerConn); okftp {
						ftp_server.Quit()
						watch_data["ftp_server"] = nil
					}
				}
			}
		}

		if goroutine_start_time_local.Before(watch_data["goroutine_start_time"].(time.Time)) == true {
			// A new goroutine has been issued. so stop this one.
			watch_data["logger"].(*log.Logger).Println("start : goroutine_start_time_local =", goroutine_start_time_local, "goroutine_start_time_current =", watch_data["goroutine_start_time"].(time.Time),
			"Exiting this goroutine")
			return
		}

		ftp_server, exists := watch_data["ftp_server"]
		//fmt.Println(ftp_server==nil, exists)
		if (poll_time >= fw.__loop_wait_time) && exists && (ftp_server!=nil) {
			ftp_server.(*ftp.ServerConn).Quit()
			watch_data["ftp_server"] = nil
		}

		watch_data["logger"].(*log.Logger).Println("About to sleep for", poll_time, "seconds")
		time.Sleep(time.Duration(poll_time)*time.Second)
		watch_data["logger"].(*log.Logger).Println("Woke up from sleep")

		if goroutine_start_time_local.Before(watch_data["goroutine_start_time"].(time.Time)) == true {
			// A new goroutine has been issued. so stop this one.
			watch_data["logger"].(*log.Logger).Println("start : goroutine_start_time_local =", goroutine_start_time_local, "goroutine_start_time_current =", watch_data["goroutine_start_time"].(time.Time),
			"Exiting this goroutine")
			return
		}

		
    }
    
}

func (fw *FTPWatcher) start_watcher(watcher_in_queue chan work, watch_data map[string]interface{}, tids []int) {
    /*
     Starts up a single watcher thread
     */
    watch_data["tid"] = 0
    watch_data["thread_object"] = ""

    ftp_server, exists := watch_data["ftp_server"]
    if exists && (ftp_server!=nil) {
		ftp_server.(*ftp.ServerConn).Quit()
    }
    watch_data["ftp_server"] = nil
    fw._start_threads(watcher_in_queue, 1)
    fw._add_work_to_chan(watch_data, watcher_in_queue, start)
    for watch_data["tid"] == 0 {
		time.Sleep(time.Second)
    }
    tids = append(tids, watch_data["tid"].(int))
    watch_data["logger"].(*log.Logger).Printf("Found thread id %d\n", watch_data["tid"].(int) )
}

func (fw *FTPWatcher) StartAllWatchers() {
    /*
     Starts up threads for all watchers
     */
	logfiles    := make([]string, len(fw.watchers))

	for idx, watch_data := range fw.watchers {
		logfiles[idx] = fmt.Sprintf("%s/%s", watch_data["log_dir"].(string), fw.__log_filename)
	}
	
    for {
		for idx, watch_data := range fw.watchers {
			thread_object, exists := watch_data["thread_object"]
			if ! ( exists && thread_object!="" ) {
				watcher_in_queue := make(chan work, 10)
				watch_data["goroutine_start_time"] = time.Now() // Used by the goroutine to check if it is declared dead
				fw.start_watcher(watcher_in_queue, watch_data, fw.tids)
			} else {
				// TODO
				// Check if thread is alive
				// if dead, restart the thread and log this info and send out a mail
				start_time, end_time, now := fw.get_schedule(watch_data)

				if start_time.IsZero() == true && end_time.IsZero() == true {
					continue
				}

				if now.After(end_time) == true || now.Before(start_time) == true {
					// Not in schedule. so don't check the logs
					continue
				}

				fi, err := os.Stat(logfiles[idx])
				if err != nil {
					watch_data["logger"].(*log.Logger).Println("Cannot access", logfiles[idx])
					continue
				}
				if time.Now().Sub(fi.ModTime()) > watch_data["log_file_stale_duration"].(time.Duration) {
					watch_data["logger"].(*log.Logger).Println("StartAllWatchers : No updates in logfile for", watch_data["log_file_stale_duration"].(time.Duration).Minutes(),
						"minutes. Last update was at", fi.ModTime(), ". Launching a new goroutine.")
					watcher_in_queue := make(chan work, 10)
					watch_data["goroutine_start_time"] = time.Now() // Used by the goroutine to check if it is declared dead
					fw.start_watcher(watcher_in_queue, watch_data, fw.tids)					
				}
			}
		}
		time.Sleep(time.Minute)
    }
}

func StartWithConfigFile( configFile string, start_daemon bool) {
    var ccfg *qcfg.CfgBlock = qcfg.NewCfg("main", configFile, false)
    blockNames := ccfg.GetBlocks()
    watchlist := make([]map[string]interface{}, 0)
    for _, block_name := range blockNames {
		hostname := ccfg.Str(block_name, _CONFIG_PARAM_ROW, "hostname", "")
		ftp_user := ccfg.Str(block_name, _CONFIG_PARAM_ROW, "user", "")
		passwd := GenPasswd(ccfg.Str(block_name, _CONFIG_PARAM_ROW, "passwd", ""))
		destination_path := ccfg.Str(block_name, _CONFIG_PARAM_ROW, "destination", "")
		mode := ccfg.Str(block_name, _CONFIG_PARAM_ROW, "mode", "")
		debug := ccfg.Str(block_name, _CONFIG_PARAM_ROW, "debug", "")
		tz := ccfg.Str(block_name, _CONFIG_PARAM_ROW, "tz", "")
		server_tz := ccfg.Str(block_name, _CONFIG_PARAM_ROW, "server_tz", "")
		dest_file_check := (ccfg.Str(block_name, _CONFIG_PARAM_ROW, "dest_file_check", "false") == "true")
	    skip_dirRfile_time_staler_than_days := int(ccfg.Int64(block_name, _CONFIG_PARAM_ROW,
			"skip_dirRfile_time_staler_than_days", 30))
	    skip_file_time_staler_than_days := int(ccfg.Int64(block_name, _CONFIG_PARAM_ROW,
			"skip_file_time_staler_than_days", 30))
	    skip_dir_name_staler_than_days := int(ccfg.Int64(block_name, _CONFIG_PARAM_ROW,
			"skip_dir_name_staler_than_days", 30))
	    thread_no := int(ccfg.Int64(block_name, _CONFIG_PARAM_ROW,
			"thread_no", 0))
	    poll_time := int(ccfg.Int64(block_name, _CONFIG_PARAM_ROW,
			"poll_time", 300))
		list_internal_read_timeout, _ := time.ParseDuration(ccfg.Str(block_name, _CONFIG_PARAM_ROW, "list_internal_read_timeout", "60s"))
		log_file_stale_duration, _    := time.ParseDuration(ccfg.Str(block_name, _CONFIG_PARAM_ROW, "log_file_stale_duration", "25m"))
	    start_directory := ccfg.Str(block_name, _CONFIG_PARAM_ROW,
			"start_dir", "")
	    distribution := ccfg.Str(block_name, _CONFIG_DISTRIBUTION_ROW,
			"method", "")
	    post_download_check := ccfg.Str(block_name, _CONFIG_DOWNLOAD_CHECK_ROW,
			"app", "" )
	    post_download_process := ccfg.Str(block_name, _CONFIG_POST_DOWNLOAD_ROW,
			"app", "" )
	    use_proxy := int(ccfg.Int64(block_name, _CONFIG_PROXY_ROW,
			"useProxy", 0 ))
	    proxy_host := ccfg.Str(block_name, _CONFIG_PROXY_ROW,
			"hostname", "" )

	    skip_patterns := ccfg.Str(block_name, _CONFIG_PARAM_ROW,
			"skip_patterns", "" )
		cooked_skip_patterns := strings.Split(skip_patterns, _SKIP_PATTERNS_SEP)
		for i:=0 ; i<len(cooked_skip_patterns) ; i++  {
			cooked_skip_patterns[i] = strings.Replace(cooked_skip_patterns[i],
				_SKIP_PATTERNS_LITERAL_SEP, _SKIP_PATTERNS_SEP, -1)
			
		}
	    start_time := ccfg.Str(block_name, _CONFIG_SCHEDULER_ROW,
			"start_time", "")
	    end_time := ccfg.Str(block_name, _CONFIG_SCHEDULER_ROW,
			"end_time", "")
	    warn_time := ccfg.Str(block_name, _CONFIG_SCHEDULER_ROW,
			"warn_time", "")
	    warn_cmd := ccfg.Str(block_name, _CONFIG_SCHEDULER_ROW,
			"warn_cmd", "")
	    alert_recp := ccfg.Str(block_name, _CONFIG_ALERT_ROW,
			"recipient", "")
	    alert_body := ccfg.Str(block_name, _CONFIG_ALERT_ROW,
			"body", "")
	    alert_subj := ccfg.Str(block_name, _CONFIG_ALERT_ROW,
			"subject", "")

	    lmirror_plugins := ccfg.Str(block_name, _CONFIG_LMIRROR_ROW,
			"plugins", "")
		lmirror_plugins_to_use := strings.Split(lmirror_plugins, ",")
	    lmirror_path_fmt := ccfg.Str(block_name, _CONFIG_LMIRROR_ROW,
			"lmirror_path_format", "")
	    lmirror_zip_fmt := ccfg.Str(block_name, _CONFIG_LMIRROR_ROW,
			"zipfmt", "xz")
	    lmirror_split_cmd := ccfg.Str(block_name, _CONFIG_LMIRROR_ROW,
			"split_cmd", "")
		
		
		watchData := make(map[string]interface{})
		watchData["blockname"] = block_name
		watchData["use_lmirror_plugins"] = lmirror_plugins_to_use
		watchData["lmirror_path_fmt"] = lmirror_path_fmt
		watchData["lmirror_zip_fmt"] = lmirror_zip_fmt
		watchData["lmirror_split_cmd"] = lmirror_split_cmd
		watchData["hostname"] = hostname
		watchData["user"] = ftp_user
		watchData["passwd"] = passwd
		watchData["dest"] = destination_path
		watchData["start_dir"] = start_directory
		watchData["mode"] = mode
		watchData["debug"] = debug
		watchData["tz"] = tz
		watchData["server_tz"] = server_tz
		watchData["dest_file_check"] = dest_file_check
		watchData["tstamp_cache"] = map[string]time.Time{}
		watchData["skip_dirRfile_time_staler_than_days"] = skip_dirRfile_time_staler_than_days
		watchData["skip_file_time_staler_than_days"] = skip_file_time_staler_than_days
		watchData["skip_dir_name_staler_than_days"] = skip_dir_name_staler_than_days
		watchData["poll_time"] = poll_time
		watchData["list_internal_read_timeout"] = list_internal_read_timeout
		watchData["log_file_stale_duration"] = log_file_stale_duration
		watchData["distribution"] = distribution
		watchData["download_check"] = post_download_check
		watchData["post_download"] = post_download_process
		watchData["use_proxy"] = use_proxy
		watchData["proxy_host"] = proxy_host
		watchData["block_name"] = block_name
		watchData["skip_patterns"] = cooked_skip_patterns
		watchData["start_time"] = start_time
		watchData["end_time"] = end_time
		watchData["warn_time"] = warn_time
		watchData["warn_cmd"] = warn_cmd
		watchData["warn_alert_recp"] = alert_recp
		watchData["warn_alert_body"] = alert_body
		watchData["warn_alert_subj"] = alert_subj
		watchData["thread_no"] = thread_no
		//fmt.Println("skip_file_time_staler_than_days = ", watchData["skip_file_time_staler_than_days"])
		watchlist = append(watchlist, watchData)
    }

    watcher := newFTPWatcher(watchlist, start_daemon)
    watcher.StartAllWatchers()
    
}

func main() {
    parseArgs()
    hostname,_ := os.Hostname()	
	conn, err := cim.NewCimConnection(hostname, "ftpwatcher" + opt.Inst, "ftpwatcher-connection-test-" + opt.Inst)
	if err == nil {
		conn.Close()
		panic("Error starting ftpwatcher instance " + opt.Inst + ". Found the instance running already.")
	}
    StartWithConfigFile(opt.Config, true)
}

var passwdMap = map[string]string{
	"__HASH__"       : "#",
	"__EQUALS__"     : "=",
	"__SEMICOLON__"  : ";",
	"__PLUS__"       : "+",
	"__PERCENT__"    : "%",
	"__LBRACKET__"   : "{",
	"__RBRACKET__"   : "}",
	"__DOUBLECOLON__": "::",
}

func GenPasswd(in string) string {
	return genutil.StrReplaceWithMap(in, passwdMap)
}
