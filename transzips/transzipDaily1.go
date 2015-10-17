package main

import (
	"github.com/LDCS/sflag"
	"fmt"
	"io/ioutil"
	"github.com/LDCS/genutil"
	"regexp"
	"strings"
	"os"
	"github.com/LDCS/qcfg"
)

var opt = struct {
    Usage		string  "zip to gz for vendor1 files"
    Inbase		string	"where input zip files are  		| /dev/null"
    Outbase		string	"where output gz files should go	| /dev/null"
    Tempbase     	string	"where the tempfiles should be created	| /dev/null"
    Infile		string	"current input file			| /dev/null"
    Metafile            string  "metafile to save the filenames         | /dev/null"
}{}

func main() {
    sflag.Parse(&opt)
    genutil.CheckFileIsReadableAndNonzeroOrDie(opt.Inbase + "/" + opt.Infile)
    fi, _ := os.Lstat(opt.Inbase + "/" + opt.Infile)
    modtime := fi.ModTime()
    outfiles := make([]string, 0)
    fmt.Println("doing file=", opt.Infile)
    buf	:= ""
    opt.Tempbase = strings.TrimRight(opt.Tempbase, "/ ")
    opt.Inbase = strings.TrimRight(opt.Inbase, "/ ")
    opt.Outbase = strings.TrimRight(opt.Outbase, "/ ")
    dir	:= opt.Tempbase
    numrex, err	:= regexp.Compile("[12][0-9][0-9][0-9][01][0-9][0-3][0-9]")	// regexp.MustCompile("\d\d\d\d\d\d\d\d")
    if err != nil {
		panic("could not compile date regexp")
    }
    infile	:= opt.Inbase + "/" + opt.Infile
    inlen	:= len(infile)
    if genutil.IsZipFilename(infile) {
		yyyymmdd2	:= genutil.GetYyyymmddFromFilenameYymmddFromEndWithSuffixLen(infile, len(".zip"), "19010101") // grab the YYMMDD from filenames of form foo_YYMMDD.zip
		if yyyymmdd2 == "19010101" { 
			fmt.Println("skipping unknown zip filename=", infile)
			return
		}
		//tmpdir	:= genutil.MakeDirOrDie(opt.Tempbase, "TEMP")
		tmpdir, tmperr := ioutil.TempDir(opt.Tempbase, "vendor1")
        if tmperr != nil { panic("Cannot create temp dir by ioutil.TempDir, err=" + tmperr.Error()) }

		buf = genutil.BashExecOrDie(false, "/usr/bin/unzip -u -d " + tmpdir + " " + infile, dir);
		for _,ff := range genutil.FileList(tmpdir) {
			fflen		:= len(ff)
			yyyymmdd	:= ff[fflen- 8:]
			if ((fflen  < 9) || !numrex.MatchString(ff[fflen-8:])) {
				fmt.Println("substituting non-yyyymmdd fname=", ff, " infile=", infile)
				yyyymmdd	= yyyymmdd2
			}
			ffile		:= tmpdir + "/" + ff
			genutil.EnsureDirOrDie(opt.Outbase, yyyymmdd)
			buf = genutil.BashExecOrDie(false, "/usr/bin/gzip " + ffile, dir);					// fmt.Println(buf)
			buf = genutil.BashExecOrDie(false, "/bin/mv -f " + ffile + ".gz " + opt.Outbase + "/" + yyyymmdd, dir);	// fmt.Println(buf)
			os.Chtimes(opt.Outbase + "/" + yyyymmdd + "/" + ff + ".gz", modtime, modtime)
			outfiles = append(outfiles, yyyymmdd + "/" + ff + ".gz")
		}
		buf = genutil.BashExecOrDie(false, "/bin/rmdir " + tmpdir, dir);						// fmt.Println(buf)
		if false { fmt.Println(buf) }
    } else if ((inlen  > 9) && numrex.MatchString(infile[inlen-8:])) {	// some files are not zipfiles, but end in a date
		yyyymmdd	:= infile[inlen-8:]
		fmt.Println("nonzip file=", infile, " yyyymmdd=", yyyymmdd)
		outfile		:= opt.Outbase + "/" + yyyymmdd + "/" + opt.Infile
		outfiles = append(outfiles, yyyymmdd + "/" + opt.Infile + ".gz")
		if true {
			genutil.EnsureDirOrDie(opt.Outbase, yyyymmdd)
			buf		= genutil.BashExecOrDie(false, "/bin/cp -f " + infile + " " + outfile, dir);				// fmt.Println(buf)
			buf		= genutil.BashExecOrDie(false, "/usr/bin/gzip -f " + outfile, dir);					// fmt.Println(buf)
		} else {
			buf		= genutil.BashExecOrDie(false, "/usr/bin/gzip -f " + infile, dir);					// fmt.Println(buf)
			genutil.EnsureDirOrDie(opt.Outbase, yyyymmdd)
			buf		= genutil.BashExecOrDie(false, "/bin/mv -f " + infile + ".gz " + opt.Outbase + "/" + yyyymmdd, dir);	// fmt.Println(buf)
		}
		os.Chtimes(outfile + ".gz", modtime, modtime)
    } else {
		fmt.Println("nonzip file=", infile, "with no yyyymmdd pattern, so doing nothing!")
    }
    ccfg := qcfg.NewCfgMem(opt.Metafile)
    ccfg.EditEntry("transzipV1Daily", "file", "outdir", opt.Outbase)
    ccfg.EditEntry("transzipV1aily", "file", "outfiles", strings.Join(outfiles, ","))
    ccfg.CfgWrite(opt.Metafile)
	
}

