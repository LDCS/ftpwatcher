# ftpwatcher

Ftpwatcher is an FTP client utility to mirror (either exactly or in logical sense) multiple ftp sites.
Each mirror can be configured differently for polling schedule, download checks and post-processing.

Files are said to be logically mirrored if they are not stored exactly at the same relative path and form as on the upstream ftp site.
For example, they may be 
  - compressed upon download to save space
  - renamed or moved into dated subdirectories to conform to local requirement
  - etc

Ftpwatcher, compiled with Go versions upto 1.3.3, has been working reliably for 2+ years in environment with approx 10 ftp sites with tens of GBs incoming daily.
An unexplained issue that was seen in earlier days, where the program hangs on one of the sites, has not been seen since the compilation with Go 1.3.3.

Ftpwatcher is part of set of programs, mainly written in golang, implementing file-driven backoffice workflow of a company in the financial services area.


The code uses the terms
   - "plugin" to refer to functions that add features
   - "transzip" to refer to change of compression or container type

How to build :

    ```
    go get -u github.com/LDCS/ftpwatcher
    ```

How to run :

    ```
    /path/to/ftpwatcher --Config="/path/to/config/file" --Inst="two digit instance number for example : 01" --Logbasedir="/home/ftpwatcher/logs/"
    ```

Example config files for common situations :

```
# Example 1 : Config file for creating a plain conservative mirror of an ftp site "ftp.example.com"
#  mirroring should take place between 01:00 and 23:00 of every day without any proxy. 

%block example1
{
     ftp-watcher         :: hostname=ftp.example.com; user=username; passwd=strongpassword;
                         += destination=/path/to/where/files/to/be/mirrored;
                         += tz=US/Eastern; # Local timezone
	                 += server_tz=US/Eastern; # FTP server's timezone
                         += skip_patterns=bin,etc; # skips these dir in the server
     scheduler           :: start_time=010000; end_time=230000;
}


# Example 2 : Same as in Example 1 but connects to the ftp site through a proxy 10.23.12.45

%block example2
{
     ftp-watcher         :: hostname=ftp.example.com; user=username; passwd=strongpassword;
                         += destination=/path/to/where/files/to/be/mirrored;
                         += tz=US/Eastern; server_tz=US/Eastern;
                         += skip_patterns=bin,etc; # skips these dir in the server
     scheduler           :: start_time=010000; end_time=230000;
     proxy               :: useProxy=1;hostname=10.23.12.45;
}

# Example 3 : Same as in Example 2, additionally does a post download check for zip files
            testzip.bash script is present at https://github.com/LDCS/ftpwatcher

%block example3
{
     ftp-watcher         :: hostname=ftp.example.com; user=username; passwd=strongpassword;
                         += destination=/path/to/where/files/to/be/mirrored;
                         += tz=US/Eastern; server_tz=US/Eastern;
                         += skip_patterns=bin,etc; # skips these dir in the server
     scheduler           :: start_time=010000; end_time=230000;
     proxy               :: useProxy=1;hostname=10.23.12.45;
     download-check      :: app=/path/to/download/check/scripts/testzip.bash;
}

# Example 4: Same as in Example 3, in addition, here files are moved from meta directory (provided by destination= ) to logical
#           mirror directory provided by lmirror_path_format. lmirror_path_format may contain the following template keywords :
# 1. __CURRDATE__ : This will be replaced by the current local date.
# 2. __CURDIR__   : This will be replaced by the absolute path in the remote ftp site for each file.
# 3. __NOMINAL_DATE__ : This will be replaced by the date in the filename in the format YYYYMMDD if present
#                      else will be replaced by the string "undated".
#
# This example also uses a split plugin allows ftpwatcher to call an external "split" program 
# (See splitplugin/README ) to do any arbitrary transformations to the file.

%block example4
{
     ftp-watcher         :: hostname=ftp.example.com; user=username; passwd=strongpassword;
                         # "destination" is now used for storing meta files like symlinks to 
                         # actual files stored in logical mirror dir provided by lmirror_path_format
                         += destination=/path/to/where/meta/files/are/stored; 
                         += tz=US/Eastern; server_tz=US/Eastern;
                         += skip_patterns=bin,etc; # skips these dir in the server
     scheduler           :: start_time=010000; end_time=230000;
     proxy               :: useProxy=1;hostname=10.23.12.45;
     download-check      :: app=/path/to/download/check/scripts/testzip.bash;
     lmirror             :: plugins=transpath,split; lmirror_path_format=/path/to/logical/mirror/__CURDIR__/; 
                         += split_cmd=/path/to/split/plugins/splitpluginExecutable;
}

# Example 5 : As Example 2, in addition changes compression format of all files to gzip using the builtin transzip plugin.

%block example5
{
     ftp-watcher         :: hostname=ftp.example.com; user=username; passwd=strongpassword;
                         += destination=/path/to/where/files/to/be/mirrored;
                         += tz=US/Eastern; server_tz=US/Eastern;
                         += skip_patterns=bin,etc; # skips these dir in the server
     scheduler           :: start_time=010000; end_time=230000;
     proxy               :: useProxy=1;hostname=10.23.12.45;
     download-check      :: app=/path/to/download/check/scripts/testzip.bash;
     lmirror             :: plugins=transzip; zipfmt=gzip;
}

```