# Command Line Interface
There's a few command line tools shipped with pyiron to help administrating and keeping up with your pyiron project as 
well as some that are used internally. All of them are installed by default in the `pyiron` script that has a few sub 
commands.

* `pyiron install` Installs the pyiron resources for the first time, if you don't get them via conda.
* `pyiron ls` List the jobs inside a project and filter them with a few primitives
   Print the run time of all finished jobs
   ```
   pyiron ls -c job totalcputime -s finished
   ```
   Print all jobs with iron
   ```
   pyiron ls -e Fe
   ```
   Print all jobs that successfully finished yesterday and a bit
   ```
   pyiron ls -s finished -i 1d5h
   ```
   Print all jobs that were aborted less than 5 hours ago and match `spx.*restart`:
   ``` 
   pyiron ls -n "spx.*restart" -i 5h -s aborted
   ```
* `pyiron rm` Delete jobs and whole projects from the database and the file system.  If you simply `rm` jobs and projects 
  they are still in the database and can lead to confusion on pyiron's part.
* `pyiron wrapper` Runs jobs from the database. pyiron uses this internally to start jobs on the remote cluster nodes, 
  but you can also use it when you set the run mode to `manual` or to manually re-run jobs.

