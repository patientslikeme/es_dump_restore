# Version 2.1.0 - September 30, 2015

* Dump index metadata along with data
* Bugfix: correctly retrieve \_settings when using the dump_type action

# Version 2.0.0 - July 1, 2015

Thanks to Alex Tomlins for this feature contribution

* Make progress bar options globally available
* VERY MINOR BREAKING CHANGE: Invert the `--noprogressbar` option, which is now `--progressbar` or `--no-progress-bar`

# Version 1.2.0 - June 24, 2015

* Allow overriding the batch size on the command line (thanks to Richard Boulton once again!)

# Version 1.1.0 - May 12, 2015

* Allow specifying additional index settings to the `restore` and `restore_alias` commands (thanks again Richard Boulton!)

# Version 1.0.0 - April 11, 2015

* Add a `restore_alias` command, which performs a restore and switches an alias to point at the result (thanks Richard Boulton!)
* Fixes a bug preventing index restoration on newer ES releases (thanks again Richard Boulton!)
* Richard Boulton tells us he's used `es_dump_restore` to perform a migration on the www.gov.uk indexes, so we think it's high time we started calling this tool ready for prime time.  Announcing 1.0.0!

# Version 0.0.13 - April 10, 2015

* Fix a regression introduced for ES version 0.90.x by the previous release (thanks again Richard Boulton!)

# Version 0.0.12 - April 10, 2015 - YANKED

* Handle null values for settings and mappings; useful in the case of trying to dump an alias (thanks Richard Boulton!)
* Handle the case of ES losing the scroll context mid-dump (thanks again Richard Boulton!)

# Version 0.0.11 - March 4, 2015

* Make it possible to load on older versions of Ruby (thanks Alex Tomlins!)

# Version 0.0.10 - December 3, 2014

* Fixes a bug introduced in 0.0.9 (thanks again to Noa Resare for the fix!)

# Version 0.0.9 - December 2, 2014 - YANKED

* Makes it possible to use ES instances not located at server root (thanks Noa Resare for the patch!)

# Version 0.0.8 - September 9, 2014

* Added --noprogressbar option for easier usage with cron (thanks Marco Dickert for the patch!)
