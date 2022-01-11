# Unreleased

# pyiron_base-0.5.0
- Create CHANGELOG.md (#623)
- Add update functionality to maintenance (#607)
- Silence expected warnings in tests. (#622)
- Drop python 3.7 support (#621)
- job name conversion in pr.load same as pr.create (#617)
- Refactor maintenance into own module (#606)
- Dependency updates: #611, #615, #610, #608
- GitHub infrastructure: #614, #604 

# pyiron_base-0.4.5
- Worker: Use Pool rather than Threadpool (#602)
- Remove jobs silently (#597)
- Remove folders in working directory during compress() (#601)
- Use Black formating (#598)
- Use notebook version of tqdm if possible  (#593)
- Remove get_pandas (#594)
- Job factory call (#495)

# pyiron_base-0.4.4
- Fix: DeprecationWarning: pyiron_base.project.generic.view_mode is deprecated: use db.view_mode. (#586)
- Add interactive with statement (#565, #595, #596)
- Implement worker class without database (#581)
- Filetable: job status return None if the file does not exist (#579)
- Extension of the Worker Class (#575)
- Support non_modal mode without database (#580)
- Add copy, split and merge methods to FlattenedStorage (#571)
- Clean up config when database is disabled (#578)
- Fix executable when it is defined in the Notebook (#574)
- dependency updates: #582 #583 #573 
- GiHub infrastructure: #584

# pyiron_base-0.4.3
- Create job name including rounding and special character replacement (#568)
- Migration to Python 3.10 (#569)
- Allow custom return codes (#537) 
- Projectpath tests (#567)
- Worker: Set job to aborted after child runtime expires (#563)
- Replace automatically special symbols in job names (#561)
- Allow default arguments to JobCore.get (#562)
- Store pandas dataframe as single object in data frame (#560)
- Allow use of __getitem__/__setitem__ with FlattenedStorage (#528)
- Add a method to draw a sample from a FlattenedStorage (#557)
- Do not show job remove progress bar during unit tests (#555)
- Raise proper exceptions (#556)
- Progressbar for removing jobs (#554)
- dependencies: #531
- GitHub infrastructure #558

# pyiron_base-0.4.2
- Fix writing ragged arrays & small update to update script (#533)
- Disable project_check_enable and retain root_path/project_path (#541)
- convert `missing_ok` to try/except (#550)
- Fix publication (#547)
- Do we need pytables? (#527)
- Fix formatting in ImportAlarm docs (#544)
- Use Subprocess rather than Wrapper Class to prevent memory issues (#536)
- Refresh job status while waiting on project  (#524)
- Fix bug when writing empty FlattenedStorage (#534)
- GitHub infrastructure: #548, #535

# pyiron_base-0.4.1
Fix read-conversion of dtype=object arrays (#530)

# pyiron_base-0.4.0
pyiron_base<=0.3.10 has a bug that writes all arrays with dtype=object even numeric ones. In this new release the writing of the arrays is fixed. 
Since this affects the content of the hdf file this is a minor version bump. 
Although it was intended, such dtype=object arrays are not yet always automatically converted into numeric ones in the reading procedure in this release. This will be fixed asap.

Merged PRs:
- Convert dtype=object arrays if possible (#518) 
- Do not copy files on restart  (#511)
- Refactor global state (#486)
- Faster hdf reading  (#512)
- Correctly check for ragged array  (#503) - Fixing the major bug
- Test for type conservation (#508)
- Worker Job Class (#497)
- Bugfix in case of None type values  (#488)
- Avoid compressing only if zipped file name has the job name in it (#507)
- erase lines in hdfio.py (#233)
- Clean up subprocess and multiprocessing calls (#498)
- Do not lose track of dtypes in get_array_ragged (#494) 
- Save pyiron tables via DataContainer instead of csv (#463)
- dependency updates: #500, #515 

# pyiron_base-0.3.10
- Settings promises (#484)
- dependency updates (#493)

# pyiron_base-0.3.9
Updates includes:
- addition of Unittest with filled template project: #476
- addition of more filtering options in `pr.iter_jobs`: #492 #477
- addition of new methods to flattened storage: #481
- bug fixes: #483, #487
- dependency updates: #490, #491 

# pyiron_base-0.3.8
- Add worker mode (#474)
- Fix Database Timeout config (#470)
- Make `FileHDFio` a `MutableMapping` (#469)
- Remove `JobCore.load_object` and clean up around it (#462)

# pyiron_base-0.3.7
- All include header in job_table even if empty (#467)
- search function for DataContainer (#460)
- Add method to project to refresh all jobs' status (#464)
- Add `delete_aborted_job` flag  (#465)
- Update resources in pyiron install (#461)

# pyiron_base-0.3.6
- Allow retrieving full flat arrays from `get_array` (#454)

# pyiron_base-0.3.5
- More helpful error on invalid job name  (#455)
- Extract all database responsibilities from Settings (#453)
- Update `HasHDF` docs when dual-inheriting (#448)
- Cleanup  (#452)
- Provide version and git repo in maintenance (#451)
- Refactor settings (#449)

# pyiron_base-0.3.4
- Use HasHDF in FlattenedStorage (#446)
- Fix template job saving and loading (#444)

# pyiron_base-0.3.3
- Smooth storage  (#434)
- default instead of abstract (#442)
- Don't overwrite existing group in `create_group` (#433)
- Refactor tables (#436)
- Add a Hdf mixin that takes care of type info + general boilerplate (#416)
- Don't call pandas' to_hdf method (#420)
- Add tqdm to Project.iter_jobs  (#423)
- Script Job use Datacontainer for output (#418)
- Fix storage  bug (#431) 
- Update template job to use ~~DataContainer~~ HasStorage (#428)
- Creator registration  integration (#427)
- Flattened storage store defaults (#429)
- Database Hotfix  (#340)

# pyiron_base-0.3.2
- Fix downstream test suite (#425)

# pyiron_base-0.3.1
- Adding project.maintenance (#396)
- Remove append from GenericJob  (#415)
- Customized TestCase for pyiron  (#419)
- Fixes for pyiron table (#414)

# pyiron_base-0.3.0
- Allow lazy loading of DataContainers  (#367)

# pyiron_base-0.2.24
- A generic units class for pyiron (#397)
- Fix HDF of `FlattenedStorage` (#389)
- Strip first axis when creating array in `add_chunk` (#390)
- Allow filtering by job type (#398)

# pyiron_base-0.2.23
- Correct unpacking (#394)
- Postgres performance (#361)

# pyiron_base-0.2.22
- Enable to remove a job with loading the full job object.  (#384)
- Extend `load_file` to handle file objects.  (#379)
- Fix `read_only` in `DataContainer.from_hdf` (#387)
- Increase chemical formula limit to 50 characters  (#386)

# pyiron_base-0.2.21
- Fix per element/chunk confusion in FlattenedStorage.add_chunk  (#383)
- Remove dead code (#372)

# pyiron_base-0.2.20
- Add `_repr_html_` to display DataContainer in notebook mode (#371)
- Add FlattenedStorage from pyiron_contrib  (#375)
- Properly cleanup copyto tests (#373)

# pyiron_base-0.2.19
* Use correct project in copy_to (#363)

# pyiron_base-0.2.18
- Update to GenericMaster
- Dependency updates

# pyiron_base-0.2.17
- Remove HDFStoreIO (#346)
- Replace nan with None (#352)
- Add Interface for list_nodes/list_groups (#333)

# pyiron_base-0.2.16
Bugfix:
- Move create pipeline to `pyiron_base`, i.e. remove it here.
- Allow for `None` in addition to  `np.nan` and convert it to `np.nan`  for the `masterid` upon importing an archived database entry.

# pyiron_base-0.2.15
- Hotfix for `copy_to`, see [this](https://github.com/pyiron/pyiron_atomistics/issues/223).

# pyiron_base-0.2.14
* Set master ID for child jobs 
* Add hook after `copy_to` on Jobs 
* Add method to manually trigger import warning 
* Change lookup order in `JobCore.__getitem__` 

# pyiron_base-0.2.13
- Remove obsolete ProjectStore
- Always add HDF5 file extension to path
- Project data hotfix

# pyiron_base-0.2.12
- allow recursive projection creation based on absolute path
- ipynb file support
- force PIL to look up registered extensions upon import
