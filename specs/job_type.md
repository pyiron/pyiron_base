# Job handling

In pyiron jobs can be very conveniantly be initialized from the `Project` instance:
```python
from pyiron import Project
pr = Project('test')

job = pr.create.job.MyJob('job_name')
```

### Job registration

To enable this feature without the burden to `import` all available `JobClasses` on startup of pyiron, new jobs need to be registered as `JobType`:

```python
from pyiron_base import JobType

JobType.register('pyiron_module.submodule.my_job_class_module', 'MyJobClass')
```

The register `@classmethod` performs some (minor) sanity checks, in particular raises an error whenever there is already such a `MyJobClass` registered with a different class. To replace an already existing job class, an `overwrite` argument is available.

### Job development

To facilitate the development of new job classes (e.g. in Notebooks), every subclass of `GenericJob` auto-registers as a new job type. 
Since this requires the import of the corresponding class at runtime, prior to the `pr.create.job.MyJob()` call, a final incorporation of the job into pyiron requires to register using the string based version from above.

Attempts to auto-register a job type which is already known to `JobType` is reported in the `pyiron.log` and otherwise ignored. 

It is also possible to `unregister` a given job using the corresponding unregister method. This is particular useful to omit the auto-registration in case of abstract job classes and can be used as `@docorator`:
```python
@JobType.unregister
class MyBaseJobClass(GenericJob):
    """Abstract base class for all jobs using XYZ..."""
```

### WIP: Job namespace / tags

It is under consideration to enhance the job registration by providing a scope for the job a la 
```python
JobType.register('module.path', 'JobClass', branch='atomistics', tag='DFT')
```
which would allow to filter the available job types by these extra inforation.
