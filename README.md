# example-ci-cd-databricks
Example CI/CD with Databricks

## Workflow
- use proper .gitignore file
- create virtual env
- install dependencies and pin them in requirements.txt

## Continuous delivery scripts
Those scripts use various Python libraries that need to be installed first. So before running them, create a virtual
environment with Python 3.7+ and run:
```
pip install -r ./cd-scripts/requirements.txt
```

### Create job
We use the python script `./cd-scripts/create_prod_job.py` to create the job. Make sure module `requests` is installed
in your (virtual) environment.

Run:
```
python ./cd-scripts/create_job.py \
--databricks-instance <databricks-instance> \
--pat <pat> \
--wheel-version <wheel-version> \
--dbfs-wheel-dir <dbfs-wheel-dir> \
--scope <scope> \
--storage-uri <storage-uri> \
--storage-key <storage-key>
```

This command will output the associated `job_id` (that you will have to reuse to update the job).

### Update job
Run:
```
python ./cd-scripts/update_job.py \
--job-id <job-id> \
--databricks-instance <databricks-instance> \
--pat <pat> \
--wheel-version <wheel-version> \
--dbfs-wheel-dir <dbfs-wheel-dir> \
--scope <scope> \
--storage-uri <storage-uri> \
--storage-key <storage-key>
```
