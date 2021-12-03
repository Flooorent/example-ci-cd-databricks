# example-ci-cd-databricks
Example CI/CD with Databricks

## Python stuff

### Dependency management
Right now all dependencies, even the ones used for dev (e.g. chispa), are pinned and stored in `requirements.txt`. This
is not optimal: at the very least prod and dev dependencies should be separated, and we should be able to know which
dependendencies are actually needed by our project and which dependencies are needed by those dependencies. In essence,
we should probably use a tool such as a [pip-tools](https://github.com/jazzband/pip-tools) or
[poetry](https://github.com/python-poetry/poetry).

### Unit testing
We use [pytest](https://docs.pytest.org/en/6.2.x/) as our testing framework and
[chispa](https://github.com/MrPowers/chispa) for testing Spark code.

### Code formatter
We use [black](https://github.com/psf/black) as our code formatter.
We configured it via file `pyproject.toml` to use lines with at maximum 120 characters. NB: it's totally up to you and
your team to decide which length you want to use, either PEP's 79, 88, or another number. The nice thing about that is
that at least we show how we can configure black.

To check which files would be formatted with which modification (without actually performing the modifications), run:
```bash
black --diff --color .
```

To format the code:
```bash
black .
```

If you can't remember that command, simply run:
```bash
make format
```

NB: black is used as a check in the CI.

### Code compliance with (most of) PEP8
We use [flake8](https://flake8.pycqa.org/en/latest/index.html) to check for code compliance with (most of) PEP8
rules. It is configured with file `.flake8`.

To check for PEP8 compliance:
```bash
flake8 .
```

If you can't remember that command, simply run:
```bash
make check
```

NB: flake8 is used as a check in the CI.

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
