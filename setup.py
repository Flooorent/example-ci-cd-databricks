import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open('requirements.txt') as f:
    required = f.read().splitlines()

setuptools.setup(
    name="example_ci_cd_databricks",
    version="0.0.3",
    author="Florent Moiny",
    author_email="florent.moiny@gmail.com",
    description="Example CI/CD project with Databricks",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Flooorent/example-ci-cd-databricks",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
    install_requires=required,
    entry_points={
        "console_scripts": [
            "job = example_ci_cd_databricks.main:entry_point",
        ],
    },
)
