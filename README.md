# Data-Warehouse
This repository contains code relating to the data warehouse for RightAtSchool.

## Requirements

The project is based on Python 3. You should install the latest stable version for your platform and add python to your path.

## Deployment

You should use git to clone this repository.

```shell
$ git clone git@github.com:IvyArbor/RightAtSchool.git
```

or

```shell
$ git clone 
```

After cloning the code, switch to the `RightAtSchool` folder:

```bash
$ cd RightAtSchool
```

and then install the dependencies, and create an environment file by running the following commands:

```bash
$ pip install -r requirements.txt
$ cp .env.example .env
```

If you have the `make` tool installed, you can complete these two steps by running `make init`.

## Configuration

The `.env` file should now contain the deployment-specific project settings. Ensure that you have updated these values before running.

## Running jobs

All the available jobs are present in the `jobs` folder. Each job is placed in a separate file. In order to run a specific job contained in `jobs/<JobName>.py`, from the main project folder run:

```shell
$ python job.py <JobName>
```

where  `<JobName>` is the name if the file without the `.py` extension.

#### Options:

Additionally, one can pass one or both of the following options when running a job:

* `-v` for running the job in verbose mode, printing additional information at each step.
* `-d` for running the job in debug mode, stopping the execution before and after important steps.

Example: `$ python job.py <JobName> -d -v`

