# Proof of Concept for ELT using Dagster, DLT and DBT

This proof-of-concept illustrates the use of Dagster, Data Load Tool (DLT) for performing extract and load, and Data Build Tool (DBT) for performing post load transforms.

It uses DuckDB to back the data lake, but this can easily be swapped to anything supported as both a:
    * [DLT destination](https://dlthub.com/docs/dlt-ecosystem/destinations/)
    * DBT adapter - [trusted](https://docs.getdbt.com/docs/trusted-adapters) or [community](https://docs.getdbt.com/docs/community-adapters)

## Project Layout

* `orchestration`: the dagster project
* `orchestration/dlt_sources`: the dlt project (with unnecessary config files managed by dagger ommitted)

## Dev Environment Setup

### Install the essentials

Note that pyenv, pipx, and direnv are merely suggestion for multiple python versions, python executable isolation, and env var setting convenience.  Feel free to choose whatever you prefer:
```
brew install docker tofu python duckdb pyenv pipx direnv
# If you installed direnv, make sure to enable for your shell: https://direnv.net/docs/hook.html
# Enable .env files with direnv
mkdir -p ~/.config/direnv
echo "[global]\nload_dotenv = "true"\n" >~/.config/direnv/direnv.toml

# This block is optional
pyenv install 3.12
pyenv global 3.12
# you may need to setup your shell for pyenv as described here: https://github.com/pyenv/pyenv?tab=readme-ov-file#set-up-your-shell-environment-for-pyenv
pipx ensurepath

# Setup a python virtual environment
python -m venv .venv
source .venv/bin/activate

# orchestration will be pwd going forward
cd orchestration
# Load up env vars for use by the pipelines
direnv allow
```

### Startup the data stores

First, edit `orchestration/.env` and `dlt/.dlt/secrets.toml` to the values you desire.

```
# Startup postgres
docker-compose up
```

### Start Dagster

```
# Install dev dependencies for the current directory, making them editable so we can see changes immediately
pip install -e ".[dev]"
# Starts dagster dev environment on http://localhost:3000
dagster dev
```

You can now use the dagster UX to materialize data assets.

## Development

Open http://localhost:3000 with your browser to see the project.

You can start writing assets in `orchestration/assets.py`. The assets are automatically loaded into the Dagster code location as you define them.

### Adding New Python Dependencies

You can specify new Python dependencies in `setup.py`.

### Unit Testing

Tests are in the `orchestration_tests` directory and you can run tests using `pytest`:

```bash
pytest orchestration_tests
```

### Schedules and Sensors

If you want to enable Dagster [Schedules](https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules) or [Sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors) for your jobs, the [Dagster Daemon](https://docs.dagster.io/deployment/dagster-daemon) process must be running. This is done automatically when you run `dagster dev`.

Once your Dagster Daemon is running, you can start turning on schedules and sensors for your jobs.

## Deploy on Dagster Cloud

The easiest way to deploy your Dagster project is to use Dagster Cloud.

Check out the [Dagster Cloud Documentation](https://docs.dagster.cloud) to learn more.

## Helpful Tools / Commands
    * faker-cli: `pipx install faker-cli`
    * browse duckdb: `duckdb /tmp/poc.duckdb`

## Project Bootstrap Command History

The following were used to bootstrap the project (no need to run them again):
```
# Install dagster CLI
pipx install dagster --python 3.12

# Scaffold dagster project - https://docs.dagster.io/getting-started/create-new-project
dagster project scaffold --name core_data

# Uninstall dagster CLI so we can reinstall a version locked with all of its other dependencies
pipx uninstall dagster
# Dagster will nest under a directory of the same name by default, change the parent directory to orchestration
mv core_data orchestration
cd orchestration

# Create the DLT definitions
mkdir dlt_sources
cd dlt_sources
dlt init chess postgres
dlt init sql_database duckdb
# Manual: move the dependencies from dlt/requirements.txt into setup.py
# Manual: port DLT secrets.tom into .env file, and then wire into Dagster assets

# Get rid of unused DLT files to avoid confusion
rm requirements.txt
rm -rf .dlt
rm *.py

# Install dlt dependencies
cd ..
pip install -e ".[dev]"
```
