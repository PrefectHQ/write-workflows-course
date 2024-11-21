# Write workflows with Prefect

This repository contains code that accompanies the Prefect course for workflows authors.

Join the free course and learn how to build resilient workflows with Prefect here. TK add link

## Setup

Use your chosen Python virtual environment manager.
We suggest using [uv](https://docs.astral.sh/uv/) because it's fast and relatively quick to set up, but feel free to use your preferred tool.
The examples below show uv.

### Download the repository

```bash
git clone https://github.com/PrefectHQ/write-workflows-course.git
```

### Navigate to the course directory

```bash
cd write-workflows-course
```

### Install uv (if needed)

For macOS/Linux:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

For Windows:

```bash
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

For troubleshooting, see the [uv installation guide](https://docs.astral.sh/uv/getting-started/installation).

### Create a new virtual environment

```bash
uv venv --python 3.12
```

You should see a note that the virtual environment was created successfully and instructions for how to activate it.

### Activate the virtual environment

```bash
source .venv/bin/activate
```

You should see the virtual environment name in parentheses in your terminal prompt .

### Install Prefect and the other necessary Python packages

```bash
uv pip install -r requirements.txt
```

## Start a self hosted Prefect server instance or connect to Prefect Cloud
