# DPK Planning Agent GUI

A web interface built with Gradio to visually generate, edit, and execute data transformation pipelines using the `data-prep-toolkit-transforms` library.

## Architecture

This project follows a component-based design for modularity, maintainability, and clarity.

### Core Design

The system is structured around three main components that mirror user intentions:

1. **Generate** – User provides a natural language prompt to generate a YAML pipeline.
2. **Edit** – User gives modification instructions to update the YAML.
3. **Run** – Final YAML is executed and outputs are captured.

Each component is implemented as a pure function (e.g., `_call_dpk_plan`, `_call_dpk_judge`, `_call_dpk_run`) and integrated using a LangGraph `StateGraph`, which provides:

- **Explicit state transitions**: Routing between generate, edit, and run via `router()`.
- **Deterministic flow control**: Helps with debugging and extensibility.
- **Built-in end states**: Ensures flow halts when a task is completed.

### State Management

`GraphState` is a typed dictionary defining all possible state variables shared across the nodes. This avoids ad-hoc state passing and enforces structure in the data pipeline.

### Gradio UI

The frontend is declaratively defined using Gradio's `Blocks` layout:

- **Textbox + Button**: For user input (prompts or edit instructions).
- **Code Components**: Display YAML and shell output interactively.
- **Clear Button**: Resets all inputs and outputs.

Three main UI sections correspond to the three states:

- **Generate**: Prompt → YAML
- **Edit**: YAML + Instructions → New YAML
- **Run**: YAML → Execution logs

### DPK Integration

- Tries to import `data-prep-toolkit-transforms`. If unavailable, falls back to demo mode with fake output.
- Uses `TransformRuntimeConfiguration` and `multi_launcher` to run YAML pipelines programmatically.
- Writes pipeline YAML to a temporary file and launches it in-process.
- Captures stdout and stderr for user visibility.

### Fallback & Error Handling

- Demo mode: If `data-prep-toolkit-transforms` is not installed, the app still runs in a limited, mock mode.
- Full traceback returned in case of exception (visible in Gradio output).

###  File Structure

All logic is contained in a single Python script (`dpk_agent_gui.py`):
## Prerequisites

- **Python Version**: Python 3.12 is recommended. The core `data-prep-toolkit-transforms` package is compatible with versions 3.10–3.12.

## Installation

### Create and Activate a Virtual Environment

Create a dedicated virtual environment using a compatible Python version (3.12 is recommended).

```bash
# Create the environment using Python 3.12
python3.12 -m venv venv

# Activate it
# On macOS/Linux:
source venv/bin/activate
# On Windows:
.\venv\Scripts\activate
```

### Install Dependencies

With your virtual environment active, run the following command to install all necessary libraries:

```bash
pip install "data-prep-toolkit-transforms[language]" gradio langgraph torch
```

## Usage

Make sure your virtual environment is active.

Run the application script from your terminal:

```bash
python dpk_agent_gui.py
```

Then, open your web browser to the local URL provided in the console (e.g., `http://127.0.0.1:7860`).

