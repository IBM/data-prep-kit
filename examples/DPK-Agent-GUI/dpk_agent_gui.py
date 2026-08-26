import os
import re
import tempfile
import traceback
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from typing import Optional, TypedDict

import gradio as gr
from langgraph.graph import END, StateGraph


DPK_AVAILABLE = False
try:
    from data_processing.runtime.transform_launcher import (
        TransformRuntimeConfiguration,
        multi_launcher,
    )

    DPK_AVAILABLE = True
    print("SUCCESS: 'data-prep-toolkit-transforms' library found and imported.")
except ImportError:
    print("FAILURE: 'data-prep-toolkit-transforms' not found. Running in demo mode.")
    print("         Install it with: pip install 'data-prep-toolkit-transforms[language]'")

    def TransformRuntimeConfiguration(params):
        pass

    def multi_launcher(params, launcher):
        pass


# --- LangGraph State Definition ---
class GraphState(TypedDict):
    """Defines the state structure for the agent's workflow."""

    next_node: Optional[str]
    prompt: Optional[str]
    edit_instructions: Optional[str]
    pipeline_yaml: Optional[str]
    run_output: Optional[str]
    error: Optional[str]


# --- DPK Integration Functions ---
def _call_dpk_plan(prompt: str) -> dict:
    if not DPK_AVAILABLE:
        return {"pipeline_yaml": f"# [DEMO] Generating pipeline for: {prompt}\n"}

    try:
        match = re.search(r"['\"]([^'\"]+)['\"]", prompt)
        if match:
            filename = match.group(1)
            yaml_output = f"""# Pipeline generated to read the specified file.
transforms:
  - name: read_and_print_file
    params:
      file_path: "{filename}"
"""
        else:
            yaml_output = f"""# Could not determine a filename from your prompt.
# Please specify a filename in quotes, e.g., "Read 'file.txt'".
transforms:
  - name: echo
    params:
      message_to_print: "Executing task for: {prompt}"
"""
        return {"pipeline_yaml": yaml_output}
    except Exception as e:
        return {"error": f"# Unexpected error in DPK plan simulation: {e}\n{traceback.format_exc()}"}


def _call_dpk_judge(existing_yaml: str, instructions: str) -> dict:
    if not DPK_AVAILABLE:
        return {"pipeline_yaml": existing_yaml.rstrip() + f"\n# [DEMO edit] {instructions.strip()}\n"}

    try:
        edited_yaml = None
        new_file_match = re.search(r"to\s+(['\"]?)([\w\.-]+)\1", instructions)
        if new_file_match:
            new_filename = new_file_match.group(2)
            (updated_yaml, num_replacements) = re.subn(
                r"(file_path:\s*['\"]).*?(['\"])", rf"\g<1>{new_filename}\g<2>", existing_yaml
            )
            if num_replacements > 0:
                edited_yaml = updated_yaml

        if edited_yaml is None:
            edited_yaml = existing_yaml + f"\n# Edit instruction applied: {instructions}\n"

        return {"pipeline_yaml": edited_yaml}
    except Exception as e:
        return {"error": f"# Unexpected error in DPK judge simulation: {e}\n{traceback.format_exc()}"}


def _call_dpk_run(pipeline_yaml: str) -> dict:
    if not DPK_AVAILABLE:
        return {"run_output": "[DEMO] Would run pipeline:\n" + pipeline_yaml}

    path = ""
    try:
        with tempfile.NamedTemporaryFile("w+", suffix=".yaml", delete=False) as f:
            f.write(pipeline_yaml)
            path = f.name

        params = {"data-prep-kit_config": path}
        launcher = TransformRuntimeConfiguration(params)

        f_out = StringIO()
        f_err = StringIO()
        with redirect_stdout(f_out), redirect_stderr(f_err):
            multi_launcher(params, launcher)

        output = f"--- STDOUT ---\n{f_out.getvalue()}\n\n--- STDERR ---\n{f_err.getvalue()}"
        return {"run_output": output}
    except Exception as e:
        return {"error": f"# Error running pipeline: {e}\n{traceback.format_exc()}"}
    finally:
        if os.path.exists(path):
            os.remove(path)


def generate_node(state: GraphState) -> dict:
    return _call_dpk_plan(state.get("prompt", ""))


def edit_node(state: GraphState) -> dict:
    return _call_dpk_judge(state.get("pipeline_yaml", ""), state.get("edit_instructions", ""))


def run_node(state: GraphState) -> dict:
    return _call_dpk_run(state.get("pipeline_yaml", ""))


def router(state: GraphState) -> str:
    return state.get("next_node", "")


workflow = StateGraph(GraphState)
workflow.set_entry_point("entry")
workflow.add_node("entry", lambda state: {**state})
workflow.add_node("generate", generate_node)
workflow.add_node("edit", edit_node)
workflow.add_node("run", run_node)
workflow.add_conditional_edges("entry", router, {"generate": "generate", "edit": "edit", "run": "run", "": END})
workflow.add_edge("generate", END)
workflow.add_edge("edit", END)
workflow.add_edge("run", END)
app = workflow.compile()


def handle_generate(prompt: str) -> str:
    if not prompt.strip():
        return "# Please enter a pipeline description."
    result = app.invoke({"next_node": "generate", "prompt": prompt})
    return result.get("pipeline_yaml") or result.get("error", "An unknown error occurred.")


def handle_edit(existing_yaml: str, instructions: str) -> str:
    if not existing_yaml.strip() or not instructions.strip():
        return existing_yaml
    result = app.invoke({"next_node": "edit", "pipeline_yaml": existing_yaml, "edit_instructions": instructions})
    return result.get("pipeline_yaml") or result.get("error", "An unknown error occurred.")


def handle_run(pipeline_yaml: str) -> str:
    if not pipeline_yaml.strip():
        return "# No pipeline to run."
    result = app.invoke({"next_node": "run", "pipeline_yaml": pipeline_yaml})
    return result.get("run_output") or result.get("error", "An unknown error occurred.")


with gr.Blocks(title="DPK Planning Agent GUI", theme=gr.themes.Soft()) as demo:
    gr.Markdown("# DPK Planning Agent GUI")
    if not DPK_AVAILABLE:
        gr.Warning("DPK library not found. Running in demo mode. Functionality will be limited.")
    with gr.Row():
        with gr.Column(scale=2):
            gr.Markdown("### 1. Generate Pipeline")
            prompt_in = gr.Textbox(
                label="Pipeline Description", placeholder="e.g. Read 'input.csv', filter rows...", lines=3
            )
            gen_btn = gr.Button("Generate YAML", variant="primary")
            gr.Examples(
                [
                    "Read a CSV from 'input.csv' and print the first 5 rows.",
                    "Read two CSVs and merge them on the 'id' column.",
                ],
                inputs=prompt_in,
                label="Example Prompts",
            )
        with gr.Column(scale=3):
            yaml_box_1 = gr.Code(label="1. Generated Pipeline (Raw Output)", language="yaml", interactive=False)
    with gr.Row():
        with gr.Column(scale=2):
            gr.Markdown("### 2. Edit Pipeline (Optional)")
            edit_in = gr.Textbox(
                label="Edit Instructions", placeholder="e.g. Change the output file to 'output.csv'", lines=3
            )
            edit_btn = gr.Button("Apply Edit", variant="primary")
        with gr.Column(scale=3):
            yaml_box_2 = gr.Code(
                label="2. Final Pipeline YAML (Edit Here Before Running)", language="yaml", interactive=True
            )
    with gr.Row():
        with gr.Column():
            gr.Markdown("### 3. Run Pipeline")
            run_btn = gr.Button("Run Final Pipeline", variant="stop")
            run_out = gr.Code(label="Pipeline Console Output", language="shell", interactive=False)

    gen_btn.click(fn=handle_generate, inputs=prompt_in, outputs=yaml_box_1)
    gen_btn.click(fn=lambda x: x, inputs=yaml_box_1, outputs=yaml_box_2)  # Copy generated YAML to final box
    edit_btn.click(fn=handle_edit, inputs=[yaml_box_1, edit_in], outputs=yaml_box_2)
    run_btn.click(fn=handle_run, inputs=yaml_box_2, outputs=run_out)
    gr.ClearButton([prompt_in, yaml_box_1, edit_in, yaml_box_2, run_out], value="Clear All Fields")

if __name__ == "__main__":
    demo.launch()
