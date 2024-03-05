from kfp_support.workflow_support.utils import PipelinesUtils


def test_pipelines():
    """
    Test pipelines utils
    """
    utils = PipelinesUtils()
    # get pipeline by name
    pipeline = utils.get_pipeline_by_name("[Tutorial] DSL - Control structures")
    assert pipeline is not None
    # get default experiment
    experiment = utils.get_experiment_by_name()
    assert experiment is not None
    # start pipeline
    run = utils.start_pipeline(pipeline=pipeline, experiment=experiment, params={})
    assert run is not None
    # wait for completion
    status, error = utils.wait_pipeline_completion(run_id=run, wait=10)
    assert status.lower() == "succeeded"
    assert error == ''
