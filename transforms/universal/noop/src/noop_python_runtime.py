from data_processing.runtime.pure_python import PythonTransformLauncher
from noop_transform import NOOPTransformConfiguration, logger


if __name__ == "__main__":
    # launcher = NOOPRayLauncher()
    launcher = PythonTransformLauncher(NOOPTransformConfiguration())
    logger.info("Launching noop transform")
    launcher.launch()
