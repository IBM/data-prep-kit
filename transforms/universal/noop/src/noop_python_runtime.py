from data_processing.runtime.pure_python import PythonTransformLauncher
from noop_transform import NOOPRayTransformConfiguration, logger


if __name__ == "__main__":
    # launcher = NOOPRayLauncher()
    launcher = PythonTransformLauncher(NOOPRayTransformConfiguration())
    logger.info("Launching noop transform")
    launcher.launch()
