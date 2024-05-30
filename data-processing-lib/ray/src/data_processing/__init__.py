# The following to try and allow a common root package to be used in multiple data-prep-kit libs
# Taken from https://stackoverflow.com/questions/26058978/python-import-different-subpackages-with-the-same-root-packge-name-and-different
from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)
