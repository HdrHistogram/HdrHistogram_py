import pytest
def pytest_addoption(parser):
    parser.addoption("--runperf", action="store_true",
        help="run perf tests")

def pytest_runtest_setup(item):
    if 'perf' in item.keywords and not item.config.getoption("--runperf"):
        pytest.skip("need --runperf option to run")