import pytest

def pytest_addoption(parser):
    parser.addoption("--host", action="store", default="localhost")
    parser.addoption("--bucket", action="store", default="testrun")

@pytest.fixture
def hostname(request):
    return request.config.getoption("--host")

@pytest.fixture
def bucket(request):
    return request.config.getoption("--bucket")
