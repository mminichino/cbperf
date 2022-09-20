import pytest


def pytest_addoption(parser):
    parser.addoption("--host", action="store", default="localhost")
    parser.addoption("--bucket", action="store", default="testrun")
    parser.addoption('--user', action='store', default="Administrator")
    parser.addoption('--password', action='store', default="password")
    parser.addoption('--external', action='store_true')


@pytest.fixture
def hostname(request):
    return request.config.getoption("--host")


@pytest.fixture
def bucket(request):
    return request.config.getoption("--bucket")


@pytest.fixture
def username(request):
    return request.config.getoption("--user")


@pytest.fixture
def password(request):
    return request.config.getoption("--password")


@pytest.fixture
def external(request):
    return request.config.getoption("--external")
