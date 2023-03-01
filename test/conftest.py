import pytest
import docker


def pytest_addoption(parser):
    parser.addoption("--hostname", action="store", default="localhost")
    parser.addoption("--external", action="store_true")


@pytest.fixture
def hostname(request):
    return request.config.getoption("--hostname")


def pytest_configure(config):
    pass


def pytest_sessionstart(session):
    external = session.config.getoption('--external')
    if external:
        return
    print("Starting test container")
    client = docker.from_env()
    container_id = client.containers.run('mminichino/cbdev:latest',
                                         detach=True,
                                         name="pytest",
                                         ports={
                                                8091: 8091,
                                                18091: 18091,
                                                8092: 8092,
                                                18092: 18092,
                                                8093: 8093,
                                                18093: 18093,
                                                8094: 8094,
                                                18094: 18094,
                                                8095: 8095,
                                                18095: 18095,
                                                8096: 8096,
                                                18096: 18096,
                                                8097: 8097,
                                                18097: 18097,
                                                11207: 11207,
                                                11210: 11210,
                                                9102: 9102,
                                                4984: 4984,
                                                4985: 4985,
                                         },
                                         )

    print("Container started")
    print("Waiting for container startup")

    while True:
        exit_code, output = container_id.exec_run(['/bin/bash',
                                                   '-c',
                                                   'test -f /demo/couchbase/.ready'])
        if exit_code == 0:
            break

    print("Waiting for Couchbase Server to be ready")
    exit_code, output = container_id.exec_run(['/demo/couchbase/cbperf/cb_perf',
                                               'list',
                                               '--host', '127.0.0.1',
                                               '--wait'])

    for line in output.split(b'\n'):
        print(line.decode("utf-8"))
    assert exit_code == 0

    print("Ready.")


def pytest_sessionfinish(session, exitstatus):
    external = session.config.getoption('--external')
    if external:
        return
    print("")
    print("Stopping container")
    client = docker.from_env()
    container_id = client.containers.get('pytest')
    container_id.stop()
    print("Removing test container")
    container_id.remove()
    print("Done.")


def pytest_unconfigure(config):
    pass
