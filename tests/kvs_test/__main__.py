#!/usr/bin/env python3

import argparse
import datetime
import os
from pathlib import Path

import docker

from . import hw2_tests, hw3_tests, hw4_tests
from .hw2_tests.advanced_tests import ADVANCED_TESTS
from .containers import ClusterConductor, ContainerBuilder
from .util import capture_logs, log

# Import all API versions
from .hw2_api import KvsFixture as KvsFixture2
from .hw3_api import KvsFixture as KvsFixture3
from .hw4_api import KvsFixture as KvsFixture4

CONTAINER_IMAGE_ID = "kvstore-hw4-test"
TEST_GROUP_ID = "hw4"


class TestRunner:
    def __init__(self, project_dir: str, docker_client: docker.DockerClient):
        self.project_dir = project_dir
        # builder to build container image
        self.builder = ContainerBuilder(
            docker_client=docker_client,
            project_dir=project_dir,
            image_id=CONTAINER_IMAGE_ID,
        )
        # network manager to mess with container networking
        self.conductor = ClusterConductor(
            docker_client=docker_client,
            group_id=TEST_GROUP_ID,
            base_image=CONTAINER_IMAGE_ID,
            external_port_base=9000,
        )

    def prepare_environment(self) -> None:
        log("\n-- prepare_environment --")
        # build the container image
        if os.environ.get("KVS_SKIP_BUILD") == "1":
            log("Skipping image build (KVS_SKIP_BUILD=1)")
        else:
            self.builder.build_image()

        # aggressively clean up anything kvs-related
        # NOTE: this disallows parallel run processes, so turn it off for that
        self.conductor.cleanup_hanging(group_only=False)

    def cleanup_environment(self) -> None:
        log("\n-- cleanup_environment --")
        # destroy the cluster
        self.conductor.destroy_cluster()
        # aggressively clean up anything kvs-related
        # NOTE: this disallows parallel run processes, so turn it off for that
        self.conductor.cleanup_hanging(group_only=True)


def parse_args():
    parser = argparse.ArgumentParser(description="Run KVS cluster tests")
    parser.add_argument("path", help="Path to the project directory containing Dockerfile")
    parser.add_argument("-f", "--filter", help="Filter tests by name")
    parser.add_argument(
        "--no-fail-fast",
        action="store_false",
        dest="fail_fast",
        help="Continue testing even if a test case fails",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory to save test results and logs",
    )
    parser.add_argument(
        "--hw",
        type=int,
        choices=[2, 3, 4],
        default=4,
        help="Which homework tests to run (2, 3, or 4)",
    )
    return parser.parse_args()


def main():
    # parse command line arguments
    args = parse_args()

    # use provided project directory path
    project_dir = args.path
    runner = TestRunner(project_dir=project_dir, docker_client=docker.from_env())

    # prepare to run tests
    runner.prepare_environment()

    # Select appropriate test set and KvsFixture class based on hw arg
    if args.hw == 2:
        tests = [
            *hw2_tests.basic.BASIC_TESTS,
            *ADVANCED_TESTS,
        ]
        KvsFixture = KvsFixture2
        log("Running Assignment 2 tests with HW2 API")
    elif args.hw == 3:
        tests = [
            *hw3_tests.BASIC_TESTS,
            *hw3_tests.CAUSAL_CONSISTENCY_TESTS,
            *hw3_tests.EVENTUAL_CONSISTENCY_TESTS,
            *hw3_tests.VIEW_CHANGE_TESTS,
            *hw3_tests.AVAILABILITY_TESTS,
        ]
        KvsFixture = KvsFixture3
        log("Running Assignment 3 tests with HW3 API")
    else:  # args.hw == 4
        tests = [
            *hw4_tests.BASIC_SHARDING_TESTS,
            *hw4_tests.ADVANCED_SHARDING_TESTS,
            *hw4_tests.RESHARDING_TESTS,
            *hw4_tests.PERFORMANCE_TESTS,
            *hw4_tests.CRITICAL_EDGE_CASE_TESTS,
        ]
        
        # Add compatibility tests only when explicitly filtered
        if args.filter and ("compatibility" in args.filter or "backwards" in args.filter):
            tests.extend(hw4_tests.COMPATIBILITY_TESTS)
        
        KvsFixture = KvsFixture4  # Use the enhanced HW4 API
        log("Running Assignment 4 tests with HW4 API (sharding support)")

    if args.output_dir is None:
        args.output_dir = Path(args.path, "test_results")
    output_dir = args.output_dir / datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # apply test filter if provided
    if args.filter:
        log(f"filtering tests by: {args.filter}")
        tests = [t for t in tests if args.filter in t.name]

    # run tests
    log("\n== RUNNING TESTS ==")
    run_tests = []
    for test in tests:
        test_dir = output_dir / test.name

        should_stop = False

        log("\n")
        with capture_logs() as logs:
            log(f"== TEST: [{test.name}] ==\n")
            run_tests.append(test)
            with runner.conductor:
                # record clients created to save logs later
                fx = KvsFixture()
                score, reason = test.execute(runner.conductor, fx)
                # dump container logs
                runner.conductor.dump_logs(path=test_dir / "nodes")
                # dump client logs
                for client in fx.clients:
                    client.dump_logs(path=test_dir / "clients")

            log("\n")
            if score:
                log(f"✓ PASSED {test.name}")
            else:
                log(f"✗ FAILED {test.name}: {reason}")
                if args.fail_fast:
                    log("FAIL FAST enabled, stopping at first failure")
                    should_stop = True

        # save test log
        (test_dir / "log.txt").write_text(logs.buffer, encoding='utf-8')

        if should_stop:
            break

    log("\n")

    with capture_logs() as logs:
        # summarize the status of all tests
        log("== TEST SUMMARY ==")
        for test in run_tests:
            log(f"  - {test.name}: {'✓' if test.score else '✗'}")

    # save summary
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "summary.txt").write_text(logs.buffer, encoding='utf-8')

    # clean up
    runner.cleanup_environment()


if __name__ == "__main__":
    main()