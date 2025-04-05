import sys
import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/keeper_config.xml",
        "configs/logger.xml",
    ],
    stay_alive=True,
)


def change_logger_level(level):
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"""echo "
<clickhouse>
    <logger>
        <level>{level}</level>
        <log>/var/log/clickhouse-server/clickhouse-server.log</log>
        <errorlog_level>error</errorlog_level>
        <errorlog>/var/log/clickhouse-server/clickhouse-server.err.log</errorlog>
        <size>200M</size>
        <count>10</count>
    </logger>
</clickhouse>
            " > /etc/clickhouse-server/config.d/logger.xml
            """,
        ]
    )


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_adjust_log_level(start_cluster):
    change_logger_level("warning")
    node.restart_clickhouse()

    assert (
        int(
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "grep '<Trace>' /var/log/clickhouse-server/clickhouse-server.log | wc -l",
                ],
                privileged=True,
                user="root",
            )
        )
        == 0
    )

    # Adjust log level.
    change_logger_level("trace")
    time.sleep(3)
    node.query(
        "SELECT * FROM system.zookeeper SETTINGS allow_unrestricted_reads_from_keeper = 'true'"
    )
    node.exec_in_container(
        [
            "bash",
            "-c",
            "sync",
        ],
        privileged=True,
        user="root",
    )
    for _ in range(10):
        if (
            int(
                node.exec_in_container(
                    [
                        "bash",
                        "-c",
                        "grep '<Trace>' /var/log/clickhouse-server/clickhouse-server.log | wc -l",
                    ],
                    privileged=True,
                    user="root",
                )
            )
            >= 1
        ):
            break
        time.sleep(0.5)
    else:
        assert False
