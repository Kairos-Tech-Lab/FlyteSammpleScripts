import typing
import pandas as pd
import numpy as np
from typing import Tuple

from flytekit import task, workflow, Workflow

@task
def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
    return a + 2, "world"


@task
def t2(a: str, b: str) -> str:
    return b + a


@workflow
def my_wf(a: int, b: str) -> Tuple[int, str]:
    x, y = t1(a=a)
    d = t2(a=y, b=b)
    return x, d

@task
def t1(a: str) -> str:
    return a + " world"


@task
def t2():
    print("side effect")


@task
def t3(a: typing.List[str]) -> str:
    """
    This is a pedagogical demo that happens to do a reduction step. Flyte is higher-order orchestration
    platform, not a map-reduce framework and is not meant to supplant Spark et. al.
    """
    return ",".join(a)

wf = Workflow(name="my.imperative.workflow.example")
wf.add_workflow_input("in1", str)
node_t1 = wf.add_entity(t1, a=wf.inputs["in1"])
wf.add_workflow_output("output_from_t1", node_t1.outputs["o0"])
wf.add_entity(t2)
wf_in2 = wf.add_workflow_input("in2", str)
node_t3 = wf.add_entity(t3, a=[wf.inputs["in1"], wf_in2])

wf.add_workflow_output(
    "output_list",
    [node_t1.outputs["o0"], node_t3.outputs["o0"]],
    python_type=typing.List[str],
)


if __name__ == "__main__":
    print(wf)
    print(wf(in1="hello", in2="foo"))
