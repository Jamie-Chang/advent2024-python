# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "interpreters-pep-734",
# ]
# ///
from __future__ import annotations

from contextlib import closing, contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from textwrap import dedent
from threading import Thread
from typing import Iterable, Iterator, Literal, cast
from interpreters_backport import interpreters
from interpreters_backport.interpreters.queues import Queue
from implementations.d6 import Grid, Pair, walk


type Shareable = (
    str | bytes | int | float | bool | None | tuple[Shareable, ...] | Queue | memoryview
)


@contextmanager
def timer(message: str):
    start = datetime.now(UTC)
    try:
        yield
    finally:
        print(
            f"{message} {(datetime.now(UTC) - start) / timedelta(seconds=1)} s elapsed"
        )


def parse_lines(lines: Iterable[str]) -> tuple[Pair, Grid]:
    start = None
    coord = (0, 0)
    rows = []
    for r, line in enumerate(lines):
        row: list[bool] = []
        for c, char in enumerate(line):
            coord = (r, c)

            if char == "^":
                start = coord
            row.append(char == "#")
        rows.append(tuple(row))

    assert start is not None
    return (start, tuple(rows))


@contextmanager
def read_lines(path: Path) -> Iterator[Iterator[str]]:
    with path.open() as f:
        yield (line.rstrip() for line in f)


class InterpreterError(Exception): ...


class Executor:
    def __init__(
        self,
        module: str,
        entrypoint: str,
        *,
        workers: int,
        **bind: Shareable,
    ) -> None:
        self.tasks = interpreters.create_queue()
        self.results = interpreters.create_queue()
        self.bind = bind

        self.code = dedent(f"""
            from {module} import {entrypoint} as entrypoint
            from interpreters_backport import interpreters

            tasks = interpreters.Queue({self.tasks.id})
            results = interpreters.Queue({self.results.id})
        
            while True:
                req = tasks.get()
                if req is None:
                    # Stop!
                    break
                try:
                    res = entrypoint(*req, {", ".join(f"{name}={name}" for name in bind)})
                except Exception as e:
                    results.put((False, repr(e)))
                else:
                    results.put((True, res))
        """)
        self.workers = workers

    def worker(self) -> None:
        interp = interpreters.create()
        if self.bind:
            interp.prepare_main(**self.bind)
        interp.exec(self.code)
        interp.close()

    @contextmanager
    def get_results(self, expected: int) -> Iterator[Iterator[Shareable]]:
        def _iter():
            for _ in range(expected):
                success, res = cast(
                    tuple[Literal[True], Shareable] | tuple[Literal[False], str],
                    self.results.get(),
                )
                if not success:
                    raise InterpreterError(res)

                yield res

        it = _iter()

        try:
            with closing(it):
                yield _iter()
        finally:
            for _ in range(self.workers):
                self.tasks.put(None)

    def map(self, its: Iterable[Shareable]) -> Iterator[Shareable]:
        threads = [Thread(target=self.worker) for _ in range(self.workers)]
        for thread in threads:
            thread.start()

        tasks = 0
        for elem in its:
            tasks += 1
            self.tasks.put(elem)

        with self.get_results(tasks) as results:
            yield from results

        for t in threads:
            t.join()


if __name__ == "__main__":
    with read_lines(Path("..") / "inputs" / "d6.txt") as lines:
        start, grid = parse_lines(lines)

    path = set(walk(grid, start))
    print("part1", len(path))
    for workers in range(1, 16):
        candidates = ((start, node) for node in path if node != start)
        executor = Executor("implementations.d6", "solve", workers=workers, grid=grid)
        with timer(f"{workers = }"):
            print("part2", sum(1 for res in executor.map(candidates) if res), end="; ")
