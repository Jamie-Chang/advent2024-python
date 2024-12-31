from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from enum import StrEnum
from itertools import pairwise
from pathlib import Path
from typing import Hashable, Iterable, Iterator, Self, assert_never
from datetime import UTC, datetime, timedelta


@contextmanager
def timer(message: str):
    start = datetime.now(UTC)
    try:
        yield
    finally:
        print(
            f"{message} {(datetime.now(UTC) - start) / timedelta(seconds=1)} s elapsed"
        )


class Tile(StrEnum):
    obstruction = "#"
    empty = "."
    start = "^"


type Pair = tuple[int, int]


up = 0
right = 1
down = 2
left = 3


def turn(direction: int) -> int:
    return (direction + 1) % 4


@dataclass(slots=True)
class Ranges:
    rows: range
    cols: range

    def __getitem__(self, key: tuple[slice, int] | tuple[int, slice]) -> Iterator[Pair]:
        match key:
            case int() as r, slice() as cols:
                return ((r, c) for c in self.cols[cols])
            case slice() as rows, int() as c:
                return ((r, c) for r in self.rows[rows])
            case _ as other:
                assert_never(other)

    def __contains__(self, key: Pair) -> bool:
        row, col = key

        return row in self.rows and col in self.cols

    def __iter__(self) -> Iterator[Pair]:
        return ((row, col) for row in self.rows for col in self.cols)

    def walk(self, start: Pair, direction: int) -> Iterator[Pair]:
        match direction:
            case 0:
                return ((row, start[1]) for row in self.rows[start[0] :: -1])
            case 2:
                return ((row, start[1]) for row in self.rows[start[0] :: 1])
            case 3:
                return ((start[0], col) for col in self.cols[start[1] :: -1])
            case 1:
                return ((start[0], col) for col in self.cols[start[1] :: 1])
            case _:
                assert False


@dataclass(slots=True)
class Grid:
    start: Pair
    ranges: Ranges
    obstructions: set[Pair]

    @classmethod
    def from_lines(cls, lines: Iterable[str]) -> Self:
        start = None
        obstructions = set()
        coord = (0, 0)
        for r, line in enumerate(lines):
            for c, char in enumerate(line):
                coord = (r, c)

                if char == "^":
                    start = coord
                if char == "#":
                    obstructions.add(coord)

        assert start is not None
        return cls(
            start,
            Ranges(range(coord[0] + 1), range(coord[1] + 1)),
            obstructions,
        )


@contextmanager
def read_lines(path: Path) -> Iterator[Iterator[str]]:
    with path.open() as f:
        yield (line.rstrip() for line in f)


def walk(grid: Grid, obstruction: Pair | None = None) -> Iterator[Pair]:
    direction = up
    start = grid.start

    ranges = grid.ranges
    obstructions = grid.obstructions
    yield start

    while True:
        walk = pairwise(ranges.walk(start, direction))

        for prev, curr in walk:
            if curr in obstructions or curr == obstruction:
                direction = turn(direction)
                start = prev
                break

            yield curr
        else:
            return


def has_loop[T: Hashable](it: Iterator[T]) -> bool:
    visited = set()
    for e in it:
        if e in visited:
            return True
        visited.add(e)
    return False


if __name__ == "__main__":
    with read_lines(Path("inputs") / "d6.txt") as lines:
        grid = Grid.from_lines(lines)

    path = set(walk(grid))
    print("part1", len(path))

    for workers in range(1, 12):
        candidates = (node for node in path if node != grid.start)

        with timer(f"{workers = }: "):
            with ThreadPoolExecutor(max_workers=workers) as executor:
                results = executor.map(
                    lambda node: has_loop(pairwise(walk(grid, node))),
                    candidates,
                )
            print("part2", sum(1 for r in results if r), end="; ")
