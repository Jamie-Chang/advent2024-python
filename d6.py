from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
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
    tiles: tuple[tuple[bool, ...], ...]

    def get_ranges(self) -> Ranges:
        return Ranges(range(len(self.tiles)), range(len(self.tiles[0])))

    @classmethod
    def from_lines(cls, lines: Iterable[str]) -> Self:
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
        return cls(start, tuple(rows))

    def __getitem__(self, key: Pair) -> bool:
        return self.tiles[key[0]][key[1]]


@contextmanager
def read_lines(path: Path) -> Iterator[Iterator[str]]:
    with path.open() as f:
        yield (line.rstrip() for line in f)


def walk(grid: Grid, obstruction: Pair | None = None) -> Iterator[Pair]:
    direction = up
    ranges = grid.get_ranges()
    start = grid.start

    yield start

    while True:
        walk = pairwise(ranges.walk(start, direction))

        for prev, curr in walk:
            if grid[curr] or curr == obstruction:
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

    with timer("time:"):
        candidates = (node for node in path if node != grid.start)
        print(
            "part2",
            sum(1 for node in candidates if has_loop(pairwise(walk(grid, node)))),
            end="; "
        )
