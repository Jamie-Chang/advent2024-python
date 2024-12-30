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
    dimensions: Pair

    def __getitem__(self, key: tuple[slice, int] | tuple[int, slice]) -> Iterator[Pair]:
        rs = range(self.dimensions[0])
        cs = range(self.dimensions[1])
        match key:
            case int() as r, slice() as cols:
                return ((r, c) for c in cs[cols])
            case slice() as rows, int() as c:
                return ((r, c) for r in rs[rows])
            case _ as other:
                assert_never(other)

    def __contains__(self, key: Pair) -> bool:
        row, col = key
        rs = range(self.dimensions[0])
        cs = range(self.dimensions[1])

        return row in rs and col in cs

    def __iter__(self) -> Iterator[Pair]:
        rs = range(self.dimensions[0])
        cs = range(self.dimensions[1])

        return ((row, col) for row in rs for col in cs)

    def walk(self, start: Pair, direction: int) -> Iterator[Pair]:
        rs = range(self.dimensions[0])
        cs = range(self.dimensions[1])

        match direction:
            case 0:
                return ((row, start[1]) for row in rs[start[0] :: -1])
            case 2:
                return ((row, start[1]) for row in rs[start[0] :: 1])
            case 3:
                return ((start[0], col) for col in cs[start[1] :: -1])
            case 1:
                return ((start[0], col) for col in cs[start[1] :: 1])
            case _:
                assert False


@dataclass(slots=True)
class Grid:
    tiles: tuple[tuple[Tile, ...], ...]

    def get_ranges(self) -> Ranges:
        return Ranges((len(self.tiles), len(self.tiles[0])))

    @classmethod
    def from_lines(cls, lines: Iterable[str]) -> Self:
        return cls(tuple(tuple(Tile(c) for c in line) for line in lines))

    def __getitem__(self, key: Pair) -> Tile:
        return self.tiles[key[0]][key[1]]


def get_start(m: Grid) -> Pair:
    return next(key for key in m.get_ranges() if m[key] is Tile.start)


@contextmanager
def read_lines(path: Path) -> Iterator[Iterator[str]]:
    with path.open() as f:
        yield (line.rstrip() for line in f)


def walk(grid: Grid, start: Pair, obstruction: Pair | None = None) -> Iterator[Pair]:
    direction = up

    ranges = grid.get_ranges()
    yield start

    while True:
        walk = pairwise(ranges.walk(start, direction))

        for prev, curr in walk:
            if grid[curr] is Tile.obstruction or curr == obstruction:
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

    start = get_start(grid)
    path = set(walk(grid, start))
    print("part1", len(path))

    for workers in range(1, 12):
        candidates = (node for node in path if node != start)

        with timer(f"{workers = }: "):
            with ThreadPoolExecutor(max_workers=workers) as executor:
                results = executor.map(
                    lambda node: has_loop(pairwise(walk(grid, start, node))),
                    candidates,
                )
            print("part2", sum(1 for r in results if r), end="; ")
