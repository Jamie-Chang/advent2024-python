from __future__ import annotations

from dataclasses import dataclass
from itertools import pairwise
from typing import Hashable, Iterator, Self, assert_never


type Pair = tuple[int, int]
type Grid = tuple[tuple[bool, ...], ...]


up = 0
right = 1
down = 2
left = 3


def turn(direction: int) -> int:
    return (direction + 1) % 4


@dataclass(slots=True)
class Ranges:
    """Traversable location in the map."""

    rows: range
    cols: range

    @classmethod
    def from_grid(cls, grid: Grid) -> Self:
        return cls(range(len(grid)), range(len(grid[0])))

    def __getitem__(self, key: tuple[slice, int] | tuple[int, slice]) -> Iterator[Pair]:
        match key:
            case int() as r, slice() as cols:
                return ((r, c) for c in self.cols[cols])
            case slice() as rows, int() as c:
                return ((r, c) for r in self.rows[rows])
            case _ as other:
                assert_never(other)

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



def walk(grid: Grid, start: Pair, obstruction: Pair | None = None) -> Iterator[Pair]:
    direction = up
    ranges = Ranges.from_grid(grid)

    yield start

    while True:
        walk = pairwise(ranges.walk(start, direction))

        for prev, (r, c) in walk:
            if grid[r][c] or (r, c) == obstruction:
                direction = turn(direction)
                start = prev
                break

            yield (r, c)
        else:
            return


def loops[T: Hashable](it: Iterator[T]) -> bool:
    visited = set()
    for e in it:
        if e in visited:
            return True
        visited.add(e)
    return False


def solve(start: Pair, obstruction: Pair, *, grid: Grid) -> bool:
    return loops(pairwise(walk(grid, start, obstruction)))
