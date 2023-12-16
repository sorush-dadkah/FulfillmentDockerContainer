from concurrent.futures import ProcessPoolExecutor


def start_process_pool_executor(
    max_workers: int,
    func: Callable[[any], any],
    items: list,
    is_append: bool = False,
    chunk_size: int = 1,
) -> None | list[any]:
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        if is_append:
            return list(executor.map(func, items, chunksize=chunk_size))
        else:
            executor.map(func, items, chunksize=chunk_size)
