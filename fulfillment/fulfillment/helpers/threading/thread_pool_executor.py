from concurrent.futures import ThreadPoolExecutor, wait

logger = get_logger(log_level="warning")


def start_thread_pool_executor(
    max_workers: int, func: Callable[[any], any], items: list, is_append: bool = False
) -> None | list[any]:
    with ThreadPoolExecutor(
        max_workers=max_workers,
    ) as executor:
        futures = []
        for item in items:
            if is_append:
                futures.append(executor.submit(func, item))
            else:
                executor.submit(func, item)

    if futures:
        wait(futures)

        final_data = []
        for future in futures:
            if future.exception():
                logger.info(future.result())
            elif future.result():
                final_data.append(future.result())

        return final_data
