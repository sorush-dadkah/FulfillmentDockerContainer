def get_batches(data: list[any], n: int) -> any:
    for i in range(0, len(data), n):
        yield data[i: i + n]
