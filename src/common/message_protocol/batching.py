def unbatch_messages(batch: str):
    start = 0
    while True:
        end = batch.find("\n", start)
        if end == -1:
            yield batch[start:]
            return

        yield batch[start:end]
        start = end + 1