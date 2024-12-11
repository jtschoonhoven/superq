import superq


def test_init() -> None:
    cfg = superq.Config(backend_in_memory=True)
    superq.TaskQueue(cfg)
