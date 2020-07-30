import trio, sys


async def child():
    print("entered child")
    try:
        await trio.sleep_forever()
    finally:
        print("leaving child")


async def starts_child_when_cancelled(nursery):
    try:
        await trio.sleep_forever()
    finally:
        # trio.Cancelled can't be made as a value manually, so check the type.
        assert sys.exc_info()[0] is trio.Cancelled
        nursery.start_soon(child)


async def main():
    async with trio.open_nursery() as nursery:
        nursery.start_soon(starts_child_when_cancelled, nursery)
        nursery.cancel_scope.cancel()


def test_start_soon_runs_to_checkpoint():
    trio.run(main)
