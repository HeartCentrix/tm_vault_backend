"""Owned seekable multipart bodies; part size does not determine heap usage."""
import asyncio
import os

READ_SIZE = 1024 * 1024


class FileBody:
    def __init__(self, source, start, length):
        self.closed, self._closing, self._reads = False, False, set()
        self.source, self.start, self.length = source, start, length
        self.position, self._epoch, self._pending_seek = 0, 0, None
        source.seek(start)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self._closing = True
        try:
            await finish_io(asyncio.gather(*self._reads, return_exceptions=True))
        finally:
            self.closed = True

    def close(self):
        # The SDK borrows this body for each HTTP attempt. Its transport may
        # close an attempt before retrying; only the owning async scope closes it.
        pass

    def __aiter__(self):
        return self._iterate(self._epoch)

    async def _iterate(self, epoch):
        while not self.closed and not self._closing and epoch == self._epoch:
            if self._reads:
                await finish_io(asyncio.gather(*self._reads, return_exceptions=True))
            if epoch != self._epoch:
                return
            self._apply_seek()
            task = asyncio.create_task(asyncio.to_thread(self.read, READ_SIZE))
            self._reads.add(task)
            def completed(task):
                self._reads.discard(task)
                if not task.cancelled():
                    task.exception()
            task.add_done_callback(completed)
            data = await asyncio.shield(task)
            if not data or epoch != self._epoch:
                return
            yield data

    def __len__(self):
        return self.length

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self._pending_seek if self._pending_seek is not None else self.position

    def seek(self, offset, whence=os.SEEK_SET):
        target = offset + (0 if whence == os.SEEK_SET else self.tell() if whence == os.SEEK_CUR else self.length)
        if whence not in (os.SEEK_SET, os.SEEK_CUR, os.SEEK_END) or not 0 <= target <= self.length:
            raise ValueError('file part seek outside its range')
        self._epoch += 1
        self._pending_seek = target
        if not self._reads:
            self._apply_seek()
        return target

    def _apply_seek(self):
        if self._pending_seek is not None:
            self.source.seek(self.start + self._pending_seek)
            self.position, self._pending_seek = self._pending_seek, None

    async def prepare_to_sign(self):
        # A retry can start signing before aiohttp's previous body read exits.
        # Join that borrowed read before synchronous SigV4 hashing uses the cursor.
        await finish_io(asyncio.gather(*self._reads, return_exceptions=True))
        self._apply_seek()

    def read(self, size=-1):
        if self.closed:
            raise ValueError('file part is closed')
        amount = min(self.length-self.position, READ_SIZE, size if size is not None and size >= 0 else READ_SIZE)
        result = self.source.read(amount)
        if len(result) != amount:
            raise OSError('upload source ended before its declared size')
        self.position += len(result)
        return result


async def finish_io(awaitable):
    """Join owned I/O even if cancellation is repeated during cleanup."""
    task, cancelled = asyncio.ensure_future(awaitable), False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
        except BaseException:
            break
    if cancelled:
        if not task.cancelled():
            task.exception()
        raise asyncio.CancelledError
    return task.result()


def part_size_for(size, *, max_parts, minimum, maximum):
    part = max(minimum, (size + max_parts - 1) // max_parts)
    if size < 0 or part > maximum:
        raise ValueError('object exceeds provider multipart limits')
    return part


def file_upload_capacity(requested, *, extra_bytes=0):
    from shared.provider_runtime import execution_budget, _PREPAID_STORAGE
    budget = execution_budget()
    prepaid = _PREPAID_STORAGE.get()
    capacity = prepaid[2] if prepaid and prepaid[:2] == (budget, asyncio.current_task()) else budget.resident_bytes
    # SDK checksum/read copies are <=1MiB; allow three per active body plus
    # fixed producer/transport room. This accounts payload, not Python RSS.
    concurrency = min(max(1, int(requested)), (capacity-extra_bytes)//(3*READ_SIZE)-2)
    if concurrency < 1:
        raise ValueError('configured storage byte capacity is too small')
    return concurrency, (concurrency+2)*3*READ_SIZE+extra_bytes


def file_identity(path):
    stat = os.stat(path)
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns


async def send_file_parts(path, size, *, part_size, concurrency, send):
    identity = file_identity(path)
    if identity[2] != size:
        raise OSError('upload source size differs from its declared size')
    async def part(number, offset, length):
        with open(path, 'rb') as source:
            async with FileBody(source, offset, length) as body:
                result = await send(number, body, length)
                if body.tell() != length:
                    raise OSError('upload did not consume its declared part')
                return number, result
    pending, results = set(), []
    count = max(1, (size+part_size-1)//part_size)
    next_part = 1
    try:
        while next_part <= count or pending:
            while next_part <= count and len(pending) < concurrency:
                offset = (next_part-1)*part_size
                pending.add(asyncio.create_task(part(next_part, offset, min(part_size, size-offset))))
                next_part += 1
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            # Join every done task as well if one result failed.
            outcomes = await asyncio.gather(*done, return_exceptions=True)
            for outcome in outcomes:
                if isinstance(outcome, BaseException):
                    raise outcome
                results.append(outcome)
        if file_identity(path) != identity:
            raise OSError('upload source changed during transfer')
        return [result for _, result in sorted(results)]
    finally:
        for task in pending:
            task.cancel()
        await finish_io(asyncio.gather(*pending, return_exceptions=True))


async def send_spooled_parts(source, *, part_size, concurrency, send):
    """Keep encrypted parts on disk, including parts larger than the byte budget."""
    import shutil
    import tempfile
    pending, results, files = set(), [], {}
    current, written, number, total = None, 0, 0, 0
    async def send_part(file, number, length):
        try:
            file.seek(0)
            async with FileBody(file, 0, length) as body:
                result = await send(number, body, length)
                if body.tell() != length:
                    raise OSError('upload did not consume its declared part')
                return number, result
        finally:
            file.close()
    async def collect():
        nonlocal pending
        done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        outcomes = await asyncio.gather(*done, return_exceptions=True)
        for task in done:
            files.pop(task).close()
        for outcome in outcomes:
            if isinstance(outcome, BaseException):
                raise outcome
            results.append(outcome)
    async def submit():
        nonlocal current, written, number
        while len(pending) >= concurrency:
            await collect()
        number += 1
        if number > 10000:
            raise ValueError('encrypted object exceeds provider multipart limits')
        task = asyncio.create_task(send_part(current, number, written))
        pending.add(task)
        files[task] = current
        current, written = None, 0
    try:
        async for chunk in source:
            view = memoryview(chunk)
            offset = 0
            while offset < len(view):
                if current is None:
                    current = tempfile.TemporaryFile(prefix='tmvault-encrypted-part-')
                amount = min(len(view)-offset, part_size-written)
                if shutil.disk_usage(tempfile.gettempdir()).free < amount+256*1024*1024:
                    raise OSError('insufficient temporary space for encrypted upload part')
                current.write(view[offset:offset+amount])
                written += amount
                total += amount
                offset += amount
                if written == part_size:
                    await submit()
        if current is not None:
            await submit()
        while pending:
            await collect()
        return [result for _, result in sorted(results)], total
    finally:
        if current is not None:
            current.close()
        for task in pending:
            task.cancel()
        try:
            await finish_io(asyncio.gather(*pending, return_exceptions=True))
        finally:
            for file in files.values():
                file.close()
            close = getattr(source, 'aclose', None)
            if close is not None:
                await finish_io(close())
