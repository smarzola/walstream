#!/usr/bin/env python3
"""Kafka v7 Produce response barrier for the native-producer walkthrough."""
import argparse
import asyncio
import hashlib
import json
import pathlib
import struct


def produce_identity(frame):
    """This probe accepts exactly the one-topic/partition/batch workload it sends."""
    pos = 8

    def number(fmt):
        nonlocal pos
        result = struct.unpack_from(fmt, frame, pos)[0]
        pos += struct.calcsize(fmt)
        return result

    def string():
        nonlocal pos
        size = number('>h')
        result = frame[pos:pos + max(size, 0)]
        pos += max(size, 0)
        return result.decode()

    string()  # client id
    string()  # nullable transactional id
    assert number('>h') == -1
    number('>i')  # timeout
    assert number('>i') == 1
    topic = string()
    assert number('>i') == 1
    assert number('>i') == 0
    size = number('>i')
    batch = frame[pos:pos + size]
    assert len(batch) == size and 12 + struct.unpack_from('>i', batch, 8)[0] == size
    return dict(topic=topic, producer_id=struct.unpack_from('>q', batch, 43)[0],
                epoch=struct.unpack_from('>h', batch, 51)[0],
                sequence=struct.unpack_from('>i', batch, 53)[0],
                count=struct.unpack_from('>i', batch, 57)[0],
                sha256=hashlib.sha256(batch).hexdigest())


async def frame(reader):
    size = struct.unpack('>i', await reader.readexactly(4))[0]
    assert 0 < size <= 16 * 1024 * 1024
    return await reader.readexactly(size)


async def main(args):
    directory = pathlib.Path(args.state)
    held = set()

    async def serve(reader, writer):
        upstream = None
        try:
            remote, upstream = await asyncio.open_connection('127.0.0.1', args.upstream)
            while True:
                request = await frame(reader)
                key, version = struct.unpack_from('>hh', request)
                identity = produce_identity(request) if key == 0 else None
                if identity:
                    assert version == 7
                    with (directory / 'requests.jsonl').open('a') as log:
                        log.write(json.dumps(identity) + '\n')
                upstream.write(struct.pack('>i', len(request)) + request)
                await upstream.drain()
                response = await frame(remote)
                if identity and identity['sequence'] == 0 and identity['topic'] not in held:
                    topic = identity['topic']
                    # Produce v7: correlation, topics count, topic string,
                    # partitions count, partition, error, base offset.
                    name_size = struct.unpack_from('>h', response, 8)[0]
                    position = 10 + name_size + 8
                    error, offset = struct.unpack_from('>hq', response, position)
                    assert error == 0 and offset == 0, (error, offset)
                    held.add(topic)
                    evidence = dict(identity, error=error, offset=offset, response_withheld=True)
                    (directory / (topic + '.committed')).write_text(json.dumps(evidence))
                    while not (directory / (topic + '.release')).exists():
                        await asyncio.sleep(0.05)
                    # Discard the successful response; producer must reconnect
                    # through this same listener and retry on the new broker.
                    return
                writer.write(struct.pack('>i', len(response)) + response)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            pass
        except Exception as error:
            (directory / 'proxy-error').write_text(repr(error))
            raise
        finally:
            if upstream:
                upstream.close()
            writer.close()

    server = await asyncio.start_server(serve, '0.0.0.0', args.listen)
    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--listen', type=int, required=True)
    parser.add_argument('--upstream', type=int, required=True)
    parser.add_argument('--state', required=True)
    asyncio.run(main(parser.parse_args()))
