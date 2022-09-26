import socket
import sys

import pyarrow as pa


class LazySink:
    def __init__(self, sink):
        self.sink = sink
        self.writer = None

    def get_output_writer(self, schema) -> pa.ipc.RecordBatchStreamWriter:
        if self.writer is not None:
            return self.writer

        self.writer = pa.ipc.new_stream(self.sink, schema)
        return self.writer

    def close(self):
        if self.writer is not None:
            try:
                self.writer.close()
            except Exception as e:
                print(e)
                pass


def process(batch):
    return batch


def main():
    # Read in data, process it. write output to in-memory sink.
    with socket.socket(socket.AF_VSOCK, socket.SOCK_STREAM) as s:
        s.bind((5, 9999))
        s.listen(10)
        conn, addr = s.accept()
        output_socket = socket.socket(socket.AF_VSOCK, socket.SOCK_STREAM)
        output_socket.connect((3, 9993))
        out_fb = output_socket.makefile('wb')
        sink = LazySink(out_fb)
        with conn.makefile('rb') as fb:
            stream = pa.ipc.open_stream(fb)
            for batch in stream:
                batch = process(batch)
                sink.get_output_writer(batch.schema).write_batch(batch)
        out_fb.flush()
        out_fb.close()
        output_socket.close()


if __name__ == '__main__':
    main()
