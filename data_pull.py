import socket
import sys
import threading

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as pds


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


def write_output_s3(out_loc):
    # Read the output.
    s = socket.socket(socket.AF_VSOCK, socket.SOCK_STREAM)
    s.bind((3, 9993))
    s.listen(10)
    conn, _ = s.accept()
    fb = conn.makefile('rb')
    try:
        input_stream = pa.ipc.open_stream(fb)
        pds.write_dataset(input_stream, out_loc, format='parquet')
        input_stream.close()
    finally:
        fb.close()
        s.close()


def main():
    # Configuration.
    ds_path = sys.argv[1]
    out_path = sys.argv[2]

    # start output writer thread
    th = threading.Thread(None, target=write_output_s3, args=(out_path,))
    th.start()

    # Send the output.
    s = socket.socket(socket.AF_VSOCK, socket.SOCK_STREAM)
    s.connect((5, 9999))
    fb = s.makefile('wb')
    handler = LazySink(fb)
    try:
        ds = pq.ParquetDataset(ds_path, use_legacy_dataset=False)
        for fragment in ds.fragments:
            for batch in fragment.to_batches():
                writer = handler.get_output_writer(batch.schema)
                writer.write_batch(batch)
                fb.flush()
    finally:
        handler.close()
        fb.close()
        s.close()
    th.join(120)


if __name__ == '__main__':
    main()
