import os as _os, json as _json, sys as _sys
_os.environ.setdefault('MINIO_ENDPOINT', 'http://localhost:9000')
_os.environ.setdefault('AWS_ACCESS_KEY_ID', 'admin')
_os.environ.setdefault('AWS_SECRET_ACCESS_KEY', 'admin123')

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs as _s3fs

_fs = _s3fs.S3FileSystem(
    key='admin', secret='admin123',
    endpoint_url='http://localhost:9000', use_ssl=False,
)

def read_parquet(path, **kw):
    return pq.read_table(path, filesystem=_fs, **kw).to_pandas()

class _DfEncoder(_json.JSONEncoder):
    def default(self, obj):
        import pandas as pd
        if pd.isna(obj): return None
        if hasattr(obj, 'isoformat'): return obj.isoformat()
        try:
            from decimal import Decimal
            if isinstance(obj, Decimal): return float(obj)
            import numpy as np
            if isinstance(obj, (np.integer, np.floating)): return obj.item()
        except: pass
        return str(obj)

def display(df, n=20):
    if isinstance(df, pd.DataFrame):
        _cols = list(df.columns)
        _df_head = df.head(n)
        _rows = _df_head.where(pd.notnull(_df_head), None).values.tolist()
        print("__TABLE_START__")
        print(_json.dumps({"columns": _cols, "rows": _rows}, cls=_DfEncoder))
        print("__TABLE_END__")
        if len(df) > n:
            print(f"\n[Showing {n} of {len(df)} rows x {len(df.columns)} columns]")
        else:
            print(f"\n[{len(df)} rows x {len(df.columns)} columns]")
    else:
        print(df)

df_bronze_orders = read_parquet("bronze/tpch/orders")
display(df_bronze_orders, n=10)
