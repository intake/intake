import tempfile

import intake


def test_reader_from_call():
    import pandas as pd

    df = pd.DataFrame(
        {
            "col1": ["a", "b"],
            "col2": [1.0, 3.0],
        },
        columns=["col1", "col2"],
    )
    with tempfile.NamedTemporaryFile(delete=False) as fp:
        df.to_csv(fp.name)
        fp.close()
        reader = intake.reader_from_call("df = pd.read_csv(fp.name)")
        read_df = reader.read()
        assert all(read_df.col1 == df.col1)
        assert all(read_df.col2 == df.col2)
