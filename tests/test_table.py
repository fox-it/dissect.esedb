from unittest.mock import MagicMock

from dissect.esedb.table import Table


def test_find_index() -> None:
    mock_column_id = MagicMock()
    mock_column_id.name = "Id"
    mock_column_bit = MagicMock()
    mock_column_bit.name = "Bit"
    mock_column_unsigned_byte = MagicMock()
    mock_column_unsigned_byte.name = "UnsignedByte"

    mock_idx_id = MagicMock(name="IxId")
    mock_idx_id.is_primary = True
    mock_idx_id.columns = [mock_column_id]
    mock_idx_bit = MagicMock(name="IxBit")
    mock_idx_bit.is_primary = False
    mock_idx_bit.columns = [mock_column_bit]
    mock_idx_multiple = MagicMock(name="IxMultiple")
    mock_idx_multiple.is_primary = False
    mock_idx_multiple.columns = [mock_column_bit, mock_column_unsigned_byte]

    table = Table(MagicMock(), 69, "index", indexes=[mock_idx_id, mock_idx_bit, mock_idx_multiple])

    assert table.find_index(["Id"]) == mock_idx_id
    assert table.find_index(["Bit"]) == mock_idx_bit
    assert table.find_index(["Bit", "UnsignedByte"]) == mock_idx_multiple
    assert table.find_index(["UnsignedByte", "Bit"]) == mock_idx_multiple
    assert table.find_index(["UnsignedByte"]) is None
    assert table.find_index(["Id", "Bit"]) == mock_idx_id
    assert table.find_index(["Bit", "SomethingElse"]) == mock_idx_bit
