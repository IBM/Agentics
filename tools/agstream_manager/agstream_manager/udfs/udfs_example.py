from pyflink.table import DataTypes
from pyflink.table.udf import udf


@udf(result_type=DataTypes.STRING())
def add_prefix(text):
    return f"Q: {text}" if text else None


@udf(result_type=DataTypes.STRING())
def uppercase_text(text):
    return text.upper() if text else None
