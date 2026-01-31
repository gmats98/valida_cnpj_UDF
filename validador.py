import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType


# Função auxiliar: converte caractere alfanumérico em número
def char_to_value(c: str) -> int:
    if c.isdigit():
        return int(c)
    else:
        return ord(c.upper()) - ord('A') + 10  # A=10, B=11, ..., Z=35

# Função principal de validação
def validar_cnpj_alfa(cnpj: str) -> bool:
    if cnpj is None:
        return False

    cnpj = cnpj.strip().upper()

    # Regex de formato: 8 alfanuméricos + 4 alfanuméricos + 2 numéricos
    pattern = r"^[A-Z0-9]{8}[A-Z0-9]{4}[0-9]{2}$"
    if not re.match(pattern, cnpj):
        return False

    # Extrai base+ordem e dígito verificador
    corpo = cnpj[:12]
    dv_informado = int(cnpj[12:])

    # Converte cada caractere em valor numérico
    valores = [char_to_value(c) for c in corpo]

    # Pesos para cálculo (mesma lógica do CNPJ tradicional, adaptada)
    pesos = [5,4,3,2,9,8,7,6,5,4,3,2]

    soma = sum(v * p for v, p in zip(valores, pesos))
    resto = soma % 11
    dv_calculado = 0 if resto < 2 else 11 - resto

    return dv_informado == dv_calculado

# ---------------------------
# Criando SparkSession
# ---------------------------
spark = SparkSession.builder.appName("ValidadorCNPJAlfa").getOrCreate()

# Registrando UDF
validar_cnpj_alfa_udf = udf(validar_cnpj_alfa, BooleanType())
spark.udf.register("validarCNPJAlfa", validar_cnpj_alfa_udf)

# ---------------------------
# Exemplo de DataFrame
# ---------------------------
data = [("AB123456CD78",), ("AB123456CD99",), ("1234567890123",)]
df = spark.createDataFrame(data, ["cnpj"])

# Usando UDF no DataFrame
df_result = df.withColumn("valido", validar_cnpj_alfa_udf(df["cnpj"]))
df_result.show()

# Usando direto em SQL
df.createOrReplaceTempView("empresas")
spark.sql("SELECT cnpj, validarCNPJAlfa(cnpj) AS valido FROM empresas").show()


