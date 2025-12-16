import io
import os
from flask import Flask, request, send_file, jsonify
from dbfread import DBF
import pandas as pd

app = Flask(__name__)

def dbf_bytes_to_df(dbf_bytes: bytes) -> pd.DataFrame:
    # dbfread precisa de arquivo (path) ou file-like.
    # Vamos gravar em um arquivo temporário.
    tmp_path = "/tmp/input.dbf"
    with open(tmp_path, "wb") as f:
        f.write(dbf_bytes)

    # tenta cp1252 e cai para latin1
    try:
        tabela = DBF(tmp_path, load=True, encoding="cp1252", char_decode_errors="ignore")
    except Exception:
        tabela = DBF(tmp_path, load=True, encoding="latin1", char_decode_errors="ignore")

    df = pd.DataFrame(iter(tabela))
    df.columns = [str(c).strip() for c in df.columns]
    return df

@app.get("/healthz")
def healthz():
    return {"status": "ok"}

@app.post("/convert")
def convert():
    if "file" not in request.files:
        return jsonify({"error": "Send multipart/form-data with field 'file'"}), 400

    f = request.files["file"]
    dbf_bytes = f.read()

    if not dbf_bytes:
        return jsonify({"error": "Empty file"}), 400

    df = dbf_bytes_to_df(dbf_bytes)

    # gera CSV em memória
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode("utf-8-sig")

    # devolve como arquivo
    return send_file(
        io.BytesIO(csv_bytes),
        mimetype="text/csv",
        as_attachment=True,
        download_name="output.csv",
    )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
