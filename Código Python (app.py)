from flask import Flask, request, send_file, abort
import dbf
import csv
import os
import tempfile

app = Flask(__name__)

@app.route("/convert", methods=["POST"])
def convert_dbf():
    if "file" not in request.files:
        return abort(400, "File not found in request")

    dbf_file = request.files["file"]

    with tempfile.TemporaryDirectory() as tmp:
        dbf_path = os.path.join(tmp, dbf_file.filename)
        csv_path = dbf_path.replace(".dbf", ".csv")

        dbf_file.save(dbf_path)

        table = dbf.Table(dbf_path)
        table.open()

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(table.field_names)

            for record in table:
                writer.writerow(list(record))

        table.close()

        return send_file(
            csv_path,
            as_attachment=True,
            download_name=os.path.basename(csv_path),
            mimetype="text/csv"
        )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
