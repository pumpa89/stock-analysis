from batch_pipeline.database import Database

def get_last_date(issuer_code):
    db = Database()
    last_date = db.get_last_date(issuer_code)
    db.close()
    return last_date  # "2023-10-01" or None