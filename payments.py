import pandas as pd
import numpy as np
import re
from pymongo import MongoClient
from fastapi import FastAPI, HTTPException, File, UploadFile
from fastapi.responses import FileResponse
from typing import List, Optional
from datetime import datetime
import os
from io import BytesIO
from bson import ObjectId
import uuid
from gridfs import GridFS

app = FastAPI()

# MongoDB URI and connection
uri = "mongodb+srv://SIdugboe:456R7xzk!@cluster0.mxdnt.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
mongo_client = MongoClient(uri, tlsAllowInvalidCertificates=True)
db = mongo_client["payment_database"]
payments_collection = db["payments"]
fs = GridFS(db)  # To store files

# Load and normalize the CSV data
def normalize_csv():
    # Read the CSV file
    file_path = "payment_information.csv"
    df = pd.read_csv(file_path)

    mandatory_fields = [
        "payee_address_line_1", "payee_city", "payee_country",
        "payee_postal_code", "payee_phone_number", "payee_email",
        "currency", "due_amount"
    ]
    
    # Check for missing values and fill missing fields with default values
    missing_fields = {}
    for field in mandatory_fields:
        missing_values = df[field].isnull().sum()
        if missing_values > 0:
            missing_fields[field] = missing_values

    df["payee_address_line_1"] = df["payee_address_line_1"].fillna("N/A")
    df["payee_city"] = df["payee_city"].fillna("Unknown")
    df["payee_country"] = df["payee_country"].fillna("Unknown")
    df["payee_postal_code"] = df["payee_postal_code"].fillna("Unknown")
    df["payee_phone_number"] = df["payee_phone_number"].fillna("N/A")
    df["payee_email"] = df["payee_email"].fillna("N/A")
    df["currency"] = df["currency"].fillna("USD")
    df["due_amount"] = df["due_amount"].fillna(0.0)

    if missing_fields:
        print(f"Missing values in mandatory fields: {missing_fields}")
    else:
        print("No missing values in mandatory fields.")


    # Validate the payee_country column
    invalid_country_code_count = df[~df["payee_country"].str.match(r"^[A-Z]{2}$")]["payee_country"].count()
    if invalid_country_code_count > 0:
        print(f"Found {invalid_country_code_count} invalid country codes in 'payee_country'. Replacing with 'Unknown'.")
        # Replace invalid values with 'Unknown'
        df["payee_country"] = df["payee_country"].apply(lambda x: x if re.match(r"^[A-Z]{2}$", x) else "Unknown")

    # Ensure valid phone number format, replace invalid phone numbers with 'N/A'
    df["payee_phone_number"] = df["payee_phone_number"].astype(str)  # Convert to string
    invalid_phoneNumber_count = df[~df["payee_phone_number"].str.match(r"^\+\d{1,15}$")]["payee_phone_number"].count()
    if invalid_phoneNumber_count > 0:
        print(f"Found {invalid_phoneNumber_count} invalid phone numbers in 'payee_phone_number'. Replacing with 'N/A'.")
        # Replace invalid values with 'N/A'
        df["payee_phone_number"] = df["payee_phone_number"].apply(lambda x: x if re.match(r"^\+\d{1,15}$", x) else "N/A")
    # df["payee_phone_number"] = df["payee_phone_number"].apply(
    #     lambda x: x if re.match(r"^\+\d{1,15}$", x) else "N/A"
    # )
 
    #the rest that dont have problems
    if not df["currency"].str.match(r"^[A-Z]{3}$").all():
        raise ValueError("Invalid currency code format in 'currency'")
    if not df["payee_email"].str.match(r"^[\w\.-]+@[\w\.-]+\.\w+$").all():
        raise ValueError("Invalid email format in 'payee_email'")

    # Standardize data types
    df["discount_percent"] = df["discount_percent"].astype(float).fillna(0.0)
    df["tax_percent"] = df["tax_percent"].astype(float).fillna(0.0)
    df["due_amount"] = df["due_amount"].astype(float)
    
    # Calculate total_due
    df["total_due"] = (
        df["due_amount"] - (df["due_amount"] * df["discount_percent"] / 100)
    ) + (df["due_amount"] * df["tax_percent"] / 100)

    # Insert data into MongoDB
    data_to_insert = df.to_dict(orient="records")
    try:
        payments_collection.insert_many(data_to_insert)
        print("Data successfully normalized and inserted into MongoDB collection.")
    except Exception as e:
        print(f"Error inserting data: {e}")

# File upload and download helpers
def save_file(file: UploadFile) -> str:
    file_id = str(uuid.uuid4())
    grid_out = fs.put(file.file, filename=file_id)
    return str(grid_out)

def get_file(file_id: str) -> BytesIO:
    grid_out = fs.get(ObjectId(file_id))
    return BytesIO(grid_out.read())

# Web services

@app.get("/get_payments")
async def get_payments(
    payment_status: Optional[str] = None,
    page: int = 1, 
    size: int = 10
):
    query = {}
    if payment_status:
        query["payee_payment_status"] = payment_status

    skip = (page - 1) * size
    payments = list(payments_collection.find(query).skip(skip).limit(size))
    
    # Adjust payment status based on due date
    today = datetime.utcnow().date()
    for payment in payments:
        due_date = datetime.strptime(payment['payee_due_date'], '%Y-%m-%d').date()
        if due_date < today and payment['payee_payment_status'] != 'completed':
            payment['payee_payment_status'] = 'overdue'
        elif due_date == today and payment['payee_payment_status'] != 'completed':
            payment['payee_payment_status'] = 'due_now'

        # Recalculate total_due based on discount, tax, and due_amount
        payment['total_due'] = (
            payment['due_amount'] - (payment['due_amount'] * payment['discount_percent'] / 100)
        ) + (payment['due_amount'] * payment['tax_percent'] / 100)

    return {"payments": payments}

@app.post("/update_payment/{payment_id}")
async def update_payment(payment_id: str, payment_data: dict):
    result = payments_collection.update_one(
        {"_id": ObjectId(payment_id)}, {"$set": payment_data}
    )
    if result.matched_count:
        return {"message": "Payment updated successfully"}
    raise HTTPException(status_code=404, detail="Payment not found")

@app.delete("/delete_payment/{payment_id}")
async def delete_payment(payment_id: str):
    result = payments_collection.delete_one({"_id": ObjectId(payment_id)})
    if result.deleted_count:
        return {"message": "Payment deleted successfully"}
    raise HTTPException(status_code=404, detail="Payment not found")

@app.post("/create_payment")
async def create_payment(payment_data: dict):
    try:
        payment_id = payments_collection.insert_one(payment_data).inserted_id
        return {"payment_id": str(payment_id)}
    except Exception as e:
        raise HTTPException(status_code=400, detail="Error creating payment")

@app.post("/upload_evidence/{payment_id}")
async def upload_evidence(payment_id: str, file: UploadFile = File(...)):
    payment = payments_collection.find_one({"_id": ObjectId(payment_id)})
    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")
    
    if payment['payee_payment_status'] != "completed":
        raise HTTPException(status_code=400, detail="Payment must be marked as completed")

    file_id = save_file(file)
    payments_collection.update_one(
        {"_id": ObjectId(payment_id)}, {"$set": {"evidence_file_id": file_id}}
    )
    return {"file_id": file_id}

@app.get("/download_evidence/{file_id}")
async def download_evidence(file_id: str):
    file = get_file(file_id)
    return FileResponse(file, media_type="application/octet-stream", headers={"Content-Disposition": "attachment; filename= evidence.pdf"})

# Initialize the app by normalizing and inserting CSV data
normalize_csv()
