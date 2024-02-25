import pandas as pd
from flask import Flask, request, jsonify, render_template
from flask_sqlalchemy import SQLAlchemy
import logging
import os

# Initialize Flask application
app = Flask(__name__)

# Configure SQLAlchemy database connection
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root:4sf21ci030@localhost:3306/result'
db = SQLAlchemy(app)

# Enable SQL statement logging
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

# Define your SQLAlchemy model
class Student(db.Model):
    studentid = db.Column(db.String(20), primary_key=True)
    name = db.Column(db.String(100))
    dob = db.Column(db.String(20))  # Date of Birth
    gender = db.Column(db.String(6))
    email = db.Column(db.String(100))
    phno = db.Column(db.String(20))  # Assuming phone numbers are stored as strings

# Create tables if they do not exist
with app.app_context():
    db.create_all()

# Route to render the main page
@app.route('/')
def hello():
    return render_template('index.html')

# Route to handle CSV file upload
@app.route('/upload', methods=['POST'])
def upload_csv():
    # Check if a file is provided
    if 'csvFile' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['csvFile']

    # Check if the file is empty
    if file.filename == '':
        return jsonify({'error': 'Empty file provided'}), 400

    # Check if the file is a CSV file
    if not file.filename.endswith('.csv'):
        return jsonify({'error': 'Invalid file format. Only CSV files are allowed'}), 400

    # Save the file to a temporary directory
    temp_file_path = os.path.join(app.root_path, 'temp.csv')
    file.save(temp_file_path)

    # Read CSV file into a DataFrame
    df = pd.read_csv(temp_file_path)

    # Remove leading/trailing spaces from column names
    df.columns = df.columns.str.strip()

    # Insert DataFrame records into MySQL database
    try:
        # Loop through each row in the DataFrame
        for index, row in df.iterrows():
            # Create an instance of Student with data from the current row
            record = Student(**row)
            # Add the record to the session
            db.session.add(record)
        
        # Commit changes to the database
        db.session.commit()
        return jsonify({'message': 'Data inserted successfully', 'file_path': temp_file_path})
    except Exception as e:
        # Rollback changes if an error occurs
        db.session.rollback()
        return jsonify({'error': f'Error inserting data: {e}'}), 500
    finally:
        # Close the database session
        db.session.close()

if __name__ == '__main__':
    app.run(debug=True)
