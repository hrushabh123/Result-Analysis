import pandas as pd
from flask import Flask, request, jsonify, render_template
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Date,text
import os
import logging

# Initialize Flask application
app = Flask(__name__)

# Configure SQLAlchemy database connection
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root:4sf21ci030@localhost:3306/result'
db = SQLAlchemy(app)

# Define your SQLAlchemy models for different pages
class Student(db.Model):
    studentid = db.Column(db.String(20), primary_key=True)
    name = db.Column(db.String(100))
    dob = db.Column(db.String(10))
    gender = db.Column(db.String(6))
    email = db.Column(db.String(100))
    phno = db.Column(db.String(20))
    entrytype = db.Column(db.String(20))
    passyear = db.Column(db.String(4))
    admissiontype = db.Column(db.String(20))
    backlog = db.Column(db.Integer)  # Update the data type to Integer


class Department(db.Model):
    departmentid = db.Column(db.String(20), primary_key=True)
    departmentname = db.Column(db.String(100))
    hod = db.Column(db.String(20))  # Assuming hod (Head of Department) is stored as string
    phno = db.Column(db.String(20))
    facultyid = db.Column(db.String(20), db.ForeignKey('faculty.Facultyid'))  # Foreign key referencing Faculty

class Faculty(db.Model):
    Facultyid = db.Column(db.String(20), primary_key=True)
    Facultyname = db.Column(db.String(100))
    gender = db.Column(db.String(20))  # Date of Birth
    phno = db.Column(db.String(20))
    email = db.Column(db.String(100))
    
class Result(db.Model):
    studentid = db.Column(db.String(20), db.ForeignKey('student.studentid'), primary_key=True)  # Foreign key referencing Student
    courseid = db.Column(db.String(20), db.ForeignKey('course.courseid'), primary_key=True)  # Foreign key referencing Course
    marksobtained = db.Column(db.String(3))
    grade=db.Column(db.String(2))
    sem = db.Column(db.String(20))
    year= db.Column(db.String(20))


class Course(db.Model):
    courseid = db.Column(db.String(20), primary_key=True)
    coursename = db.Column(db.String(20))
    credit = db.Column(db.String(20))

# Create tables if they do not exist
with app.app_context():
    db.create_all()

# Route to render the main page
@app.route('/')
def hello():
    return render_template('index.html')

# Route to render the student page
@app.route('/Student')
def student_page():
    return render_template('Student.html')

@app.route('/update_backlog', methods=['POST'])
def update_backlog_route():
    # Call the function to update backlog
    update_backlog()
    return jsonify({'message': 'Backlog updated successfully'})


# Route to render the student page
# @app.route('/Student')
# def student_page():
#     # Query the database to retrieve student data
#     students = Student.query.all()
#     return render_template('Student.html', students=students)


# Route to render the department page
@app.route('/Department')
def department_page():
    return render_template('Department.html')

@app.route('/Faculty')
def Faculty_page():
    return render_template('Faculty.html')

@app.route('/Result')
def Result_page():
    return render_template('Result.html')

@app.route('/Course')
def Course_page():
    return render_template('Course.html')

# Route to handle CSV file upload for any page
@app.route('/upload/<page>', methods=['POST'])
def upload_csv(page):
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

    # Determine the data model based on the page
    if page == 'student':
        data_model = Student
    elif page == 'department':
        data_model = Department
    elif page == 'faculty':
        data_model = Faculty
    elif page == 'result':
        data_model = Result
    elif page == 'course':
        data_model = Course
    else:
        return jsonify({'error': 'Invalid page'}), 400

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
            # Create an instance of the corresponding data model with data from the current row
            record = data_model(**row)
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


def update_backlog():
    try:
        # Reset all backlogs to 0
        db.session.query(Student).update({Student.backlog: 0})
        db.session.commit()

        # Execute SQL update statement to increment backlog count for students with failing grades
        sql = """
        UPDATE student 
        SET backlog = backlog + 1
        WHERE studentid IN (SELECT studentid FROM result WHERE grade = 'F')
        """
        result = db.session.execute(text(sql))
        db.session.commit()
        
        affected_rows = result.rowcount
        logging.info(f"Backlog counts updated successfully for {affected_rows} students.")
        logging.info("Committing changes to the database.")
    except Exception as e:
        db.session.rollback()
        logging.error(f"Error updating backlog counts: {e}")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    app.run(debug=True)
