import pandas as pd
from flask import Flask, request, jsonify, render_template
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Date,text
import os
import logging

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root:4sf21ci030@localhost:3306/result'
db = SQLAlchemy(app)

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
    backlog = db.Column(db.Integer) 


class Department(db.Model):
    departmentid = db.Column(db.String(20), primary_key=True)
    departmentname = db.Column(db.String(100))
    hod = db.Column(db.String(20))  
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
    supplementary=db.Column(db.String(5))


class Course(db.Model):
    courseid= db.Column(db.String(20), primary_key=True)
    coursename= db.Column(db.String(300))
    credit= db.Column(db.String(20))

# Create tables if they do not exist
with app.app_context():
    db.create_all()
    YEAR1_VIEW_QUERY = """
     CREATE OR REPLACE VIEW YEAR1 AS
        SELECT
            S.PASSYEAR,
            COUNT(DISTINCT S.STUDENTID) AS TotalStudents,
            COUNT(DISTINCT CASE WHEN R.YEAR = '1' AND NOT EXISTS (SELECT 1 FROM RESULT R2 WHERE R2.STUDENTID = R.STUDENTID AND R2.YEAR <= '1' AND (R2.GRADE = 'F' OR R2.SUPPLEMENTARY = 'YES')) THEN S.STUDENTID ELSE NULL END) AS year1,
            COUNT(DISTINCT CASE WHEN R.YEAR = '2' AND NOT EXISTS (SELECT 1 FROM RESULT R2 WHERE R2.STUDENTID = R.STUDENTID AND R2.YEAR <= '2' AND (R2.GRADE = 'F' OR R2.SUPPLEMENTARY = 'YES')) THEN S.STUDENTID ELSE NULL END) AS year2,
            COUNT(DISTINCT CASE WHEN R.YEAR = '3' AND NOT EXISTS (SELECT 1 FROM RESULT R2 WHERE R2.STUDENTID = R.STUDENTID AND R2.YEAR <= '3' AND (R2.GRADE = 'F' OR R2.SUPPLEMENTARY = 'YES')) THEN S.STUDENTID ELSE NULL END) AS year3,
            COUNT(DISTINCT CASE WHEN R.YEAR = '4' AND NOT EXISTS (SELECT 1 FROM RESULT R2 WHERE R2.STUDENTID = R.STUDENTID AND R2.YEAR <= '4' AND (R2.GRADE = 'F' OR R2.SUPPLEMENTARY = 'YES')) THEN S.STUDENTID ELSE NULL END) AS year4
        FROM
            STUDENT S
        JOIN
            RESULT R ON S.STUDENTID = R.STUDENTID
        GROUP BY
            S.PASSYEAR;
    """
    db.session.execute(text(YEAR1_VIEW_QUERY))
    db.session.commit()
    PASSED_ALL_SUBJECTS_VIEW_QUERY = """
        # CREATE OR REPLACE VIEW PASSED_ALL_SUBJECTS AS
        # SELECT
        #     S.PASSYEAR,
        #     (SELECT COUNT(DISTINCT STUDENTID) FROM RESULT) AS TotalStudents,
        #     COUNT(DISTINCT CASE WHEN R.YEAR = '1' THEN S.STUDENTID ELSE NULL END) AS year1,
        #     COUNT(DISTINCT CASE WHEN R.YEAR = '2' THEN S.STUDENTID ELSE NULL END) AS year2,
        #     COUNT(DISTINCT CASE WHEN R.YEAR = '3' THEN S.STUDENTID ELSE NULL END) AS year3,
        #     COUNT(DISTINCT CASE WHEN R.YEAR = '4' THEN S.STUDENTID ELSE NULL END) AS year4
        # FROM
        #     STUDENT S
        # JOIN
        #     RESULT R ON S.STUDENTID = R.STUDENTID
        # WHERE
        #     NOT EXISTS (SELECT 1 FROM RESULT R2 WHERE R2.STUDENTID = R.STUDENTID AND R2.GRADE = 'F')
        # GROUP BY
        #     S.PASSYEAR;
            CREATE OR REPLACE VIEW PASSED_ALL_SUBJECTS AS
    SELECT
        S.PASSYEAR,
        COUNT(DISTINCT S.STUDENTID) AS TotalStudents,
        COUNT(DISTINCT CASE 
            WHEN R.YEAR = '1' AND 'F' NOT IN (SELECT R2.GRADE FROM RESULT R2 WHERE R2.STUDENTID = R.STUDENTID AND R2.YEAR = '1') THEN S.STUDENTID 
            WHEN R.YEAR = '2' AND 'F' NOT IN (SELECT R2.GRADE FROM RESULT R2 WHERE R2.STUDENTID = R.STUDENTID AND R2.YEAR <= '2') THEN S.STUDENTID 
            WHEN R.YEAR = '3' AND 'F' NOT IN (SELECT R2.GRADE FROM RESULT R2 WHERE R2.STUDENTID = R.STUDENTID AND R2.YEAR <= '3') THEN S.STUDENTID 
            WHEN R.YEAR = '4' AND 'F' NOT IN (SELECT R2.GRADE FROM RESULT R2 WHERE R2.STUDENTID = R.STUDENTID AND R2.YEAR <= '4') THEN S.STUDENTID 
            ELSE NULL 
        END) AS year1,
        COUNT(DISTINCT CASE 
            WHEN R.YEAR = '2' AND 'F' NOT IN (SELECT R2.GRADE FROM RESULT R2 WHERE R2.STUDENTID = R.STUDENTID AND R2.YEAR <= '2') THEN S.STUDENTID 
            WHEN R.YEAR = '3' AND 'F' NOT IN (SELECT R2.GRADE FROM RESULT R2 WHERE R2.STUDENTID = R.STUDENTID AND R2.YEAR <= '3') THEN S.STUDENTID 
            WHEN R.YEAR = '4' AND 'F' NOT IN (SELECT R2.GRADE FROM RESULT R2 WHERE R2.STUDENTID = R.STUDENTID AND R2.YEAR <= '4') THEN S.STUDENTID 
            ELSE NULL 
        END) AS year2,
        COUNT(DISTINCT CASE 
            WHEN R.YEAR = '3' AND 'F' NOT IN (SELECT R2.GRADE FROM RESULT R2 WHERE R2.STUDENTID = R.STUDENTID AND R2.YEAR <= '3') THEN S.STUDENTID 
            WHEN R.YEAR = '4' AND 'F' NOT IN (SELECT R2.GRADE FROM RESULT R2 WHERE R2.STUDENTID = R.STUDENTID AND R2.YEAR <= '4') THEN S.STUDENTID 
            ELSE NULL 
        END) AS year3,
        COUNT(DISTINCT CASE 
            WHEN R.YEAR = '4' AND 'F' NOT IN (SELECT R2.GRADE FROM RESULT R2 WHERE R2.STUDENTID = R.STUDENTID AND R2.YEAR <= '4') THEN S.STUDENTID 
            ELSE NULL 
        END) AS year4
    FROM
        STUDENT S
    JOIN
        RESULT R ON S.STUDENTID = R.STUDENTID
    GROUP BY
        S.PASSYEAR;



    """
    db.session.execute(text(PASSED_ALL_SUBJECTS_VIEW_QUERY))
    db.session.commit()


@app.route('/')
def hello():
    # return render_template('index.html')
    year1_data = db.session.execute(text("SELECT * FROM YEAR1")).fetchall()
    passed_all_subjects_data = db.session.execute(text("SELECT * FROM PASSED_ALL_SUBJECTS")).fetchall()
    return render_template('index.html', year1_data=year1_data, passed_all_subjects_data=passed_all_subjects_data)


@app.route('/Student')
def student_page():
    return render_template('Student.html')

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

@app.route('/reeval')
def reevaluation_page():
    return render_template('reeval.html')

@app.route('/supplementary')
def supplementary_page():
    return render_template('supplementary.html')

@app.route('/resultupdate')
def resultupdate_page():
    return render_template('resultupdate.html')

@app.route('/upload/<page>', methods=['POST'])
def upload_csv(page):
    if 'csvFile' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['csvFile']

    if file.filename == '':
        return jsonify({'error': 'Empty file provided'}), 400

    if not file.filename.endswith('.csv'):
        return jsonify({'error': 'Invalid file format. Only CSV files are allowed'}), 400

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

    df = pd.read_csv(temp_file_path, skipinitialspace=False)


    df.columns = df.columns.str.strip() #ignore the spaces in the columns

    try:
        for index, row in df.iterrows():
            record = data_model(**row)
            db.session.add(record)
        
        db.session.commit()

        if page == 'result':
            update_backlog()

        return jsonify({'message': 'Data inserted successfully', 'file_path': temp_file_path})
    except Exception as e:
        db.session.rollback()
        logging.error(f"Error uploading course CSV: {e}")

        return jsonify({'error': f'Error inserting data: {e}'}), 500
    finally:
        db.session.close()


def update_backlog():
    try:
        db.session.query(Student).update({Student.backlog: 0})
        db.session.commit()

        sql = """
        UPDATE student
        SET backlog = (
            SELECT COUNT(*)
            FROM result
            WHERE result.studentid = student.studentid
            AND (result.grade = 'F' OR result.supplementary = 'YES')
        );
        """
        result = db.session.execute(text(sql))
        db.session.commit()
        
        affected_rows = result.rowcount
        logging.info(f"Backlog counts updated successfully for {affected_rows} students.")
        logging.info("Committing changes to the database.")
    except Exception as e:
        db.session.rollback()
        logging.error(f"Error updating backlog counts: {e}")

@app.route('/update/resultreeval', methods=['POST'])
def update_result_csv():
    if 'csvFile' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['csvFile']

    if file.filename == '':
        return jsonify({'error': 'Empty file provided'}), 400

    if not file.filename.endswith('.csv'):
        return jsonify({'error': 'Invalid file format. Only CSV files are allowed'}), 400

    try:
        temp_file_path = os.path.join(app.root_path, 'temp.csv')
        file.save(temp_file_path)

        df = pd.read_csv(temp_file_path)

        df.columns = df.columns.str.strip()

        for index, row in df.iterrows():
            result_record = Result.query.filter_by(studentid=row['studentid'], courseid=row['courseid']).first()

            if result_record:
                result_record.marksobtained = row['marksobtained']
                result_record.grade = row['grade']
                result_record.sem = row['sem']
                result_record.year = row['year']

        db.session.commit()
        update_backlog()

        return jsonify({'message': 'Data updated successfully', 'file_path': temp_file_path})
    except Exception as e:
        db.session.rollback()
        logging.error(f"Error updating result CSV: {e}")
        return jsonify({'error': f'Error updating data: {e}'}), 500
    finally:
        db.session.close()

@app.route('/update/supplementary', methods=['POST'])
def update_supplementary_csv():
    if 'csvFile' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['csvFile']

    if file.filename == '':
        return jsonify({'error': 'Empty file provided'}), 400

    if not file.filename.endswith('.csv'):
        return jsonify({'error': 'Invalid file format. Only CSV files are allowed'}), 400

    try:
        temp_file_path = os.path.join(app.root_path, 'temp.csv')
        file.save(temp_file_path)

        df = pd.read_csv(temp_file_path)

        df.columns = df.columns.str.strip()

        for index, row in df.iterrows():
            result_record = Result.query.filter_by(studentid=row['studentid'], courseid=row['courseid']).first()

            if result_record:
                result_record.marksobtained = row['marksobtained']
                result_record.grade = row['grade']
                result_record.sem = row['sem']
                result_record.year = row['year']
                result_record.supplementary=row['supplementary']

        db.session.commit()
        update_backlog()
        return jsonify({'message': 'Data updated successfully', 'file_path': temp_file_path})
    except Exception as e:
        db.session.rollback()
        logging.error(f"Error updating result CSV: {e}")
        return jsonify({'error': f'Error updating data: {e}'}), 500
    finally:
        db.session.close()

@app.route('/submit_manual_result', methods=['POST'])
def manual_update_result():
    student_id = request.form['studentId']
    course_id = request.form['courseId']
    marks_obtained = request.form['marksObtained']
    grade = request.form['grade']
    sem = request.form['sem']
    year = request.form['year']
    supplementary = request.form['supplementary']

    existing_result = Result.query.filter_by(studentid=student_id, courseid=course_id).first()
    if existing_result:
        # Update the existing record
        existing_result.marksobtained = marks_obtained
        existing_result.grade = grade
        existing_result.sem = sem
        existing_result.year = year
        existing_result.supplementary = supplementary
        db.session.commit()
        update_backlog()

        return 'Result updated successfully'

    return 'No matching record found for update'

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    app.run(debug=True)
