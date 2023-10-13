import pandas as pd
import sqlite3
import json
import logging
import os
import numpy

os.chdir("C:/Users/kamin/learning/subscriber-pipeline-starter-kit/dev/")

#logs even debug messages
logging.basicConfig(filename='cleanse_db.log', filemode='w', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', force=True)

#Get a logger:
logger = logging.getLogger(__name__)

###################################################################################################

#DB connection

###################################################################################################
#conn = sqlite3.connect("C:/Users/kamin/learning/subscriber-pipeline-starter-kit/dev/cademycode.db")


###################################################################################################

#Data ingestion
## courses == career_paths

###################################################################################################
#students = pd.read_sql_query("SELECT * FROM cademycode_df", conn)
#courses = pd.read_sql_query("SELECT * FROM cademycode_df", conn)
#student_jobs = pd.read_sql_query("SELECT * FROM cademycode_student_jobs", conn)


#change students table -> df
def cleanse_students_table(df):



    ###################################################################################################

    #Work with df table

    ###################################################################################################
    #Create [age] table
    # To approximate the age based on a date of birth ("dob") column in a pandas DataFrame, you can indeed use the current time minus the date of birth
    # However, to convert that timedelta to years, you'll need an additional step:
    #   1. Subtract the date of birth from the current time to get the age as a timedelta. 
    #   2.Convert that timedelta to years.

    now = pd.to_datetime("now")
    df["age"] = (now - pd.to_datetime(df["dob"])).dt.days / 365.25
    df['age'] = df['age'].astype(int)

    #Create [Age Group] table
    df["age_group"] = (df["age"]//10) * 10

    #Create [Age Range] table
    df["age_range"] = df["age_group"].astype(str) + "-" + (df["age_group"] + 9).astype(str)

    ###################################################################################################

    #missing value

    #find duplicate
    #dup_name = df[df["name"].duplicated(keep=False)]
    #df.loc[(df["name"] == "Robbie Davies") | (df["name"] == "Melvin Felt")]

    #find unique in sex
    #df["sex"].unique()

    #split contact info
    ###################################################################################################

    # Convert the 'contact_info' column to a list of dictionaries
    df['contact_info'] = df['contact_info'].apply(eval).tolist()

    # Use json_normalize to flatten the data
    normalized_df = pd.json_normalize(df['contact_info'])

    # Drop the 'contact_info' column
    df.drop('contact_info', axis=1, inplace=True)

    # Split the mailing_address into separate columns
    address_df = normalized_df['mailing_address'].str.split(', ', expand=True)
    address_df.columns = ['street', 'city', 'state', 'costal Code']

    # Drop the 'mailing_address' column
    normalized_df.drop('mailing_address', axis=1, inplace=True)

    # Concatenate the new columns and drop the original mailing_address column
    df = pd.concat([df, address_df, normalized_df], axis=1)

    ###################################################################################################

    #Fix Data type
    df['job_id'] = df['job_id'].astype(float)
    df['num_course_taken'] = df['num_course_taken'].astype(float)
    df['current_career_path_id'] = df['current_career_path_id'].astype(float)
    df['time_spent_hrs'] = df['time_spent_hrs'].astype(float)

    ###################################################################################################

    #Handling Missing Data

    ###################################################################################################
    ##num_course_taken Table
    missing_course_taken = df[df[["num_course_taken"]].isnull().any(axis=1)]

    #Creat Dataframe for store missing data
    missing_data = pd.DataFrame()
    missing_data = pd.concat([missing_data, missing_course_taken])

    #drop missing data in num_course_taken column
    df = df.dropna(subset=['num_course_taken'])

    ###################################################################################################
    ##job_id Table
    missing_job_id = df[df[["job_id"]].isnull().any(axis=1)]

    #store missing data
    missing_data = pd.concat([missing_data, missing_job_id])

    #drop missing data in job_id column
    df = df.dropna(subset=['job_id'])

    ###################################################################################################
    ##current_career_path_id Table
    #missing_carreer_path_id = df[df[["current_career_path_id"]].isnull().any(axis=1)]

    #Fill null with 0
    df = df.fillna({'time_spent_hrs': 0, 'current_career_path_id': 0})
    
    return(df, missing_data)


#df.to_csv("cleaned-df.csv", index = False)

#change courses table -> df
def cleanse_courses(df):

    ###################################################################################################


    #Work with df table



    ###################################################################################################

    #add career path id 0 -> not applicable

    df.head()

    not_applicable = {
        'career_path_id': 0,
        'career_path_name': 'not applicable',
        'hours_to_complete': 0
    } 

    df.loc[len(df)] = not_applicable
    
    return df



    #df.to_csv("cleaned-df.csv", index = False)
    
    
    
#change student_jobs table -> df
def cleanse_student_jobs(df):

    ###################################################################################################


    #Work with student jobs table



    ###################################################################################################

    #drop duplicate column
    df = df.drop_duplicates()
    return df

    #student_jobs.to_csv("cleaned-student_jobs.csv", index = False)


###################################################################################################


#Joining the table



###################################################################################################

#final_df = df.merge(df, left_on= "current_career_path_id", right_on="career_path_id", how= "left")
#final_df = final_df.merge(student_jobs, on="job_id", how="left")

#final_df.to_csv("Final-DataFrame.csv", index= False)
#missing_data.to_csv("Incomplete_data.csv", index= False)
###################################################################################################


#Load cleansed to new SQLite DB



###################################################################################################

# Create a connection to the SQLite database
#conn2 = sqlite3.connect("cademycode_cleaned.db")

# Write the Final DataFrame to a new table in the database
#final_df.to_sql('final_df', conn2, if_exists='replace', index=False)

# Write the Incomplete DataFrame to a new table in the database
#missing_data.to_sql('incomplete_data', conn2, if_exists='replace', index=False)

#conn2.close()

#db_missing = pd.read_sql_query("SELECT * FROM incomplete_data", conn2)



###################################################################################################
###################################################################################################
#   Create Unit Test
#       1.Create logger
#       2.Create Unit test
#           2.1 Test Null
#           2.2 Test Schema (Check DB table and DataFrame that we're working with aren't the same then an errir can be thrown during observe)
#           2.3 Test Number Columns (The local DF should have the same number of columns as the DB DF)
#           2.4

def test_null(df):
    df_missing = df[df.isnull().any(axis=1)]
    count_missing = len(df_missing)
    
    #how many missing row they are
    try:
        assert count_missing == 0, "There are " +str(count_missing) + "null in the table"
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else:
        print("No null row found")
        
        
        
#local_df: DataFrame that we are working with
#db_df: DataFrame that's already observed
def test_schema(local_df, db_df):
    #variable called error that will keep track of how many data types are incorrect
    errors = 0
    for col in db_df:
        #in try block, we'll check if the local DF has its column as the correct data type.
        #If its's not, then we'll increament our errors
        try:
            if local_df[col].dtypes != db_df[col].dtypes:
                errors += 1
        #in except block, let's except only name error.
        #In the event that one of the column doesn't exist in the local DF or vice versa
        #and we'll output this into our logger.
        except NameError as ne:
            logger.exception(ne)
            raise ne
        
    #outside for loop, we'll have a condition that check if we have any errors.
    #and the assertion error message should output how many errors we have
    #and we'll also output this message into our logger
    if errors > 0:
        assert_error_msg = str(errors) + " column(s) dtypes aren't the same"
        logger.exception(assert_error_msg)
        
    #Ouside of if condition, we'll also have as assert statement that will assert that errors has to be zero.
    #Otherwise, we'll also display the assert error message.
    assert errors == 0, assert_error_msg
    
    
#Another Unit test also related to its schema,
#and it's that the local DF should have the same number of columns as the DB DF
#So let's create unit test for that:
def test_num_cols(local_df, db_df):
    try:
        assert len(local_df.columns) == len(db_df.columns)
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else:
        print("Number of columns are the same.")
        
        
        
#To ensure that the join keys are all present in every single table we're joning with
#Because we're performing left joins, there's the possibility that's we're creating null values in our DF 
#let's create unit tests that will ensure that the join keys are the same
def test_for_path_id(students, courses):
    #Get all the unique IDs in the students table
    student_table = students.current_career_path_id.unique()
    #Check if any of the student table path IDs are also in the career paths ID 
    #by using NumPy function is in comparing our student table variable to  the unique job IDs of the career paths table
    is_subnet = np.isin(student_table, courses.career_path_id.unique())
    #Get all missing IDs by filtering for any IDs that are not present in the students table.
    missing_id = student_table[~is_subnet]
    
    #Now we have missing IDs, we can create a try and except block
    #In try block : We have assertion staement that the length of missing ID has to be equal to zero.
    try:
        assert len(missing_id) == 0, "Missing career_path_id(s): " + str(missing_id) + " in 'courses' table"
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else:
        print("All career_path_ids are present.")
        
        
        
#Unit test for job_id
#This time instead of looking for the career path ID, We're going to look for the job ID.
def test_for_job_id(students, student_jobs):
    student_table = students.job_id.unique()
    is_subnet = np.isin(student_table, student_jobs.job_id.unique())
    missing_id = student_table[~is_subnet]
    
    try:
        assert len(missing_id) == 0, "Missing job_id(s): " + str(missing_id) + " in 'student_jobs' table"
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else:
        print("All career_path_ids are present.")
        
        
        
        
        
        
        
#Create the main method
#The main method will be our driver when it comes to executing all of our cleansing and testing processes.
#1st thing we should do is 
#       1.Create a new log event that says that we have started the cleansing process

def main():
    #we'll send to our logger "start log", the signal that we've started the main method
    logger.into("Start Log")
    
    #Check we have the change log,
    #If we do have a chnge log, then we want to grab the latest version and increament on that version.
    #If we dont have, we should create this file and then initialze the new version.
    #To do this open up a file called change log:
    #a+ = specify append to this file or will creat this file
    with.open(".dev/changlog.md", "a+") as f:
        #that this file, let's read all the lines that exist.
        lines = f.readlines()
    #Check length of the lines
    #If it's zero: the file is new, so set a ner var called next version, and set that equal to zero
    if len(lines) == 0:
        next_ver = 0
    #However if length of the line is greater than zero, that means a file exists 
    #and a version number is contained in the file somewhere.
    else:
        #version number is formatted such as: major ver.-> X, minor ver -> Y, patch -> Z
        #When it come to adding more rows or more data to DF, we can consider that a patch, 
        #so our increament will mainly be on the Z variable
        #X,Y,Z
        #Parse the string to get from the first line of the file to pur latest version number
        #split line from first line or index, then delimit by period,
        #grab the third number that split from period -> [2] and we'll also grab the value contained from the split -> [0],
        #this produce a str var, to cast this to an integer ,we do that by calling the int function -> int()
        # +1  -> We also want to increment this to represent the next version number
        next_ver = int(lines[0].split(",")[2][0]) +1
    
    #access the tables in our database
    con = sqlite3.connect("C:/Users/kamin/learning/subscriber-pipeline-starter-kit/dev/cademycode.db")
    students = pd.read_sql_query("SELECT * FROM cademycode_df", con)
    courses = pd.read_sql_query("SELECT * FROM cademycode_df", con)
    student_jobs = pd.read_sql_query("SELECT * FROM cademycode_student_jobs", con)
    con.close()
        
        
    #Attempt to conncet to our cleanesed database
    #if there are any new students that condt currently live in the cleansed database
    #We only want to run this process on new students because we're tying to save computation time.
    #In order to do that, let's create a try and excepr block.
    #why use try -> sometime we'll run this code, we wont necessarl have the cleansed DB already created,
    #so there's no point in connecting to the cleansed DB if doesn't already exist
    
    #connect to the cleansed DB
    try:
        con = sqlite3.connect("C:/Users/kamin/learning/subscriber-pipeline-starter-kit/dev/cademycode_cleaned.db")
        clean_db = pd.read_sql_query("SELECT * FROM final_df", con)
        missing_db = pd.read_sql_query("SELECT * FROM incomplete_data", con)
        con.close()
        
        #filiter our students table for only students that are not in the cleaned DB
        #so we'll compare the UUID of the students table and the cleansed df table
        new_students = students[~np.isin(students.uuid.unique(), clean_db.uuid.unique())]
    
    #For except block, If the DB doesn't exist, then we'll simply just assign new students as all the students.
    except:
        new_students = students
        clean_db = []
        
    clean_new_students, missing_data = cleanse_students_table(new_students)
    
    #Make sure that any missing data that we observe is actually new missing data
    try:
        #filter for any data that doesn't already exist in the DB
        new_missing_data = missing_data[~np.isin(missing_data.uuid.unique(), missing_db.uuid.unique())]
    except:
        #if we dont have missing -> new missing data is all the missing data
        new_missing_data = missing_data
    
    #if ther are any rows in the new missing data DF -> initiate a connection to our cleansed and observe the missing data by using the append parameter. 
    if len(new_missing_data) > 0:
        sqlite_connection = sqlite3.connect("c:\Users\kamin\learning\subscriber-pipeline-starter-kit\cademycode_cleaned.db")
        missing_data.to_sql("incomplete_data", sqlite_connection, if_exists="append", index=False)
        sqlite_connection.close()
        
    if len(clean_new_students) > 0:
        cleanse_courses = cleanse_courses(courses)
        cleanse_student_jobs = cleanse_student_jobs(student_jobs)
        
        #run unit test
        test_for_job_id(clean_new_students, cleanse_student_jobs)
        test_for_path_id(clean_new_students, cleanse_courses)
        
        #merge DF together -> We have one DF that contains all three DF together
        df_clean = clean_new_students.merge(
            cleanse_courses,
            left_on="current_career_path_id",
            right_on="carreer_path_id",
            how="left"
        )
        df_clean = df_clean.merge(
            cleanse_student_jobs,
            on="job_id",
            how="left"
        )
        
        #Make sure DF correct schema and complete data before upserting to our cleanse DB
        #1.Check clean db table even has any rows
        if len(clean_db) > 0:
            test_num_cols(df_clean, clean_db)
            test_schema(df_clean, clean_db)
        test_null(df_clean)
        
        #Assuming that our cleansed DF passed the last three unit test
        #goto connect out SQLite DB and then being theupsetion process
        sqlite_connection = sqlite3.connect("c:\Users\kamin\learning\subscriber-pipeline-starter-kit\cademycode_cleaned.db")
        missing_data.to_sql("final_df", sqlite_connection, if_exists="append", index=False)
        clean_db = pd.read_sql_query("SELECT * FROM final_df", con)
        sqlite_connection.close()
        
        #create the CSV output
        clean_db.to_csv("cademycode_cleaned.csv")
        
        new_lines = [
            "## 0.0." + str(next_ver) + "\n" +
            "### Added\n" +
            "- " +str(len(df_clean)) + " more data to DB of raw data\n" +
            "- " +str(len(new_missing_data)) + " new missing data to incomplete_data table\n" +
            "\n"
        ]
        
        #join the list
        w_kines = "".join(new_lines + lines)
                
                
        # open up the change log
        with open("changlog.md", "w") as f:
            for line in w_lines:
                f.write(line)
    
    #print no new data            
    else:
        print("no new data")
        logger.info("no new data")
    logger.info("End Log")
    
    
#Automated script
if __name__ == "__main__":
    main()
        
        
        



