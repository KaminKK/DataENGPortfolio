### Background

---

Company XYZ recently migrated database systems causing some of the date_joined records to be NULL. You're told by an analyst in human resources NULL records for the date_joined field indicates the employees joined prior to 2010. You also find out there are multiple employees with the same name and duplicate records for some employees.

### Question

---

Given this, write code to find the number of employees that joined each month. You can group all of the null values as Dec 1, 2009.

### Table

---

| employee_name | employee_id | date_joined | age | yrs_of_experience |
| ------------- | ----------- | ----------- | --- | ----------------- |
| Andy          | 123456      | 2015-02-15  | 45  | 24                |
| Beth          | 78945       | None        | 36  | 14                |
| Cindy         | 654123      | 2017-05-16  | 34  | 14                |
| Dale          | 963852      | 2018-01-15  | 25  | 4                 |
| Andy          | 123457      | 2009-07-10  | 45  | 23                |
| Cindy         | 654124      | None        | 34  | 15                |


### Solution

---

1. Create Table
2. Create DataFrame
3. Cleaning Data
4. Fill NULL value with '2019-12-01'
5. Create a new column 'join_month' to extract the month from the 'date_joined' column
6. Group by 'join_month' and count the number of employees in each group
