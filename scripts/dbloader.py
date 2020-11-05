import pyodbc 
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=localhost;'
                      'Database=Reddit_Submissions;'
                      'Trusted_Connection=yes;')
 
cursor = conn.cursor()

cursor.execute('''
                INSERT INTO Reddit_Comments.dbo.Table_1 (Name, Age, City)
                VALUES
                ('Bob',55,'Montreal'),
                ('Jenny',66,'Boston')
                ''')
conn.commit()

cursor.execute('SELECT * FROM Reddit_Comments.dbo.Table_1')

for row in cursor:
    print(row)