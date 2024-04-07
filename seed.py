import psycopg2
from psycopg2 import Error
from faker import Faker as fk
import random 
from tabulate import tabulate

#______________________________________________________________________________________________________________________________________________________________________________
#______________________________________________________________________________________________________________________________________________________________________________
#______________________________________________________________________________________________________________________________________________________________________________
# To work with programm it is necessary to create the DB postgresql with user="postgres",     password="password",    host="localhost",    port="5432",    database="postgres"
#______________________________________________________________________________________________________________________________________________________________________________
#______________________________________________________________________________________________________________________________________________________________________________
#______________________________________________________________________________________________________________________________________________________________________________

fake=fk('it_IT')
def generate_random_task(min,max):
    task_name = fake.catch_phrase()[:25]  # Generate a random task name
    description = fake.text()[:50]  # Generate a random description
    status_id = random.choice([1,2,3])   #(['New', 'In Progress', 'Completed'])  # Randomly choose a status
    user_id = int(random.randint(min,max))
    return (task_name, description,status_id,user_id)

def help():
    return("\'1' - Отримати всі завдання певного користувача.\n\
'2' - Вибрати завдання за певним статусом. \n\
'3' - Оновити статус конкретного завдання.\n\
'4' - Отримати список користувачів, які не мають жодного завдання.\n\
'5' - Додати нове завдання для конкретного користувача.\n\
'6' - Отримати всі завдання, які ще не завершено.\n\
'7' - Видалити конкретне завдання.\n\
'8' - Знайти користувачів з певною електронною поштою.\n\
'9'- Оновити ім'я користувача.\n\
'10'- Отримати кількість завдань для кожного статусу.\n\
'11' - Отримати завдання, які призначені користувачам з певною доменною частиною електронної пошти.\n\
'12' - Отримати список завдань, що не мають опису.\n\
'13' - Вибрати користувачів та їхні завдання, які є у статусі 'В ПРОГРЕСІ'.\n\
'14' - Отримати користувачів та кількість їхніх завдань.\n\
'users' - to add 100 users\n\
'tasks' - to add 100 tasks\n\
'exit' - Щоб вийти\n\
'help' - для інформації")
help()

try:
    conn = psycopg2.connect(
    user="postgres",
    password="password",
    host="localhost",
    port="5432",
    database="postgres"
    )
    cursor = conn.cursor()
    
    sqt_create_users = "CREATE TABLE if not exists users (id SERIAL PRIMARY key, fullname VARCHAR(100), email VARCHAR(100) unique);"
    sql_crete_status = "CREATE TABLE IF NOT EXISTS status (id SERIAL primary key,name VARCHAR(50) unique);"
    sql_create_tasks = "create table if not exists tasks (id serial primary key, title varchar(100), description text, status_id INTEGER,	foreign key (status_id) REFERENCES status (id),	user_id INTEGER,	foreign key (user_id) REFERENCES users (id) on delete cascade);"
    sql_status = "INSERT INTO status (name) VALUES ('New'), ('In Progress'), ('Completed');"
    cursor.execute(sqt_create_users)
    cursor.execute(sql_crete_status)
    cursor.execute(sql_create_tasks)
    cursor.execute(sql_status)    
    conn.commit()    

except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)   

def if_user_exists(id): 
    try:
        conn = psycopg2.connect(
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
        database="postgres"
        )
        cursor = conn.cursor()
        
        sql = f"SELECT * FROM users WHERE id = {id}"
        cursor.execute(sql)
        stat = cursor.fetchone()
        conn.commit()
        return stat

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)

def if_ststus_exists(id):
    try:
        conn = psycopg2.connect(
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
        database="postgres"
        )
        cursor = conn.cursor()
        sql = f"SELECT * FROM status WHERE id = {id}"
        cursor.execute(sql)
        stat = cursor.fetchone()
        conn.commit()
        return stat

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)

def if_task_exists(id):
    try:
        conn = psycopg2.connect(
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
        database="postgres"
        )
        cursor = conn.cursor()
        sql = f"SELECT * FROM tasks WHERE id = {id}"
        cursor.execute(sql)
        stat = cursor.fetchone()
        conn.commit()
        return stat

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)       

def fill_users():
    users=[]
    for _ in range(100):
        users.append((fake.name(),fake.email()))

    try:
        conn = psycopg2.connect(
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
        database="postgres"
        )
        cursor = conn.cursor()
        
        add_users = "INSERT INTO users (fullname, email) VALUES (%s, %s);"

        cursor.executemany(add_users,users)

        cursor.execute("SELECT * FROM users;")
        result=cursor.fetchall()
        if result:
            print(tabulate(result,["ID", "NAME","EMAIL"], tablefmt='fancy_grid'))
        else:
            print("Table already exists")

        conn.commit()    

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)   

def fill_tasks():
    tasks=[]


    try:
        conn = psycopg2.connect(
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
        database="postgres"
        )
        cursor = conn.cursor()
        
        add_tasks = "INSERT INTO tasks (title, description,status_id,user_id) VALUES (%s, %s, %s,%s);"
        
        cursor.execute("SELECT MIN(id) FROM users;")
        user_id_min = cursor.fetchone()[0]

        cursor.execute("SELECT MAX(id) FROM users;")
        user_id_max = cursor.fetchone()[0]
        print(user_id_min)
        print(user_id_max)
        for _ in range(100):
            tasks.append(generate_random_task(user_id_min,user_id_max))

        #random_user_id = int(random.randint(user_id_min,user_id_max))
        cursor.executemany(add_tasks,tasks)

        cursor.execute("SELECT * FROM tasks;")
        result=cursor.fetchall()
        if result:
            print(tabulate(result,["ID", "TITLE","DESCRIPTION","STATUS ID","USER ID"], tablefmt='fancy_grid'))
        else:
            print("Table already exists")

        conn.commit()    

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)   



def all_tasks_of_user():  #1
    try:
        conn = psycopg2.connect(
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
        database="postgres"
        )
        cursor = conn.cursor()
        
        id=input("Input user ID: ")
        if if_user_exists(id):
            sql = f"SELECT id, title, description, status_id  FROM tasks WHERE user_id = {id}"
            cursor.execute(sql)
            max_user = cursor.fetchall()
            if max_user:
                print(tabulate(max_user, ["TASK ID","TITLE","DESCRIPTION","STATUS ID"], tablefmt='fancy_grid'))
            else:
                print("No tasks assigned")
        else: 
            print("No user with this ID in database")
        conn.commit()    

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)

def tasks_with_status():  #2
    try:
        conn = psycopg2.connect(
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
        database="postgres"
        )
        cursor = conn.cursor()
        
        id=input("Input status: 1 for New, 2 for In Progree, 3 for Completed: ")
        if if_ststus_exists(id):
            sql = f"SELECT t.id, t.title, t.description,u.fullname  FROM tasks as t  left join users as u on u.id = t.user_id where t.status_id = {id}"
            cursor.execute(sql)
            tasks_with_stat = cursor.fetchall()
            print(tasks_with_stat)
            if tasks_with_stat:
                print(tabulate(tasks_with_stat, ["TASK ID","TITLE","DESCRIPTION","EXECUTOR NAME"], tablefmt='fancy_grid'))
            else:
                print("No tasks with this status")
        else: 
            print("No status with this ID in database")
        conn.commit()
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)

def change_status(): #3
    try:
        conn = psycopg2.connect(
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
        database="postgres"
        )
        cursor = conn.cursor()
        
        id=input("Input task ID: ")
        stat_name=input("Input new status: ")
        if if_task_exists(id):
            sql = f"UPDATE tasks SET status_id = (SELECT id FROM status WHERE name = '{stat_name}') WHERE id = {id}"
            cursor.execute(sql)
            sql1 = f"SELECT t.id, t.title, t.description, s.name, u.fullname  FROM tasks as t  left join users as u on u.id = t.user_id LEFT JOIN status as s ON  s.id= t.status_id where t.id = {id}"
            cursor.execute(sql1)
            table = cursor.fetchall()
            print(tabulate(table, ["TASK ID","TITLE","DESCRIPTION","STATUS","EXECUTOR"], tablefmt='fancy_grid'))
        else: 
            print("No task with this ID")
        conn.commit()
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)    

def users_no_tasks(): #4
    try:
        conn = psycopg2.connect(
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
        database="postgres"
        )
        cursor = conn.cursor()
    
        sql = f"SELECT u.id, u.fullname FROM users as u WHERE u.id NOT IN (SELECT DISTINCT user_id FROM tasks)"
        cursor.execute(sql)
        max_user = cursor.fetchall()
        if max_user:
            print(tabulate(max_user, ["USED ID","FULLNAME"], tablefmt='fancy_grid'))
        else:
            print("No users without tasks")
        conn.commit()    

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)

def add_task_for_user(id): #5
    try:
        conn = psycopg2.connect(
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
        database="postgres"
        )
        cursor = conn.cursor()
        

        
        if if_user_exists(id):
            title=input("Input task title: ")
            description=input("Provide task description: ")
            status=int(input("Input status 1,2,3: "))
            string = (title,description,status,int(id))
            sql = "INSERT INTO tasks (title,description,status_id,user_id) VALUES(%s,%s,%s,%s);"
            cursor.execute(sql,string)
            print("Task added")
            cursor.execute("SELECT id, title, description, status_id, user_id FROM tasks ORDER BY id DESC LIMIT 1")
            max_user = cursor.fetchmany()
            print(tabulate(max_user, ["TASK ID","TITLE","DESCRIPTION","STATUS ID","USED ID"], tablefmt='fancy_grid'))

        else: 
            print("No user with this ID in database")
        conn.commit()    

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)   

def show_not_closed():  #6
    try:
        conn = psycopg2.connect(
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
        database="postgres"
        )
        cursor = conn.cursor()
        
        sql = "SELECT s.id, s.title, s.description, u.fullname FROM tasks as s left join users as u on u.id = s.user_id WHERE s.status_id != 3"
        cursor.execute(sql)
        result = cursor.fetchall()
        if result:
            print(tabulate(result, ["TASK ID","TITLE","DESCRIPTION","FULL NAME"], tablefmt='fancy_grid'))
        else:
            print("No unassigned tasks")

        conn.commit()    

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)   

def delete_task(): #7
    try:
        conn = psycopg2.connect(
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
        database="postgres"
        )
        cursor = conn.cursor()
        
        id=input("Input task ID that You want to remove: ")
        if if_task_exists(id):
            sql = f"DELETE FROM tasks WHERE id = {id};"
            cursor.execute(sql)
            print(f"Task with id={id} is deleted")
            # tasks_with_stat = cursor.fetchall()
            # print(tasks_with_stat)
            # if tasks_with_stat:
            #     print(tabulate(tasks_with_stat, ["TASK ID","TITLE","DESCRIPTION","EXECUTOR NAME"], tablefmt='fancy_grid'))
            # else:
            #     print("No tasks with this status")
        else: 
            print("No tasks with this ID in database")
        conn.commit()
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)    

def select_by_mail(): #8
    try:
        conn = psycopg2.connect(
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
        database="postgres"
        )
        cursor = conn.cursor()

        email=input("Input user email: ")

        cursor.execute("SELECT id, fullname, email FROM users WHERE email LIKE %s",(email,))
        max_user = cursor.fetchall()
        if max_user:
            print(tabulate(max_user, ["ID","NAME","EMAIL"], tablefmt='fancy_grid'))
        else:
            print("No user with this email")

        conn.commit() 


 

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
 
def change_name(): #9
    try:
        conn = psycopg2.connect(
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
        database="postgres"
        )
        cursor = conn.cursor()
        
        id=input("Input user ID: ")
        name=input("Input new name: ")
        if if_user_exists(id):
            #sql = f"UPDATE users SET name = %s WHERE id = %s"
            cursor.execute("UPDATE users SET fullname = %s WHERE id = %s",(name,id))
            cursor.execute("SELECT * FROM users WHERE id = %s",(id,))
            max_user = cursor.fetchall()
            if max_user:
                print(tabulate(max_user, ["ID","NAME","EMAIL"], tablefmt='fancy_grid'))
            else:
                print("No user with this ID")
        else: 
            print("No user with this ID in database")
        conn.commit()    

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)    

def group_t_by_stat(): #10
    try:
        conn = psycopg2.connect(
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
        database="postgres"
        )
        cursor = conn.cursor()
        
        cursor.execute("SELECT status_id, COUNT(*) AS task_count FROM tasks GROUP BY status_id;")
        max_user = cursor.fetchall()
        if max_user:
            print(tabulate(max_user, ["STATUS","AMOUNT"], tablefmt='fancy_grid'))
        else:
            print("No user with this ID")

        conn.commit()    

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)    

def select_by_domain(): #11
    try:
        conn = psycopg2.connect(
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
        database="postgres"
        )
        cursor = conn.cursor()

        email=input("Input user email: ")

        cursor.execute("SELECT tasks.id, tasks.title , tasks.description , users.fullname, users.email  FROM tasks JOIN users ON tasks.user_id = users.id WHERE users.email LIKE %s;",('%'+email,))
        max_user = cursor.fetchall()
        if max_user:
            print(tabulate(max_user, ["ID","TITLE","FESCRIPTION","NAME","EMAIL"], tablefmt='fancy_grid'))
        else:
            print("No user with this email")

        conn.commit()
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)

def tasks_no_discr(): #12
    try:
        conn = psycopg2.connect(
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
        database="postgres"
        )
        cursor = conn.cursor()

        cursor.execute("SELECT tasks.id, tasks.title FROM tasks WHERE tasks.description IS NULL OR tasks.description = '';")
        max_user = cursor.fetchall()
        if max_user:
            print(tabulate(max_user, ["ID","TITLE"], tablefmt='fancy_grid'))
        else:
            print("No tasks without description")

        conn.commit()
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)

def users_tasks_in_progree(): #13
    try:
        conn = psycopg2.connect(
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
        database="postgres"
        )
        cursor = conn.cursor()

        cursor.execute("SELECT users.id, users.fullname, tasks.title,status.name FROM users INNER JOIN tasks ON users.id = tasks.user_id join status on tasks.status_id = status.id WHERE tasks.status_id = 2;")
        max_user = cursor.fetchall()
        if max_user:
            print(tabulate(max_user, ["ID","TITLE"], tablefmt='fancy_grid'))
        else:
            print("No tasks without description")

        conn.commit()
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)

def users_task_amount(): #14
    try:
        conn = psycopg2.connect(
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
        database="postgres"
        )
        cursor = conn.cursor()

        cursor.execute("SELECT users.id, users.fullname, COUNT(tasks.id) AS task_count FROM users LEFT JOIN tasks ON users.id = tasks.user_id GROUP BY users.id;")
        max_user = cursor.fetchall()
        if max_user:
            print(tabulate(max_user, ["ID","NAME","AMOUNT OF TASKS ASSIGNED"], tablefmt='fancy_grid'))
        else:
            print("No tasks without description")

        conn.commit()
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)   
   

while True:
    task = (input(f"Input task number 1 to 14: ")).lower()
    if task == "exit":     
        break

    elif task == "1":
        all_tasks_of_user()
    elif task == "2":
        tasks_with_status()
    elif task =="3":
        change_status()
    elif task == "4":
        users_no_tasks()
    elif task == "5":
        add_task_for_user(input("Provide the ID of user, to whom you want to assign the task: "))       
    elif task == "6":
        show_not_closed()
    elif task == "7":
        delete_task()
    elif task == "8":
        select_by_mail()
    elif task == "9":
        change_name()  
    elif task == "10":
        group_t_by_stat()    
    elif task == "11":
        select_by_domain()
    elif task == "12":
        tasks_no_discr()
    elif task == "13":
        users_tasks_in_progree()
    elif task == "14":
        users_task_amount()
    elif task == "help":
        print(help())
    elif task == "users":
        fill_users()
    elif task == "tasks":
        fill_tasks()
    else:
        print("Command not recognized, for help type 'help'")



 