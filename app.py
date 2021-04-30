import argparse
import string
from time import time
from itertools import cycle
import faker
import pymysql.cursors
from random import choice, randint


def time_manager(func):
    def timer(*args, **kwargs):
        start_val = time()
        result = func(*args, **kwargs)
        end_val = time()
        print(f'Время выполнения запроса: {end_val - start_val}')
        return result

    return timer


def create_table(args):
    """Функция создает базу данных и добавляет в нее таблицу USERS"""
    connection.cursor().execute("""drop database if exists ptmk""")

    create = """create database ptmk;
            use ptmk;
            CREATE TABLE users(`id` SERIAL, `name` VARCHAR(255),
           `birthday` DATE, `gender` CHAR(1) COLLATE utf8_unicode_ci, PRIMARY KEY (`id`))"""
    for element in create.split(';'):
        try:
            connection.cursor().execute(element)
            connection.commit()
        except ValueError as err:
            print(f"FAIL IN {str(element)}, {err}")


def add_row(args):
    """Функция создает запись в таблице USERS"""
    connection.cursor().execute("""use ptmk""")
    with connection.cursor() as cursor:
        cursor.execute(f"""insert into users (name, birthday, gender) values ('{args.input_name}',
                        '{args.input_birthday}', '{args.input_gender}');""")
    connection.commit()


def select_all(args):
    """
    Вывод всех строк с уникальным значением ФИО+дата,
    отсортированным по ФИО , вывести ФИО, Дату
    рождения, пол, кол-во полных лет.
    """
    connection.cursor().execute("""use ptmk""")
    with connection.cursor() as cursor:
        cursor.execute(f"""select distinct name, birthday, gender, 
                    (
                        (YEAR(CURRENT_DATE) - YEAR(birthday)) -                             
                        (DATE_FORMAT(CURRENT_DATE, '%m%d') < DATE_FORMAT(birthday, '%m%d')) 
                    ) AS age 
                    from users order by name;""")
        for row in cursor.fetchall():
            full_info = ' '.join(str(el) for el in row)
            print(full_info)
    connection.commit()


def add_data(args):
    """
    Заполнение автоматически 1000000 строк. Распределение
    пола в них должно быть относительно равномерным,
    начальной буквы ФИО также. Заполнение автоматически
    100 строк в которых пол мужской и ФИО начинается с "F".
    """
    connection.cursor().execute("""use ptmk""")
    with connection.cursor() as cursor:
        fake_data = []
        fake_gender = cycle(['M', 'F'])
        fake = faker.Faker()
        letters = string.ascii_lowercase

        for _ in range(1000000):
            rand_fullname = [''.join(choice(letters) for _ in range(randint(2, 15))) for _ in range(3)]
            rand_date = fake.date()
            fake_data.append(f"('{' '.join(rand_fullname).title()}', '{rand_date}', '{next(fake_gender)}')")

        for _ in range(100):
            rand_fullname = [''.join(choice(letters) for _ in range(randint(2, 15))) for _ in range(3)]
            full_name_start_f = f"f{' '.join(rand_fullname)}".title()
            rand_date = fake.date()
            fake_data.append(f"('{full_name_start_f}', '{rand_date}', 'M')")

        cursor.execute("""start transaction;""")
        cursor.execute(f"""insert into users (name, birthday, gender) values {', '.join(fake_data)};""")
        cursor.execute("""commit;""")
    connection.commit()


@time_manager
def select_man_f(args):
    """
    Результат выборки из таблицы по критерию: пол мужской,
    ФИО  начинается с "F". Сделать замер времени выполнения.
    """
    connection.cursor().execute("""use ptmk""")
    with connection.cursor() as cursor:
        cursor.execute(f"""select distinct name, birthday, gender, 
                        (
                        (YEAR(CURRENT_DATE) - YEAR(birthday)) -                             
                        (DATE_FORMAT(CURRENT_DATE, '%m%d') < DATE_FORMAT(birthday, '%m%d')) 
                        ) AS age 
                        from users 
                        where gender = 'M'
                        and name LIKE 'F%'
                        order by name
                        ;""")
        for row in cursor.fetchall():
            full_info = ' '.join(str(el) for el in row)
            print(full_info)
    connection.commit()


parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers()

parser_1 = subparsers.add_parser('1')
parser_1.set_defaults(func=create_table)

parser_2 = subparsers.add_parser('2')
parser_2.add_argument('input_name')
parser_2.add_argument('input_birthday')
parser_2.add_argument('input_gender')
parser_2.set_defaults(func=add_row)

parser_3 = subparsers.add_parser('3')
parser_3.set_defaults(func=select_all)

parser_4 = subparsers.add_parser('4')
parser_4.set_defaults(func=add_data)

parser_5 = subparsers.add_parser('5')
parser_5.set_defaults(func=select_man_f)

if __name__ == '__main__':
    connection = pymysql.connect(host='localhost',
                                 user=input('Имя пользователя: '),
                                 password=input('Пароль: '))
    args = parser.parse_args()
    args.func(args)

    connection.close()
