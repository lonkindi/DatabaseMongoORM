import csv
import re
from pprint import pprint
import datetime

from pymongo import MongoClient

client = MongoClient("localhost", 27017)
ticket_db = client['concert_db']


def read_data(csv_file, db):  #
    """
    Загрузить данные в бд из CSV-файла
    """
    with open(csv_file, encoding='utf8') as csvfile:
        # прочитать файл с данными и записать в коллекцию
        reader = csv.DictReader(csvfile)
        concert_dict = list(reader)
        for item in concert_dict:
            item['Цена'] = int(item['Цена'])
            date_str = item['Дата']
            date_new = date_str.replace('.', '') + '2020'
            item['Дата'] = datetime.datetime.strptime(date_new, '%d%m%Y')
        res_id = db.concert_collection.insert_many(concert_dict).inserted_ids
        print(f'Документы в количестве {len(res_id)} шт. добавлены в базу')


def find_cheapest(db):
    """
    Отсортировать билеты из базы по возрастанию цены
    Документация: https://docs.mongodb.com/manual/reference/method/cursor.sort/
    """
    res = list(db.concert_collection.find().sort('Цена'))
    pprint(res)


def find_by_name(name, db):
    """
    Найти билеты по имени исполнителя (в том числе – по подстроке, например "Seconds to"),
    и вернуть их по возрастанию цены
    """

    regex = re.compile(f'.*{name}.*')
    res = list(db.concert_collection.find({"Исполнитель": {'$regex': regex}}).sort('Цена'))
    pprint(res)


def sort_by_date(db):
    res = list(db.concert_collection.find().sort('Дата'))
    pprint(res)


if __name__ == '__main__':
    ticket_db.concert_collection.delete_many({}) # очистка коллекции
    read_data('artists.csv', ticket_db)
    find_cheapest(ticket_db)
    artist = input('Введите текст для поиска исполнителя -> ')
    find_by_name(artist, ticket_db)
    sort_by_date(ticket_db)
