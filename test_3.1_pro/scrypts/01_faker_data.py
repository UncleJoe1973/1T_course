#!/usr/bin/env python
# coding: utf-8

from faker import Faker
from faker.providers import company
import hashlib
import csv
import os
import shutil

print(f"Faker's startind")
 
if os.path.exists('data'):
		shutil.rmtree('data')

os.mkdir(r'data')

fake = Faker()
Faker.seed(0)
lst = list()
for _ in range(20):
    lst.append(fake.company())
lst = [(hashlib.md5(bytes(i, 'utf8')).hexdigest(), i.replace('\'', '')) for i in lst]

with open(r'data/publishers.csv', 'w', newline='') as csvfile:
    spamwriter = csv.writer(csvfile, delimiter=';', quotechar='\'', quoting=csv.QUOTE_NONNUMERIC)
    spamwriter.writerows(lst)

Faker.seed(1)
lst = list()
for _ in range(50):
    _ = []
    _.append(fake.ssn())
    tmp = fake.unique.first_name_nonbinary() + ' ' + fake.unique.last_name_nonbinary()
    _.insert(0, hashlib.md5(bytes(tmp, 'utf8')).hexdigest())
    _.append(tmp)
    _.append(fake.unique.address().replace('\n', ' '))
    _.append(fake.unique.phone_number()) 
 
    lst.append(_)

with open(r'data/readers.csv', 'w', newline='') as csvfile:
    spamwriter = csv.writer(csvfile, delimiter=';', quotechar='\'', quoting=csv.QUOTE_NONNUMERIC)
    spamwriter.writerows(lst)

Faker.seed(2)
lst = list()
for _ in range(5):
    _ = []
    tmp = fake.unique.first_name_nonbinary() + ' ' + fake.unique.last_name_nonbinary()
    _.append(hashlib.md5(bytes(tmp, 'utf8')).hexdigest())
    _.append(tmp)
    _.append(fake.job()) 
 
    lst.append(_)

with open(r'data/employees.csv', 'w', newline='') as csvfile:
    spamwriter = csv.writer(csvfile, delimiter=';', quotechar='\'', quoting=csv.QUOTE_NONNUMERIC)
    spamwriter.writerows(lst)

Faker.seed(3)
lst = list()
for _ in range(40):
    _ = []
    tmp = fake.unique.first_name_nonbinary() + ' ' + fake.unique.last_name_nonbinary()
    _.append(hashlib.md5(bytes(tmp, 'utf8')).hexdigest())
    _.append(tmp)
 
    lst.append(_)

with open(r'data/authors.csv', 'w', newline='') as csvfile:
    spamwriter = csv.writer(csvfile, delimiter=';', quotechar='\'', quoting=csv.QUOTE_NONNUMERIC)
    spamwriter.writerows(lst)

print(f"Faker's finishing")



