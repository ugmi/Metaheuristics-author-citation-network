#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 26 16:16:36 2023

@author: milasiunaite
"""

import requests
import xmltodict
import mysql.connector
from json import load, dump


def add_record_additional(mydb, mycursor, all_ids, data, ele, entry, sql):
    """
    Insert a row to the database (name 'additional').

    Parameters
    ----------
    mydb : database
        Connection to the database.
    mycursor : cursor
        Cursor connected to the database.
    all_ids : dict
        Dictionary of lists of publications, authors, and affiliations.
    data : dict
        Dictionary containing information about current state of the project.
    ele : dict
        Dictionary with info about the publication.
    entry : tuple
        Tuple containing the id of the referenced publication.
    sql : dict
        Dictionary of strings for inserting rows into the database.

    Returns
    -------
    all_ids : TYPE
        DESCRIPTION.
    data : TYPE
        DESCRIPTION.

    """
    if (int(ele['scopus-id']),) in all_ids['others']:
        data['indatabase'] += 1
        mycursor.execute(
            f"SELECT referenced_by FROM additional WHERE id={ele['scopus-id']}")
        articles_citing_ele = str(mycursor.fetchall()[0][0])
        if str(entry[0]) in articles_citing_ele.split(','):
            return all_ids, data
        articles_citing_ele = f'{articles_citing_ele},{entry[0]}'
        mycursor.execute(
            f'UPDATE additional SET referenced_by="{articles_citing_ele}" WHERE id={ele["scopus-id"]}')
    else:
        if (int(ele['scopus-id']),) in all_ids['publications']:
            data['indatabase'] += 1
            mycursor.execute(
                f'SELECT cites FROM publications WHERE eid={entry[0]}')
            references = str(mycursor.fetchall()[0][0])
            if str(ele['scopus-id']) in references.split(','):
                return all_ids, data
            elif references.split(',')[0] != '':
                references = f"{references},{ele['scopus-id']}"
            else:
                references = str(ele['scopus-id'])
            mycursor.execute(
                f'UPDATE publications SET cites="{references}" WHERE eid={entry[0]}')
            mydb.commit()
            return all_ids, data
        data['newlyadded'] += 1
        all_ids['others'].add((int(ele['scopus-id']),))
        authors, values_to_insert = [], []
        if ele['author-list'] is not None and 'author' in ele['author-list']:
            for author_info in ele['author-list']['author']:
                try:
                    authid = author_info['@auid']
                except KeyError:
                    authid = ''
                except TypeError:
                    continue
                if authid == '':
                    authid = data['auth_my']
                    data['auth_my'] += 1
                authors.append(str(authid))
                if (int(authid),) not in all_ids['authors']:
                    all_ids['authors'].add((int(authid),))
                    if author_info['affiliation'] is not None and '@id' in author_info['affiliation']:
                        afid = author_info['affiliation']['@id']
                    else:
                        afid = ''
                    if 'author-url' in author_info:
                        url = author_info['author-url']
                    else:
                        url = ''
                    if 'ce:given-name' in author_info:
                        first_name = author_info['ce:given-name']
                    else:
                        first_name = ''
                    if 'ce:initials' in author_info:
                        initials = author_info['ce:initials']
                    else:
                        initials = ''
                    values_to_insert.append(
                        (authid, author_info['ce:indexed-name'],
                         author_info['ce:surname'], first_name,
                         initials, afid, url))
            if len(values_to_insert) > 0:
                mycursor.executemany(sql['a'], values_to_insert)
                mydb.commit()
        if 'title' in ele:
            title = ele['title']
        else:
            title = ''
        try:
            doi = ele['ce:doi']['#text']
        except KeyError:
            doi = ''
        except TypeError:
            doi = ele['ce:doi'][0]['#text']
        if 'sourcetitle' in ele:
            source = ele['sourcetitle']
        else:
            source = ''
        try:
            citedby = int(ele['citedby-count']['#text'])
        except KeyError:
            citedby = 0
        except TypeError:
            try:
                citedby = int(ele['citedby-count'][0]['#text'])
            except KeyError:
                citedby = 0
        if 'prism:coverDate' in ele:
            date = ele['prism:coverDate']
        else:
            date = None
        mycursor.execute(sql['p'], (ele['scopus-id'], title, ele['url'],
                                    ele['type'], ','.join(authors), citedby,
                                    date, doi, source, str(entry[0])))
    mydb.commit()
    return all_ids, data


db_data = load(open('mydb_setup.json'))
mydb = mysql.connector.connect(**db_data)
sql = {
    'a': ('INSERT INTO authors (id, authname, surname, given_name,'
          ' initials, afids, url) VALUES (%s, %s, %s, %s, %s, %s, %s)'),
    'p': ('INSERT INTO additional (id, title, url, reference_type, authors,'
          ' citedby, date, doi, source, referenced_by) VALUES'
          ' (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)')
    }
api = 'https://api.elsevier.com/content/abstract/eid/{eid}?view=REF'
headers = requests.utils.default_headers()
data = load(open('headers.json'))
for head in data:
    headers[head] = data[head]
data = load(open('save.json'))
data['records_checked'] = 0
data['newlyadded'] = 0
data['indatabase'] = 0
all_ids = dict()
mycursor = mydb.cursor()
mycursor.execute('SELECT eid FROM publications WHERE field LIKE "BA%"')
all_ids['subfield'] = set(mycursor.fetchall())
mycursor.execute('SELECT eid FROM publications')
all_ids['publications'] = set(mycursor.fetchall())
mycursor.execute('SELECT id FROM authors')
all_ids['authors'] = set(mycursor.fetchall())
mycursor.execute('SELECT id from additional')
all_ids['others'] = set(mycursor.fetchall())
reference_set = set()
mycursor.execute('SELECT referenced_by FROM additional')
reference_strings = set(mycursor.fetchall())
for string in reference_strings:
    reference_list = string[0].split(',')
    for ref in reference_list:
        reference_set.add((int(ref),))
for entry in all_ids['subfield'].difference(reference_set):
    eid = f'2-s2.0-{entry[0]}'
    response = requests.get(api.format(eid=eid), headers=headers)
    try:
        result_dict = xmltodict.parse(response.content)['abstracts-retrieval-response']['references']
    except KeyError:
        continue
    mycursor.execute(
        f'UPDATE publications SET ref_count={int(result_dict["@total-references"])} WHERE eid={entry[0]}')
    mydb.commit()
    if result_dict['@total-references'] == '1':
        result_dict['reference'] = [result_dict['reference']]
    try:
        for ele in result_dict['reference']:
            all_ids, data = add_record_additional(
                mydb, mycursor, all_ids, data, ele, entry, sql)
            data['records_checked'] += 1
    except KeyboardInterrupt:
        with open('save.json', 'w', encoding='utf8') as json_file:
            dump(data, json_file, ensure_ascii=False)
        foo = mycursor.fetchall()  # Collect records to avoid raising errors
        for ele in result_dict['reference']:
            all_ids, data = add_record_additional(
                mydb, mycursor, all_ids, data, ele, entry, sql)
        break
with open('save.json', 'w', encoding='utf8') as json_file:
    dump(data, json_file, ensure_ascii=False)
