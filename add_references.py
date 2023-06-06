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
    if (int(ele['scopus-id']),) in all_ids['ref']:
        data['indatabase'] = data['indatabase'] + 1
        mycursor.execute('SELECT referenced_by FROM additional WHERE id={eid}'.format(eid=ele['scopus-id']))
        cit = str(mycursor.fetchall()[0][0])
        if str(entry[0]) in cit.split(','):
            return all_ids, data
        string = cit + ',' + str(entry[0])
        mycursor.execute('UPDATE additional SET referenced_by="{string}" WHERE id={eid}'.format(string=string, eid=ele['scopus-id']))
    else:
        if (int(ele['scopus-id']),) in all_ids['all']:
            data['indatabase'] = data['indatabase'] + 1
            mycursor.execute('SELECT cites FROM publications WHERE eid={eid}'.format(eid=entry[0]))
            cit = str(mycursor.fetchall()[0][0])
            if str(ele['scopus-id']) in cit.split(','):
                return all_ids, data
            elif cit.split(',')[0] != '':
                cit = cit + ',' + str(ele['scopus-id'])
            else:
                cit = str(ele['scopus-id'])
            mycursor.execute('UPDATE publications SET cites="{cites}" WHERE eid={eid}'.format(cites=cit, eid=entry[0]))
            mydb.commit()
            return all_ids, data
        data['newlyadded'] = data['newlyadded'] + 1
        all_ids['ref'].add((int(ele['scopus-id']),))
        authors, val = [], []
        try:
            for a in ele['author-list']['author']:
                try:
                    authid = a['@auid']
                except KeyError:
                    authid = ''
                except TypeError:
                    continue
                if authid == '':
                    authid = data['auth_my']
                    data['auth_my'] = data['auth_my'] + 1
                authors.append(str(authid))
                if (int(authid),) not in all_ids['a']:
                    all_ids['a'].add((int(authid),))
                    try:
                        afid = a['affiliation']['@id']
                    except KeyError:
                        afid = ''
                    except TypeError:
                        afid = ''
                    try:
                        url = a['author-url']
                    except KeyError:
                        url = ''
                    except TypeError:
                        url = ''
                    try:
                        prenom = a['ce:given-name']
                    except KeyError:
                        prenom = ''
                    val.append((authid, a['ce:indexed-name'],
                                a['ce:surname'], prenom, a['ce:initials'],
                                afid, url))
            if len(val) > 0:
                mycursor.executemany(sql['a'], val)
                mydb.commit()
        except KeyError:
            pass
        except TypeError:
            pass
        try:
            title = ele['title']
        except KeyError:
            title = ''
        try:
            doi = ele['ce:doi']['#text']
        except KeyError:
            doi = ''
        except TypeError:
            doi = ele['ce:doi'][0]['#text']
        try:
            source = ele['sourcetitle']
        except KeyError:
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
        try:
            date = ele['prism:coverDate']
        except KeyError:
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
all_ids = dict()
mycursor = mydb.cursor()
mycursor.execute('SELECT eid FROM publications WHERE field LIKE "%ANT%"')
all_ids['fa'] = set(mycursor.fetchall())
mycursor.execute('SELECT eid FROM publications')
all_ids['all'] = set(mycursor.fetchall())
mycursor.execute('SELECT id FROM authors')
all_ids['a'] = set(mycursor.fetchall())
mycursor.execute('SELECT id from additional')
all_ids['ref'] = set(mycursor.fetchall())
ref_set = set()
mycursor.execute('SELECT referenced_by FROM additional')
ref_str = set(mycursor.fetchall())
for string in ref_str:
    refl = string[0].split(',')
    for ref in refl:
        ref_set.add((int(ref),))
for entry in all_ids['fa'].difference(ref_set):
    eid = '2-s2.0-' + str(entry[0])
    response = requests.get(api.format(eid=eid), headers=headers)
    try:
        rdict = xmltodict.parse(response.content)['abstracts-retrieval-response']['references']
    except KeyError:
        continue
    mycursor.execute('UPDATE publications SET ref_count={n} WHERE eid={eid}'.format(n=int(rdict['@total-references']), eid=entry[0]))
    mydb.commit()
    if rdict['@total-references'] == '1':
        rdict['reference'] = [rdict['reference']]
    try:
        for ele in rdict['reference']:
            all_ids, data = add_record_additional(mydb, mycursor, all_ids, data, ele, entry, sql)
    except KeyboardInterrupt:
        with open('save.json', 'w', encoding='utf8') as json_file:
            dump(data, json_file, ensure_ascii=False)
        foo = mycursor.fetchall()  # Collect records to avoid raising errors
        for ele in rdict['reference']:
            all_ids, data = add_record_additional(mydb, mycursor, all_ids, data, ele, entry, sql)
        break
with open('save.json', 'w', encoding='utf8') as json_file:
    dump(data, json_file, ensure_ascii=False)
