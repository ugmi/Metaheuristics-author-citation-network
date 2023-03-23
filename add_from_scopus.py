#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 23 21:08:14 2023

@author: milasiunaite
"""

import requests
import mysql.connector
import json
from unidecode import unidecode, UnidecodeError


def get_keywords():
    keyword_to_abbr = dict()
    f = open('list-of-labels.txt', 'r')
    line = f.readline()
    while line != '':
        ln = line.split(':')
        keyword_to_abbr[ln[0][:-1]] = ln[1][1:-1]
        line = f.readline()
    f.close()
    return keyword_to_abbr


def normalize(text):
    norm = text.casefold()
    norm = unidecode(norm, 'ignore')
    norm = norm.replace('optimis', 'optimiz')
    norm = norm.replace('-', ' ')
    return norm


def field(ele, data, keyword, kw):
    try:
        abstract = ele['dc:description']
    except KeyError:
        abstract = ''
    try:
        title = ele['dc:title']
    except KeyError:
        title = ''
    ntitle = normalize(title)
    if ntitle.find(keyword) != -1:
        label = kw
        if ele['eid'] not in data['eids']:
            data['eids'].append(ele['eid'])
    else:
        norm_abs = normalize(abstract)
        if norm_abs.find(keyword) != -1:
            label = kw
            if ele['eid'] not in data['eids']:
                data['eids'].append(ele['eid'])
        else:
            label = 'OTHER'
    return label, data


def values_to_insert(ele, label, authors, affiliations, cites=''):
    try:
        abstract = ele['dc:description']
    except KeyError:
        abstract = ''
    try:
        title = ele['dc:title']
    except KeyError:
        title = ''
    try:
        issue = ele['prism:issueIdentifier']
    except KeyError:
        issue = ''
    try:
        authkeywords = ele['authkeywords']
    except KeyError:
        authkeywords = ''
    try:
        issn = ele['prism:issn']
    except KeyError:
        issn = ''
    try:
        volume = ele['prism:volume']
    except KeyError:
        volume = ''
    try:
        doi = ele['prism:doi']
    except KeyError:
        doi = ''
    try:
        source = ele['prism:publicationName']
    except KeyError:
        source = ''
    try:
        source_id = ele['source-id']
    except KeyError:
        source_id = 0
    val = (ele['eid'][7:], title, source, issn, volume, issue,
           ele['prism:coverDate'], doi, abstract, ele['citedby-count'],
           ','.join(affiliations), ele['subtypeDescription'],
           ele['author-count']['@total'], ','.join(authors), authkeywords,
           source_id, ele['prism:url'], label, cites)
    return val


def add_record(mydb, mycursor, all_ids, data, ele, sql, keyword, kw, eid=''):
    ele_eid = int(ele['eid'][7:])
    if (ele_eid,) not in all_ids['p']:  # Add new record to table publications
        data['newlyadded'] = data['newlyadded'] + 1
        all_ids['p'].add((int(ele['eid'][7:]),))
        affiliations, val, authors = [], [], []
        try:
            for a in ele['affiliation']:
                afid = a['afid']
                affiliations.append(afid)
                if (int(afid),) not in all_ids['aff']:
                    all_ids['aff'].add((int(afid),))
                    val.append((afid, a['affilname'], a['affiliation-city'],
                                a['affiliation-country'], a['affiliation-url']))
            if len(val) > 0:
                mycursor.executemany(sql['v'], val)
                mydb.commit()
        except KeyError:
            pass  # No info on affiliations
        val = []
        try:
            for a in ele['author']:
                authid = a['authid']
                if authid == '':
                    authid = data['auth_my']
                    data['auth_my'] = data['auth_my'] + 1
                authors.append(authid)
                if (int(authid),) not in all_ids['a']:
                    all_ids['a'].add((int(authid),))
                    try:
                        afids = ','.join([e['$'] for e in a['afid']])
                    except KeyError:
                        afids = ''  # No affiliation ids given
                    val.append((authid, a['authname'], a['surname'],
                                a['given-name'], a['initials'], afids,
                                a['author-url']))
            if len(val) > 0:
                mycursor.executemany(sql['a'], val)
                mydb.commit()
        except KeyError:
            pass  # No info on authors
        label, data = field(ele, data, keyword, kw)
        if eid == '':  # Insert document from field of interest
            mycursor.execute(sql['p'], values_to_insert(
                ele, label, authors, affiliations))
        else:  # Insert document that cites a publication in the field
            mycursor.execute(sql['p'], values_to_insert(
                ele, label, authors, affiliations, cites=str(int(eid[7:]))))
        mydb.commit()
        # If true, remove from additional table
        if (ele_eid,) in all_ids['add']:
            # Get publications that cite the given article
            mycursor.execute(
                'SELECT referenced_by FROM additional WHERE id={eid}'.format(eid=ele_eid))
            refs = str(mycursor.fetchall()[0][0])
            for ref in refs.split(','):
                mycursor.execute(
                    'SELECT cites FROM publications WHERE eid={eid}'.format(eid=ref))
                cit = str(mycursor.fetchall()[0][0])
                if str(ele_eid) in cit.split(','):
                    continue
                elif cit != '':
                    cit = cit + ',' + str(ele_eid)
                else:
                    cit = str(ele_eid)
                mycursor.execute('UPDATE publications SET cites="{cites}" WHERE eid={eid}'.format(
                    cites=cit, eid=ref))
            # Remove from additional table
            all_ids['add'].discard((ele_eid,))
            mycursor.execute(
                'DELETE FROM additional WHERE id={eid}'.format(eid=ele_eid))
            mydb.commit()
    elif eid == '':  # Update label for article that's already in the table
        data['indatabase'] = data['indatabase'] + 1
        mycursor.execute(
            'SELECT field FROM publications WHERE eid={eid}'.format(eid=ele_eid))
        label = str(mycursor.fetchall()[0][0])
        if kw in label.split(','):
            return all_ids, data
        if label == 'OTHER':
            label = kw
            if str(ele['eid']) not in data['eids']:
                data['eids'].append(str(ele['eid']))
        else:
            label = label + ',' + kw
        mycursor.execute('UPDATE publications SET field="{label}" WHERE eid={eid}'.format(
            label=label, eid=ele_eid))
        mydb.commit()
    return all_ids, data


def main():
    # Set up a connection to the local database
    mydb = mysql.connector.connect(
        host='localhost',
        user='root',
        database='trial',
        password='RunTheNum6!',
        auth_plugin='mysql_native_password',
    )
    mycursor = mydb.cursor()
    # Configure headers, set api key and string
    headers = requests.utils.default_headers()
    headers['X-ELS-APIkey'] = 'd6fce2f6c18155e6666a768000ae3280'
    # Read data from file
    data = json.load(open('save.json'))
    # Collect ids
    all_ids = dict()
    mycursor.execute('SELECT eid FROM publications')
    all_ids['p'] = set(mycursor.fetchall())
    mycursor.execute('SELECT id FROM affiliations')
    all_ids['aff'] = set(mycursor.fetchall())
    mycursor.execute('SELECT id FROM authors')
    all_ids['a'] = set(mycursor.fetchall())
    mycursor.execute('SELECT id FROM additional')
    all_ids['add'] = set(mycursor.fetchall())
    sql = {'a': ('INSERT INTO authors (id, authname, surname, given_name,'
                 ' initials, afids, url) VALUES (%s, %s, %s, %s, %s, %s, %s)'),
           'v': ('INSERT INTO affiliations (id, name, city, country, url)'
                 ' VALUES (%s,%s, %s, %s, %s)'),
           'p': ('INSERT INTO publications (eid, title, source, issn, volume,'
                 ' issue, date, doi, abstract, citedby, affiliation, type,'
                 ' author_count, authors, author_keywords, source_id, url,'
                 ' field, cites) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,'
                 ' %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)')}
    keywords_abbr = get_keywords()
    for ele in keywords_abbr.copy():
        keyword = ele
        kw = keywords_abbr[ele]
        while data['limit'] > 50:
            try:
                if data['api'] == '':
                    data['api'] = 'http://api.elsevier.com/content/search/scopus?query=TITLE("{keyword}")%20OR%20ABS("{keyword}")&cursor=*&view=COMPLETE'
                    cursor = '*'
                else:
                    x = data['api'].find('cursor')
                    y = data['api'].find('&view')
                    cursor = data['api'][x+7:y]
                    cursor = cursor.replace('%3D', '=')
                # Get documents from SCOPUS that match the specified keyword
                if len(data['eids']) == 0:
                    response = requests.get(data['api'].format(
                        keyword=keyword), headers=headers)
                    data['limit'] = int(
                        response.headers['X-RateLimit-Remaining'])
                    # Convert response object to json, which is easier to work with
                    response = response.json()['search-results']
                    if response['cursor']['@current'] == response['cursor']['@next']:
                        # The above condition is true if we reached the end of result set
                        cursor = '*'
                        data['api'] = ''
                        break
                    try:
                        cursor = response['cursor']['@next']
                        for link in response['link']:
                            if link['@ref'] == 'next':
                                data['api'] = link['@href']
                        try:
                            for ele in response['entry']:
                                all_ids, data = add_record(
                                    mydb, mycursor, all_ids, data, ele, sql, keyword, kw)
                        except KeyError:
                            print(response)
                            continue
                    except KeyboardInterrupt:
                        cursor = response['cursor']['@next']
                        for link in response['link']:
                            if link['@ref'] == 'next':
                                data['api'] = link['@href']
                        with open('save.json', 'w', encoding='utf8') as json_file:
                            json.dump(data, json_file, ensure_ascii=False)
                        foo = mycursor.fetchall()  # Collect records to avoid raising errors
                        for ele in response['entry']:
                            all_ids, data = add_record(
                                mydb, mycursor, all_ids, data, ele, sql, keyword, kw)
                        return
                else:  # Get the citing articles for the publications in the queue
                    api = 'http://api.elsevier.com/content/search/scopus?query=refeid({eid})&cursor={cursor}&view=COMPLETE&sort=citedby-count'
                    while data['limit'] > 10 and len(data['eids']) > 0:
                        eid = data['eids'][0]
                        response = requests.get(api.format(
                            eid=eid, cursor=data['cursor']), headers=headers)
                        data['limit'] = int(
                            response.headers['X-RateLimit-Remaining'])
                        try:
                            response = response.json()['search-results']
                        except KeyError:
                            data['cursor'] = '*'
                            continue
                        if data['cursor'] == response['cursor']['@next']:
                            data['cursor'] = '*'
                            data['eids'].remove(eid)
                            continue
                        try:
                            data['cursor'] = response['cursor']['@next']
                            for ele in response['entry']:
                                ele_eid = int(ele['eid'][7:])
                                if (ele_eid,) in all_ids['p']:
                                    data['indatabase'] = data['indatabase'] + 1
                                    mycursor.execute(
                                        'SELECT cites from publications WHERE eid={eid}'.format(eid=ele_eid))
                                    cit = str(mycursor.fetchall()[0][0])
                                    if str(int(eid[7:])) in cit.split(','):
                                        continue
                                    if cit == '':
                                        cit = str(int(eid[7:]))
                                    else:
                                        cit = cit + ',' + str(int(eid[7:]))
                                    mycursor.execute('UPDATE publications SET cites="{cites}" WHERE eid={eid}'.format(
                                        cites=cit, eid=ele_eid))
                                    mydb.commit()
                                    continue
                                all_ids, data = add_record(
                                    mydb, mycursor, all_ids, data, ele, sql, keyword, kw, eid=eid)
                        except KeyboardInterrupt:
                            foo = mycursor.fetchall()  # Collect records to avoid raising errors
                            data['cursor'] = '*'
                            with open('save.json', 'w', encoding='utf8') as json_file:
                                json.dump(data, json_file, ensure_ascii=False)
                            data['cursor'] = response['cursor']['@next']
                            for ele in response['entry']:
                                ele_eid = int(ele['eid'][7:])
                                if (ele_eid,) in all_ids['p']:
                                    data['indatabase'] = data['indatabase'] + 1
                                    mycursor.execute(
                                        'SELECT cites from publications WHERE eid={eid}'.format(eid=ele_eid))
                                    cit = str(mycursor.fetchall()[0][0])
                                    if str(int(eid[7:])) in cit.split(','):
                                        continue
                                    if cit == '':
                                        cit = str(int(eid[7:]))
                                    else:
                                        cit = cit + ',' + str(int(eid[7:]))
                                    mycursor.execute('UPDATE publications SET cites="{cites}" WHERE eid={eid}'.format(
                                        cites=cit, eid=ele_eid))
                                    mydb.commit()
                                    continue
                                all_ids, data = add_record(
                                    mydb, mycursor, all_ids, data, ele, sql, keyword, kw, eid=eid)
                            return
            except KeyboardInterrupt:
                with open('save.json', 'w', encoding='utf8') as json_file:
                    json.dump(data, json_file, ensure_ascii=False)
                return  # If we haven't fetched the records from SCOPUS yet
        f = open('added_keywords.txt', 'a')
        f.write('\n' + keyword + ' : ' + kw)
        f.close()
        keywords_abbr.pop(keyword)
        f = open('list-of-labels.txt', 'w')
        for key in keywords_abbr:
            f.write(key + ' : ' + keywords_abbr[key] + '\n')
        f.close()
    with open('save.json', 'w', encoding='utf8') as json_file:
        json.dump(data, json_file, ensure_ascii=False)

main()
