#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 28 17:09:27 2023

@author: milasiunaite
"""

import mysql.connector
import requests
from json import load, dump, JSONDecodeError
from unidecode import unidecode
from html import unescape
from random import shuffle
import xmltodict
from mysql.connector.errors import DataError


def get_ids_to_correct(table):
    db_data = load(open('mydb_setup.json'))
    mydb = mysql.connector.connect(**db_data)
    mycursor = mydb.cursor()
    f = open('to_correct.txt', 'w')
    if table == 'publications':
        mycursor.execute('SELECT eid, author_count, authors FROM publications WHERE doi = "" and author_count != 0')
        data = mycursor.fetchall()
        for row in data:
            if row[1] != len(row[2].split(',')):
                f.write(f'{row[0]}, ')
    elif table == 'additional':
        mycursor.execute('SELECT id, authors FROM additional WHERE authors != ""')
        data = mycursor.fetchall()
        mycursor.execute('SELECT id FROM authors')
        author_set = set(mycursor.fetchall())
        for row in data:
            a_list = row[1].split(',')
            for authid in a_list:
                if (int(authid),) not in author_set:
                    f.write(f'{row[0]}, ')
                    break
    elif table == 'authors':
        mycursor.execute('SELECT id, authname, surname, initials FROM authors')
        data = mycursor.fetchall()
        for row in data:
            if row[3] is not None and row[3] != '' and row[1] != row[2] + ' ' + row[3]:
                f.write(f'{row[0]}, ')
    f.write('EOF')
    f.close()


def correct_reference(mydb, mycursor, scopus_id, author_set):
    scopus_id = int(scopus_id)
    data = load(open('save.json'))
    data_updated = False
    mycursor.execute(f'SELECT authors, referenced_by FROM additional WHERE id={scopus_id}')
    row = mycursor.fetchall()[0]
    citing = row[1].split(',')
    eid = f'2-s2.0-{int(citing[0])}'
    # Connect to SCOPUS
    headers = requests.utils.default_headers()
    data_head = load(open('headers.json'))
    for head in data_head:
        headers[head] = data_head[head]
    api = 'https://api.elsevier.com/content/abstract/eid/{eid}?view=REF'
    response = requests.get(api.format(eid=eid), headers=headers)
    try:
        result_dict = xmltodict.parse(response.content)['abstracts-retrieval-response']['references']
    except KeyError:
        return author_set
    if result_dict['@total-references'] == '1':
        result_dict['reference'] = [result_dict['reference']]
    r_list = result_dict['reference']
    ids = [int(ele['scopus-id']) for ele in r_list]
    try:
        i = ids.index(scopus_id)
    except ValueError:
        print('Reference not found')
        return author_set
    a_list = r_list[i]['author-list']
    if a_list is None:
        print('No authors')
        return author_set
    else:
        a_list = a_list['author']
    try:
        author_string = ','.join([a['@auid'] for a in a_list])
    except TypeError:
        if type(a_list) is dict:
            author_string = a_list['@auid']
            a_list = [a_list]
        else:
            print(a_list)
            raise Exception('TypeError')
    except KeyError:
        for a in a_list:
            if '@auid' not in a:
                a['@auid'] = str(data['auth_my'])
                data['auth_my'] += 1
        data_updated = True
        author_string = ','.join([a['@auid'] for a in a_list])
    values_to_insert = []
    for author_info in a_list:
        authid = int(author_info['@auid'])
        if (authid,) not in author_set:
            author_set.add((int(authid),))
            if ('affiliation' in author_info and author_info['affiliation'] is not None and '@id' in author_info['affiliation']):
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
                 author_info['ce:surname'], first_name, initials, afid, url))
    if len(values_to_insert) > 0:
        mycursor.executemany(
            'INSERT INTO authors (id, authname, surname, given_name, initials, afids, url) VALUES (%s, %s, %s, %s, %s, %s, %s)',
            values_to_insert)
        mydb.commit()
    if set(row[0].split(',')) != set(author_string.split(',')):
        try:
            mycursor.execute(f'UPDATE additional SET authors="{author_string}" WHERE id={scopus_id}')
            mydb.commit()
        except DataError:
            print(f'Author string too long: {scopus_id}')
    if data_updated:
        with open('save.json', 'w', encoding='utf8') as json_file:
            dump(data, json_file, ensure_ascii=False)
    return author_set


def correct_record(mydb, mycursor, scopus_id, author_set, table='publications'):
    # Connect to SCOPUS
    headers = requests.utils.default_headers()
    data = load(open('headers.json'))
    for head in data:
        headers[head] = data[head]
    eid = f'2-s2.0-{scopus_id}'
    api = 'https://api.elsevier.com/content/abstract/eid/{eid}?view=META_ABS'
    response = requests.get(api.format(eid=eid), headers=headers)
    try:
        a_list = xmltodict.parse(response.content)['abstracts-retrieval-response']['authors']
        if a_list is None:
            print('No authors')
            return author_set
        else:
            a_list = a_list['author']
    except KeyError:
        print(xmltodict.parse(response.content))
        return author_set
    try:
        author_string = ','.join([a['@auid'] for a in a_list])
    except TypeError:
        if type(a_list) is dict:
            author_string = a_list['@auid']
            a_list = [a_list]
        else:
            print(a_list)
            raise Exception('TypeError')
    values_to_insert = []
    for author_info in a_list:
        authid = int(author_info['@auid'])
        if (authid,) not in author_set:
            author_set.add((int(authid),))
            if ('affiliation' in author_info and author_info['affiliation'] is not None and '@id' in author_info['affiliation']):
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
                 author_info['ce:surname'], first_name, initials, afid, url))
    if len(values_to_insert) > 0:
        mycursor.executemany(
            'INSERT INTO authors (id, authname, surname, given_name, initials, afids, url) VALUES (%s, %s, %s, %s, %s, %s, %s)',
            values_to_insert)
        mydb.commit()
    if table == 'publications':
        mycursor.execute(f'SELECT authors FROM publications WHERE eid={scopus_id}')
        authors = mycursor.fetchall()[0][0]
        if set(authors.split(',')) != set(author_string.split(',')):
            try:
                mycursor.execute(f'UPDATE publications SET authors="{author_string}" WHERE eid={scopus_id}')
                mydb.commit()
            except DataError:
                print(f'Author string too long: {scopus_id}')
    elif table == 'additional':
        mycursor.execute(f'SELECT authors FROM additional WHERE id={scopus_id}')
        authors = mycursor.fetchall()[0][0]
        if set(authors.split(',')) != set(author_string.split(',')):
            try:
                mycursor.execute(f'UPDATE additional SET authors="{author_string}" WHERE id={scopus_id}')
                mydb.commit()
            except DataError:
                print(f'Author string too long: {scopus_id}')
    return author_set


def correct_from_file():
    db_data = load(open('mydb_setup.json'))
    mydb = mysql.connector.connect(**db_data)
    mycursor = mydb.cursor()
    mycursor.execute('SELECT id FROM authors')
    author_set = set(mycursor.fetchall())
    f = open('to_correct_a.txt', 'r')  # Check the file name
    file = f.readlines()
    ids = file[0].split(', ')
    ids.pop(-1)  # Remove End Of File string
    shuffle(ids)
    for eid in ids:
        # author_set = correct_record(mydb, mycursor, eid, author_set, table='additional')  # Check table name
        author_set = correct_reference(mydb, mycursor, eid, author_set)
        print(f'Corrected {eid}')


def remove_faulty_edges(eid):
    eid = str(eid)
    db_data = load(open('mydb_setup.json'))
    mydb = mysql.connector.connect(**db_data)
    mycursor = mydb.cursor()
    conditions = [f'referenced_by LIKE "%,{eid},%"',
                  f'referenced_by LIKE "%,{eid}"',
                  f'referenced_by LIKE "{eid},%"']
    mycursor.execute(f'DELETE FROM additional WHERE referenced_by="{eid}"')
    mydb.commit()
    for i in range(3):
        mycursor.execute(
            f'SELECT id, referenced_by FROM additional WHERE {conditions[i]}')
        records = mycursor.fetchall()
        for rec in records:
            if i == 0:
                updated_str = rec[1].replace(f',{eid},', ',')
            elif i == 1:
                updated_str = rec[1][:-(len(eid)+1)]
            else:
                updated_str = rec[1][(len(eid)+1):]
            mycursor.execute(
                f'UPDATE additional SET referenced_by="{updated_str}" WHERE id={rec[0]}')
            mydb.commit()


def relabel_indatabase(old, new):
    """
    Relabel the algorithm in the database for all affected records.

    Example: we want to replace old label 'PS' with 'PSO'.
    Then the field string 'CS,PS' becomes 'CS,PSO'.

    Parameters
    ----------
    old : string
        Old label.
    new : string
        New label.

    """
    db_data = load(open('mydb_setup.json'))
    mydb = mysql.connector.connect(**db_data)
    mycursor = mydb.cursor()
    mycursor.execute(
        f'SELECT eid, field FROM publications WHERE field LIKE "%{old}%"')
    data = mycursor.fetchall()
    for ele in data:  # Update each record.
        fields = ele[1].replace(old, new)
        mycursor.execute(
            f'UPDATE publications SET field="{fields}" WHERE eid={ele[0]}')
        mydb.commit()


def rename_dict(labels):
    """
    Generate dictionary to relate old and new labels.

    Example
    -------
        Argument labels=['HS','CS'] eturns {'HS': 'A1', 'CS': 'A2'}.

    Parameters
    ----------
    labels : list
        List of current labels.

    Returns
    -------
    renamed : dict
        Dictionary where old labels - keys, new labels - corresponding values.

    """
    renamed = dict()
    for i in range(len(labels)):
        renamed[labels[i]] = 'A' + str(i)
    return renamed


def normalize(text):
    """Return string in lower case and with non-standard letters removed."""
    norm = text.casefold()
    norm = norm.replace('&amp;', '&')  # Replace HTML entity '&amp;' with '&'
    norm = unescape(norm)
    norm = unidecode(norm, 'ignore')
    norm = norm.replace('.-', '.')  # For normalizing initials
    replacements = {
        ('/', ''), ('?', ''), ('&', ''), ('-', ' '), ('"', ''), ("'", ""),
        ('!', ''), ('@', ''), ('#', ''), ('$', ''), ('^', ''), ('\\', ' '),
        ('*', ''), ('=', ''), ('`', ''), (':', ''), (';', ''), ('|', ''),
        ('~', ''), ('±', ''), ('{', ''), ('}', ''), ('[', ' '), (']', ''),
        ('   ', ' '), ('  ', ' ')
    }
    for old, new in replacements:
        norm = norm.replace(old, new)
    return norm


def merge_records(mydb, mycursor, idp, ida):
    """
    Merge two identical records if their authors match.

    The function updates the ids (aliases) associated with relevant authors.
    Keeps the record in the publications table and deletes the other one.

    Parameters
    ----------
    mydb : database
        Connection to the database.
    mycursor : cursor
        Cursor connected to the database.
    idp : int
        The id of the record in the publications table.
    ida : int
        The id of the record in the additional table.

    Raises
    ------
    Exception
        Occurs if the authors of the records do not match.

    """
    mycursor.execute(f'SELECT authors, author_count FROM publications WHERE eid={idp}')
    res = mycursor.fetchall()[0]
    info_idp = {
        'author_string': res[0],
        'author_count': res[1]}
    mycursor.execute(f'SELECT authors, referenced_by FROM additional WHERE id={ida}')
    res = mycursor.fetchall()[0]
    info_ida = {
        'author_string': res[0],
        'citing_articles': res[1]}
    info_idp.update({'authors': dict()})
    info_ida.update({'authors': dict()})
    ids_idp = set(info_idp['author_string'].split(','))
    ids_ida = set(info_ida['author_string'].split(','))
    if ids_ida != ids_idp:
        # Get the information about the authors of publication with id idp.
        if ids_ida == {''} or ids_idp == {''}:
            return
        for authid in ids_idp:
            mycursor.execute(f'SELECT authname, given_name, aka FROM authors WHERE id={authid}')
            author_info = mycursor.fetchall()[0]  # (authname, given_name, aka)
            try:
                aka = author_info[2].split()
            except AttributeError:
                aka = []
            info_idp['authors'].update(
                {normalize(author_info[0]): (authid, author_info[1], aka)})
        # Get the information about the authors of publication with id ida.
        for authid in ids_ida:
            mycursor.execute(f'SELECT authname, given_name, aka FROM authors WHERE id={authid}')
            author_info = mycursor.fetchall()[0]  # (authname, given_name, aka)
            try:
                aka = author_info[2].split(',')
            except AttributeError:
                aka = []
            info_ida['authors'].update(
                {normalize(author_info[0]): (authid, author_info[1], aka)})
        names_ida = set(info_ida['authors'].keys())
        names_idp = set(info_idp['authors'].keys())
        if names_ida != names_idp:
            print(info_idp['authors'])
            print(info_ida['authors'])
            print('---------------------')
            return
        aliases = dict()  # Store ids with corresponding aliases.
        for name in names_idp:
            # Add & append ids in aliases if we match atuhor names.
            authid_ida = info_ida['authors'][name][0]
            authid_idp = info_idp['authors'][name][0]
            if authid_ida == authid_idp:
                continue
            else:
                if authid_ida not in info_idp['authors'][name][2]:
                    aliases[authid_idp] = [authid_ida] + \
                        info_idp['authors'][name][2]
                if authid_idp not in info_ida['authors'][name][2]:
                    aliases[authid_ida] = [authid_idp] + \
                        info_ida['authors'][name][2]
        # Merge authors.
        if len(aliases) != 0:
            for ala in aliases:
                mycursor.execute(f'UPDATE authors SET aka="{",".join(aliases[ala])}" WHERE id={ala}')
            mydb.commit()
    # Update the reference lists of articles that cite the duplicated record.
    for ref in info_ida['citing_articles'].split(','):
        mycursor.execute(f'SELECT cites FROM publications WHERE eid={ref}')
        referenced_articles = str(mycursor.fetchall()[0][0])
        if str(idp) in referenced_articles.split(','):
            continue
        elif referenced_articles.split(',')[0] == '':
            referenced_articles = str(idp)
        else:
            referenced_articles = f'{referenced_articles},{idp}'
        mycursor.execute(f'UPDATE publications SET cites="{referenced_articles}" WHERE eid={ref}')
        mydb.commit()
    # Remove the other record.
    mycursor.execute(f'DELETE FROM additional WHERE id={ida}')
    mydb.commit()
    print('Removed ' + str(ida))
    return


def merge_matching_doi():
    """
    Merge all records with identical doi from the two tables.

    For every doi in additional table, if it is also in the publications table,
    get associated scopus ids and titles. If titles match, merge records.
    """
    db_data = load(open('mydb_setup.json'))
    mydb = mysql.connector.connect(**db_data)
    mycursor = mydb.cursor()
    mycursor.execute('SELECT doi FROM additional')
    other_dois = set(mycursor.fetchall())
    mycursor.execute('SELECT doi FROM publications')
    publication_dois = set(mycursor.fetchall())
    overlap = list(publication_dois.intersection(other_dois))
    # Introduce randomness to avoid repeatedly going over the same records
    shuffle(overlap)
    for doi in overlap:
        mycursor.execute(
            f'SELECT eid, title from publications WHERE doi="{doi[0]}"')
        id_p, title_p = mycursor.fetchall()[0]
        mycursor.execute(
            f'SELECT id, title from additional WHERE doi="{doi[0]}"')
        id_a, title_a = mycursor.fetchall()[0]
        if normalize(title_p) == normalize(title_a) or title_a == '':
            merge_records(mydb, mycursor, id_p, id_a)
        else:
            print(title_p)
            print(title_a)
            print('-------------')


def merge_matching_title():
    """
    Merge all records with identical titles from the two tables.

    Only merges the records if one of the records has no associated doi.
    """
    db_data = load(open('mydb_setup.json'))
    mydb = mysql.connector.connect(**db_data)
    mycursor = mydb.cursor()
    mycursor.execute('SELECT title, date, source, id, doi FROM additional')
    info_other = mycursor.fetchall()
    mycursor.execute('SELECT title, date, source, eid FROM publications')
    info_publications = mycursor.fetchall()
    publication_titles, other_titles = dict(), dict()
    for ele in info_other:
        other_titles[normalize(ele[1])] = {
            'date': str(ele[1]), 'source': ele[2], 'id': ele[3], 'doi': ele[4]}
    for ele in info_publications:
        publication_titles[normalize(ele[1])] = {
            'date': str(ele[1]), 'source': ele[2], 'eid': ele[3]}
    overlap = list(set(publication_titles.keys()).intersection(set(other_titles.keys())))
    # Introduce randomness to avoid repeatedly going over the same records
    shuffle(overlap)
    for ele in overlap:
        if other_titles[ele]['doi'] == '':
            # Check if dates match.
            year_and_month_match = (other_titles[ele]['date'][:-3] == publication_titles[ele]['date'][:-3])
            if other_titles[ele]['date'] == 'None' or year_and_month_match:
                merge_records(mydb, mycursor, publication_titles[ele]['eid'],
                              other_titles[ele]['id'])
            else:
                print(ele)
                print(other_titles[ele])
                print(publication_titles[ele])
                print('----------------------')


def get_initials(given_name):
    """Return initials in one string for a given name."""
    initials = ''
    for ele in given_name.split(' '):
        name = ele.strip('- ')
        if name == '':
            continue
        if '-' in name:
            split = name.split('-')
            if split[0][-1] == '.':  # If first name ends with .
                initials = f'{initials}{split[0]}-'
            else:
                initials = f'{initials}{split[0][0]}.-'
            if split[1][-1] == '.':  # If second name ends with .
                initials = f'{initials}{split[1]}'
            else:
                initials = f'{initials}{split[1][0]}.'
        elif name[-1] == '.':  # If name ends with .
            initials = f'{initials}{name}'
        else:
            initials = f'{initials}{name[0]}.'
    return initials


def correct_author_names():
    db_data = load(open('mydb_setup.json'))
    mydb = mysql.connector.connect(**db_data)
    mycursor = mydb.cursor()
    mycursor.execute('SELECT id, authname, surname, given_name, initials FROM authors')
    data = mycursor.fetchall()
    shuffle(data)
    for row in data:
        update_string = ''
        if row[3] != '' and row[3] is not None:
            initials = get_initials(row[3])
            if row[4] != initials:
                update_string = f'initials = "{initials}"'
        elif row[4] is not None:
            initials = row[4]
        else:
            initials = ''
        authname = row[2] + ' ' + initials
        authname = authname.strip()
        if row[1] != authname:
            if update_string != '':
                update_string = f'{update_string}, authname = "{authname}"'
            else:
                update_string = f'authname = "{authname}"'
        if update_string != '':
            mycursor.execute(f'UPDATE authors SET {update_string} WHERE id={row[0]}')
            mydb.commit()
            print(f'Updated {row[0]}')


def correct_authors_from_file():
    db_data = load(open('mydb_setup.json'))
    mydb = mysql.connector.connect(**db_data)
    mycursor = mydb.cursor()
    f = open('to_correct.txt', 'r')
    file = f.readlines()
    ids = file[0].split(', ')
    ids.pop(-1)  # Remove End Of File string
    shuffle(ids)
    for authid in ids:
        update_string = ''
        mycursor.execute(f'SELECT authname, surname, given_name, initials FROM authors WHERE id={authid}')
        row = mycursor.fetchall()[0]
        if row[2] != '' and row[2] is not None:
            initials = get_initials(row[2])
            if row[3] != initials:
                update_string = f'initials = "{initials}"'
        elif row[3] is not None:
            initials = row[3]
        else:
            initials = ''
        authname = row[1] + ' ' + initials
        authname = authname.strip()
        if row[0] != authname:
            if update_string != '':
                update_string = f'{update_string}, authname = "{authname}"'
            else:
                update_string = f'authname = "{authname}"'
        if update_string != '':
            mycursor.execute(f'UPDATE authors SET {update_string} WHERE id={authid}')
            mydb.commit()
            print(f'Corrected {authid}')


def get_date(date_parts):
    """
    Generate a date string from a list.

    Examples
    --------
        ['2020', '12', '24'] -> '2020-12-24',
        ['2020', '12'] -> '2020-12-01',
        ['2020'] -> '2020-01-01'.

    Parameters
    ----------
    date_parts : list
        A list containing the year and/or month and/or day.

    Returns
    -------
    date : str
        Date string. Fixed 10 character length.

    """
    # Convert to string if date parts are numeric.
    if date_parts is None or date_parts[0] is None:
        return ''
    if sum(type(x) is str for x in date_parts) != len(date_parts):
        date_parts = [str(x) for x in date_parts]
    # Construct the date string.
    if len(date_parts) == 3:
        date = '-'.join(date_parts)
        if len(date) < 10:  # Zeros missing.
            if date[5] != '0' and date[6] == '-':
                date = f'{date[:5]}0{date[5:]}'
        if len(date) < 10:
            date = f'{date[:-1]}0{date[-1]}'
    elif len(date_parts) == 2:  # Day missing.
        date = f'{date_parts[0]}-{date_parts[1]}-01'
        if len(date) < 10:
            date = f'{date[:5]}0{date[5:]}'
    elif len(date_parts) == 1:
        date = f'{date_parts[0]}-01-01'
    else:
        date = ''
    return date


def title_metadata(title, authors, date=''):
    """
    Get the doi and other metadata based on the publication`s title.
    
    Get a list of 5 works that most closely match the given title (and date, 
    if given). Check if any of these works match the title exactly. If there is
    such a work, check if the authors also match. If so, return the additional
    information in a form of a dictionary.

    Parameters
    ----------
    title : str
        Title of the publication.
    authors : set
         Set of author names in the form of 'Surname N.'.
    date : str, optional
        Date of publication. The default is ''.

    Returns
    -------
    metadata : dict
        Information about the publication from crossref.

    """
    metadata = dict()
    norm = normalize(title)
    title_mod = title.replace('&', '')
    # Update the headers to get into the polite pool.
    headers = requests.utils.default_headers()
    headers['User-Agent'] = f"{headers['User-Agent']} mailto:ugnemmilasiunaite@gmail.com"
    query = f'{title_mod}, {", ".join(authors)}'
    if date == '':
        url = f'https://api.crossref.org/works?query.bibliographic="{query}"&rows=5'
    else:
        url = f'https://api.crossref.org/works?query.bibliographic="{query}, {date}"&rows=5'
    # Get a list of 5 best matching records based on title and/or date.
    try:
        response = requests.get(url, headers=headers).json()['message']['items']
    except TypeError:
        print(requests.get(url, headers=headers).json())
        raise Exception('ERROR')
    for i in range(5):
        try:
            title_resp = response[i]['title'][0]
        except (IndexError, KeyError):
            continue
        if normalize(title_resp) == norm:
            # Get info about the authors.
            metadata['authors'] = dict()
            try:
                for author in response[i]['author']:
                    try:
                        temp = {'surname': author['family']}
                    except KeyError:
                        temp = {'surname': ''}
                    try:
                        temp.update({'given_name': author['given'],
                                     'initials': get_initials(author['given'])})
                    except KeyError:
                        temp.update({'given_name': '',
                                     'initials': ''})
                    authname = f"{temp['surname']} {temp['initials']}".strip()
                    metadata['authors'].update({authname: temp})
                metadata['author_count'] = len(response[i]['author'])
            except KeyError:
                metadata.clear()
                continue
            # Check if authors match.
            norm_authors = set(normalize(auth) for auth in authors)
            norm_meta_authors = set(normalize(key) for key in metadata['authors'].keys())
            if norm_authors != norm_meta_authors:
                metadata.clear()
                break
            # Continue to next record if authors do not match.
            if len(metadata) == 0:
                continue
            metadata['doi'] = response[i]['DOI']
            try:
                metadata['ref_count'] = response[i]['reference-count']
            except KeyError:
                pass
            if 'reference' in response[i]:
                metadata['references'] = response[i]['reference']
            try:
                metadata['type'] = response[i]['type']
            except KeyError:
                pass
            metadata['citedby'] = response[i]['is-referenced-by-count']
            try:
                metadata['source'] = response[i]['container-title'] if type(response[i]['container-title']) is str else '; '.join(response[i]['container-title'])
            except KeyError:
                metadata['source'] = ''
            try:
                metadata['date'] = get_date(response[i]['published']['date-parts'][0])
            except KeyError:
                try:
                    metadata['date'] = get_date(response[i]['issued']['date-parts'][0])
                except KeyError:
                    try:
                        metadata['date'] = get_date(response[i]['published-online']['date-parts'][0])
                    except KeyError:
                        metadata['date'] = ''
            if 'abstract' in response[i]:
                metadata['abstract'] = response[i]['abstract']
                metadata['abstract'] = metadata['abstract'].replace('"', '`')
                metadata['abstract'] = metadata['abstract'].replace("'", '`')
            else:
                metadata['abstract'] = ''
            break
    return metadata


def doi_metadata(doi):
    """
    Get metadata from crossref via a doi for a particular publication.

    Parameters
    ----------
    doi : string
        DOI associated with the publication.

    Returns
    -------
    metadata : dictionary
        Dictionary containing the metadata for the given doi.

    """
    metadata = dict()
    # Update the headers to get into the polite pool.
    headers = requests.utils.default_headers()
    headers['User-Agent'] = f"{headers['User-Agent']} mailto:ugnemmilasiunaite@gmail.com"
    url = f'https://api.crossref.org/works/{doi}'
    try:
        response = requests.get(url, headers=headers).json()['message']
    except JSONDecodeError:
        return metadata
    # Get info about the authors.
    metadata['authors'] = dict()
    try:
        for author in response['author']:
            try:
                temp = {'surname': author['family']}
            except KeyError:
                temp = {'surname': ''}
            try:
                temp.update({'given_name': author['given'],
                             'initials': get_initials(author['given'])})
            except KeyError:
                temp.update({'given_name': '',
                             'initials': ''})
            authname = f"{temp['surname']} {temp['initials']}".strip()
            metadata['authors'].update({authname: temp})
        metadata['author_count'] = len(response['author'])
    except KeyError:
        metadata['author_count'] = 0
    if len(response['title']) == 0 or response['title'] is None:
        metadata['title'] = ''
    else:
        metadata['title'] = response['title'][0] if type(response['title']) is list else response['title']
    metadata['ref_count'] = response['reference-count']
    try:
        metadata['references'] = response['reference']
    except KeyError:
        metadata['references'] = []
    metadata['type'] = response['type']
    try:
        metadata['date'] = get_date(response['published']['date-parts'][0])
    except KeyError:
        try:
            metadata['date'] = get_date(response['issued']['date-parts'][0])
        except KeyError:
            try:
                metadata['date'] = get_date(response['published-online']['date-parts'][0])
            except KeyError:
                metadata['date'] = ''
    # Check if there is an abstract.
    if 'abstract' in response:
        metadata['abstract'] = response['abstract']
        metadata['abstract'] = metadata['abstract'].replace('"', '`')
        metadata['abstract'] = metadata['abstract'].replace("'", '`')
    else:
        metadata['abstract'] = ''
    metadata['citedby'] = response['is-referenced-by-count']
    metadata['source'] = response['container-title'] if type(response['container-title']) is str else '; '.join(response['container-title'])
    return metadata


def get_update_string(metadata, row, match_type):
    """
    Generate string for updating records in the database.

    Parameters
    ----------
    metadata : dict
        Information from crossref.
    row : tuple
        Information on the record from the dataabse.
    match_type : string
        Information used to match on crossref. Either 'doi' or 'title'.

    Returns
    -------
    string : string
        The string to update the records.

    """
    if match_type == 'doi':
        if row[0] == '':
            string = f'title="{metadata["title"]}"'
        else:
            string = ''
    elif match_type == 'title':
        string = f'doi="{metadata["doi"]}"'
    # Check if need to update the date.
    if metadata['date'] != '':
        if str(row[2]) == '' or row[2] is None:
            string = f'{string}, date="{metadata["date"]}"'
        elif metadata['date'] != str(row[2]):
            # Check if years match.
            if metadata['date'][:4] == str(row[2])[:4]:
                # Check if months match.
                if metadata['date'][5:7] == str(row[2])[5:7]:
                    if metadata['date'][-2:] != '01':
                        string = f'{string}, date="{metadata["date"]}"'
                elif int(metadata['date'][5:7]) < int(str(row[2])[5:7]):
                    if metadata['date'][5:] != '01-01':
                        # Keep the earlier date if months diverge.
                        string = f'{string}, date="{metadata["date"]}"'
            elif int(metadata['date'][:4]) < int(str(row[2])[:4]):
                # Keep the earlier date if years diverge.
                string = f'{string}, date="{metadata["date"]}"'
    # Check if need to update citedby count.
    if int(metadata['citedby']) > int(row[3]):
        string = f'{string}, citedby={metadata["citedby"]}'
    # Check if need to update the source.
    if metadata['source'] is not None and metadata['source'] != '':
        if row[5] == '' or row[5] is None:
            string = f'{string}, source="{metadata["source"]}"'
    # Check if need to update abstract.
    if len(row) == 8 and row[7] == '' and metadata['abstract'] != '':
        string = f'{string}, abstract="{metadata["abstract"]}"'
    return string.strip(', ')


def fill_missing(table, match_type):
    """
    Fill missing info for records in additional table with a doi.

    We select records from additional table without a title but with a doi.
    Get additional information from crossref based on the doi; edit if needed.
    Insert the additional information into the table.

    Parameters
    ----------
    table : string
    Name of table where the record is located.

    match_type : string
    Information used to match on crossref. Either 'doi' or 'title'.

    """
    db_data = load(open('mydb_setup.json'))
    mydb = mysql.connector.connect(**db_data)
    mycursor = mydb.cursor()
    mycursor.execute('SELECT id FROM authors')
    author_set = set(mycursor.fetchall())
    if match_type == 'doi':
        if table == 'additional':
            mycursor.execute('SELECT title, doi, date, citedby, authors, '
                             'source, id FROM additional WHERE doi != "" and '
                             'authors = ""')
        elif table == 'publications':
            mycursor.execute('SELECT title, doi, date, citedby, authors, '
                             'source, eid, abstract FROM publications WHERE '
                             'doi != "" and (authors = "" or abstract = "")')
        else:
            raise Exception('Please choose a different table.')
    elif match_type == 'title':
        if table == 'publications':
            mycursor.execute('SELECT title, doi, date, citedby, authors, '
                             'source, eid, abstract FROM publications WHERE '
                             'doi = "" and authors != "" and title != ""')
        elif table == 'additional':
            mycursor.execute('SELECT title, doi, date, citedby, authors,'
                             ' source, id FROM additional WHERE title is NOT'
                             ' NULL and authors != "" and doi = ""')
        else:
            raise Exception('Please choose a different table.')
    else:
        raise Exception('Please choose a different match_type.')
    records = mycursor.fetchall()
    # Introduce randomness to avoid repeatedly going over the same records
    shuffle(records)
    for row in records:
        # Get metadata.
        if match_type == 'doi':
            metadata = doi_metadata(row[1])
        else:
            if row[2] is None:
                date = ''
            else:
                date = row[2]
            authors = set()
            for ele in row[4].split(','):
                mycursor.execute(f'SELECT authname from authors where id={ele}')
                try:
                    authors.add(mycursor.fetchall()[0][0])
                except IndexError:
                    if table == 'publications':
                        author_set = correct_record(mydb, mycursor, row[6], author_set)
                        print(f'Corrected {row[6]}')
                    else:
                        print(f'A{ele} in {row[6]}')
                    authors = set()
                    break
            if len(authors) != 0:
                metadata = title_metadata(row[0], authors, date=date)
            else:
                metadata = {}
        if len(metadata) == 0:
            continue
        # Prepare information to update via making a string.
        string = get_update_string(metadata, row, match_type)
        # Check if need to update authors.
        if row[4] == '' and len(metadata['authors']) != 0:
            data = load(open('save.json'))
            authors, values_to_insert = [], []
            for author in metadata['authors']:
                author_info = metadata['authors'][author]
                authid = data['auth_my']
                data['auth_my'] += 1
                authors.append(str(authid))
                values_to_insert.append(
                    (authid, author, author_info['surname'],
                     author_info['given_name'], author_info['initials']))
            if len(authors) != 0:
                mycursor.executemany('INSERT INTO authors (id, authname,'
                                     ' surname, given_name, initials) VALUES '
                                     '(%s, %s, %s, %s, %s)', values_to_insert)
                mydb.commit()
                with open('save.json', 'w', encoding='utf8') as json_file:
                    dump(data, json_file, ensure_ascii=False)
                authors = ','.join(authors)
                if string != '':
                    string = f'{string}, authors="{authors}"'
                else:
                    string = f'authors="{authors}"'
        # Update the record.
        if len(string) != 0:
            if table == 'additional':
                mycursor.execute(f'UPDATE additional SET {string} WHERE id={row[6]}')
            else:
                mycursor.execute(f'UPDATE publications SET {string} WHERE eid={row[6]}')
            mydb.commit()
            print(f'Updated {row[6]}')


def main():
    return


if __name__ == '__main__' or __name__ == 'builtins':
    main()
