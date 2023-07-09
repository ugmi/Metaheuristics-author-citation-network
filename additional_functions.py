#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 28 17:09:27 2023

@author: milasiunaite
"""

import mysql.connector
import requests
from json import load, dump
from unidecode import unidecode


def relabel_indatabase(prev, current):
    """
    Relabel the algorithm in the database for all affected records.

    Parameters
    ----------
    prev : string
        Old label.
    current : string
        New label.

    Returns
    -------
    None.

    """
    db_data = load(open('mydb_setup.json'))
    mydb = mysql.connector.connect(**db_data)
    mycursor = mydb.cursor()
    mycursor.execute(
        'SELECT eid, field FROM publications WHERE field LIKE "%' + prev + '"')
    data = mycursor.fetchall()
    for ele in data:  # Update each record.
        fields = ele[1].replace(prev, current)
        mycursor.execute('UPDATE publications SET field="{string}" WHERE eid={eid}'.format(string=fields, eid=ele[0]))
        mydb.commit()


def rename_dict(labels):
    """
    Generate dictionary to relate old and new labels.

    Parameters
    ----------
    labels : list
        List of current labels.

    Returns
    -------
    renamed : dict
        Dictionary where old labels are keys and new labels the corresponding values.

    """
    renamed = dict()
    for i in range(len(labels)):
        renamed[labels[i]] = 'A' + str(i)
    return renamed


def normalize(text):
    """
    Generalize the text by removing any non-standard letters or signs.

    Parameters
    ----------
    text : string
        Text to generalize.

    Returns
    -------
    norm : string
        Generalized text.

    """
    norm = text.casefold()
    norm = unidecode(norm, 'ignore')
    replacements = {
        ('/', ''), ('?', ''), ('&', ''), ('-', ' '), ('"', ''), ("'", ""),
        ('!', ''), ('@', ''), ('#', ''), ('$', ''), ('^', ''), ('*', ''),
        ('=', ''), ('`', ''), (':', ''), (';', ''), ('|', ''), ('~', ''),
        ('±', ''), ('{', ''), ('}', ''), ('[', ' '), (']', ''), ('\\', ' '),
        ('   ', ' '), ('  ', ' ')
        }
    for old, new in replacements:
        norm = norm.replace(old, new)
    return norm


def merge_records(mydb, mycursor, idp, ida):
    """
    Merge two identical records if their authors match.

    Also updates the ids associated with relevant authors.

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

    Returns
    -------
    None.

    """
    mycursor.execute('SELECT authors, author_count FROM publications WHERE eid={eid}'.format(eid=idp))
    info_idp = list(mycursor.fetchall()[0])
    mycursor.execute('SELECT authors, referenced_by FROM additional WHERE id={eid}'.format(eid=ida))
    info_ida = list(mycursor.fetchall()[0])
    # Get the information about the authors of publication with id idp.
    info_idp.append(dict())
    for auth in info_idp[0].split(','):
        mycursor.execute('SELECT authname, given_name, aka FROM authors WHERE id={aid}'.format(aid=int(auth)))
        info = mycursor.fetchall()[0]
        try:
            aka = info[2].split()
        except AttributeError:
            aka = []
        info_idp[2].update(
            {normalize(info[0]): (auth, info[1], aka)})
    # Get the information about the authors of publication with id ida.
    info_ida.append(dict())
    for auth in info_ida[0].split(','):
        mycursor.execute('SELECT authname, given_name, aka FROM authors WHERE id={aid}'.format(aid=int(auth)))
        info = mycursor.fetchall()[0]
        try:
            aka = info[2].split(',')
        except AttributeError:
            aka = []
        info_ida[2].update(
            {normalize(info[0]): (auth, info[1], aka)})
    del info, auth, aka
    aliases = dict()  # Store ids with corresponding aliases.
    for name in info_ida[2]:
        if name in info_idp[2]:
            # Check if not already present in the dictionary.
            if info_ida[2][name][0] in aliases:
                if info_idp[2][name][0] not in aliases[info_ida[name][0]]:
                    aliases[info_ida[2][name][0]].append(info_idp[2][name][0])
            else:
                aliases[info_ida[2][name][0]] = [info_idp[2][name][0]] + info_ida[2][name][2]
            if info_idp[2][name][0] in aliases:
                if info_ida[2][name][0] not in aliases[info_idp[name][0]]:
                    aliases[info_idp[2][name][0]].append(info_ida[2][name][0])
            else:
                aliases[info_idp[2][name][0]] = [info_ida[2][name][0]] + info_idp[2][name][2]
    # Check if matches for all authors had been found.
    if len(aliases) != len(info_idp[2])*2:
        print(info_idp[2])
        print(info_ida[2])
        raise Exception('Authors don`t match')
    # Merge authors.
    for ala in aliases:
        mycursor.execute('UPDATE authors SET aka="{aka}" WHERE id={aid}'.format(aka=','.join(aliases[ala]), aid=int(ala)))
    mydb.commit()
    # Update related records.
    for ref in info_ida[1].split(','):
        mycursor.execute('SELECT cites FROM publications WHERE eid={eid}'.format(eid=int(ref)))
        cit = str(mycursor.fetchall()[0][0])
        if str(idp) in cit.split(','):
            continue
        elif cit.split(',')[0] == '':
            cit = str(idp)
        else:
            cit = cit + ',' + str(idp)
        mycursor.execute('UPDATE publications SET cites="{cites}" WHERE eid={eid}'.format(cites=cit, eid=int(ref)))
        mydb.commit()
    # Remove the other record.
    mycursor.execute(
        'DELETE FROM additional WHERE id={eid}'.format(eid=ida))
    mydb.commit()


def merge_matching_doi():
    """
    Merge all records with identical doi from the two tables.

    Returns
    -------
    None.

    """
    db_data = load(open('mydb_setup.json'))
    mydb = mysql.connector.connect(**db_data)
    mycursor = mydb.cursor()
    mycursor.execute('SELECT doi FROM additional')
    additional = mycursor.fetchall()
    mycursor.execute('SELECT doi FROM publications')
    publications = mycursor.fetchall()
    for doi in additional:
        if doi in publications:
            mycursor.execute('SELECT eid from publications WHERE doi="{s}"'.format(s=doi[0]))
            idp = mycursor.fetchall()[0][0]
            mycursor.execute('SELECT id from additional WHERE doi="{s}"'.format(s=doi[0]))
            ida = mycursor.fetchall()[0][0]
            merge_records(mydb, mycursor, idp, ida)


def merge_matching_title():
    """
    Merge all records with identical titles from the two tables.

    Only merges the records if one of the records has no associated doi.

    Returns
    -------
    None.

    """
    db_data = load(open('mydb_setup.json'))
    mydb = mysql.connector.connect(**db_data)
    mycursor = mydb.cursor()
    mycursor.execute('SELECT title, date, source, id, doi FROM additional')
    additional = mycursor.fetchall()
    mycursor.execute('SELECT title, date, source, eid FROM publications')
    publications = mycursor.fetchall()
    titles_pub, titles_add = dict(), dict()
    for ele in additional:
        titles_add[normalize(ele[1])] = {'date': str(ele[1]), 'source': ele[2],
                                         'id': ele[3], 'doi': ele[4]}
    for ele in publications:
        titles_pub[normalize(ele[1])] = {'date': str(ele[1]), 'source': ele[2],
                                         'eid': ele[3]}
    for ele in titles_pub:
        if ele in titles_add:
            # Check if other information matches.
            if titles_add[ele]['doi'] == '':
                merge_records(mydb, mycursor, titles_pub[ele]['eid'], titles_add[ele]['id'])


def title_metadata(title):
    # TODO: query crossref to get doi based on title.
    metadata = dict()
    # Update the headers to get into the polite pool.
    headers = requests.utils.default_headers()
    headers['User-Agent'] = headers['User-Agent'] + ' mailto:ugnemmilasiunaite@gmail.com'
    url = 'https://api.crossref.org/works?query.bibliographic'
    response = requests.get(url, headers=headers).json()['message']
    return


def doi_metadata(doi):
    """
    Get metadata from crossref via a doi for a particular publication.

    Parameters
    ----------
    doi : string
        DOI associated with the publication.

    Returns
    -------
    metadata : dict
        Dictionary containing the metadata for the given doi.

    """
    metadata = dict()
    # Update the headers to get into the polite pool.
    headers = requests.utils.default_headers()
    headers['User-Agent'] = headers['User-Agent'] + ' mailto:ugnemmilasiunaite@gmail.com'
    url = 'https://api.crossref.org/works/' + doi
    response = requests.get(url, headers=headers).json()['message']
    # Get info about the authors.
    metadata['authors'] = []
    for auth in response['author']:
        metadata['authors'].append(
            {'surname': auth['family'], 'given_name': auth['given']})
        # Find the initials of the author.
        initials = ''
        for name in auth['given'].split(' '):
            if '-' in name:
                split = name.split('-')
                if split[0][-1] == '.':
                    initials = initials + split[0] + '-'
                else:
                    initials = initials + split[0][0] + '.-'
                if split[1][-1] == '.':
                    initials = initials + split[1]
                else:
                    initials = initials + split[1][0] + '.'
            elif name[-1] == '.':
                initials = initials + name
            else:
                initials = initials + name[0] + '.'
        metadata['authors'][-1].update({'initials': initials})
        authname = auth['family'] + ' ' + metadata['authors'][-1]['initials']
        metadata['authors'][-1].update({'authname': authname})
    metadata['title'] = response['title'][0] if type(response['title']) is list else response['title']
    metadata['ref_count'] = response['reference-count']
    metadata['references'] = response['reference']
    metadata['type'] = response['type']
    # Get the publication date.
    if len(response['published']['date-parts'][0]) == 3:
        metadata['date'] = '-'.join(response['published']['date-parts'][0])
        if len(metadata['date']) < 10:
            if metadata['date'][5] != 0 and metadata['date'][6] == '-':
                metadata['date'] = metadata['date'][:5] + '0' + metadata['date'][5:]
        if len(metadata['date']) < 10:
            metadata['date'] = metadata['date'][-1] + '0' + metadata['date'][-1]
    elif len(response['published']['date-parts'][0]) == 2:
        metadata['date'] = '-'.join(response['published']['date-parts'][0]) + '-01'
        if len(metadata['date']) < 10:
            metadata['date'] = metadata['date'][:5] + '0' + metadata['date'][5:]
    elif len(response['published']['date-parts'][0]) == 1:
        metadata['date'] = response['published']['date-parts'][0] + '-01-01'
    else:
        metadata['date'] = ''
    # TODO: check for abstract.
    metadata['citedby'] = response['is-referenced-by-count']
    metadata['author_count'] = len(response['author'])
    metadata['source'] = response['container-title'] if type(response['container-title']) is str else ';'.join(response['container-title'])
    return metadata


def fill_missing():
    """
    Fill missing info for records in additional table with a doi.

    Returns
    -------
    None.

    """
    db_data = load(open('mydb_setup.json'))
    data = load(open('save.json'))
    mydb = mysql.connector.connect(**db_data)
    mycursor = mydb.cursor()
    mycursor.execute('SELECT id, doi, date, citedby, authors, source FROM'
                     ' additional WHERE title is NULL and doi != ""')
    records = mycursor.fetchall()
    for rec in records:
        # Get metadata related to the doi.
        metadata = doi_metadata(rec[1])
        # Prepare which columns to update via making a string.
        string = 'title="' + metadata['title'] + '"'
        # Check if need to update the date.
        if str(rec[2]) == '':
            string = string + ', date="' + metadata['date'] + '"'
        elif metadata['date'] != str(rec[2]):
            if int(metadata['date'][:4]) == int(str(rec[2])[:4]):
                if int(metadata['date'][5:7]) == int(str(rec[2])[5:7]):
                    if metadata['date'][-2:] != '01':
                        string = string + ', date="' + metadata['date'] + '"'
                elif int(metadata['date'][5:7]) < int(str(rec[2])[5:7]) and metadata['date'][5:] != '01-01':
                    string = string + ', date="' + metadata['date'] + '"'
            elif int(metadata['date'][:4]) < int(str(rec[2])[:4]):
                string = string + ', date="' + metadata['date'] + '"'
        # Check if need to update citedby count.
        if int(metadata['citedby']) > int(rec[3]):
            string = string + ', citedby=' + str(metadata['citedby'])
        # Check if need to update authors.
        if rec[4] != '':
            authors, val = [], []
            for auth in metadata['authors']:
                authid = data['auth_my']
                data['auth_my'] = data['auth_my'] + 1
                authors.append(str(authid))
                val.append((authid, auth['authname'], auth['surname'],
                            auth['given_name'], auth['initials']))
            mycursor.executemany('INSERT INTO authors (id, authname, surname,'
                                 ' given_name, initials) VALUES (%s, %s, %s,'
                                 ' %s, %s)', val)
            mydb.commit()
            with open('save.json', 'w', encoding='utf8') as json_file:
                dump(data, json_file, ensure_ascii=False)
            authors = ','.join(authors)
            string = string + ', authors="' + authors + '"'
        # Check if need to update the source.
        if rec[5] == '' or rec[5].casefold() != metadata['source'].casefold():
            string = string + ', source="' + metadata['source'] + '"'
        # Update the record.
        mycursor.execute('UPDATE additional SET {string} WHERE eid={eid}'.format(string=string, eid=rec[0]))
        mydb.commit()


def main():
    return


if __name__ == '__main__' or __name__ == 'builtins':
    main()
