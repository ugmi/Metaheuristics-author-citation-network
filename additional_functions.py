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
from html import unescape


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
        'SELECT eid, field FROM publications WHERE field LIKE "%' + old + '"')
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
        Argument labels=['HS','CS'] returns {'HS': 'A1', 'CS': 'A2'}.

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

    """
    mycursor.execute(
        f'SELECT authors, author_count FROM publications WHERE eid={idp}')
    info_idp = list(mycursor.fetchall()[0])
    mycursor.execute(
        f'SELECT authors, referenced_by FROM additional WHERE id={ida}')
    info_ida = list(mycursor.fetchall()[0])
    # Get the information about the authors of publication with id idp.
    info_idp.append(dict())
    for author in info_idp[0].split(','):
        mycursor.execute(
            f'SELECT authname, given_name, aka FROM authors WHERE id={int(author)}')
        author_info = mycursor.fetchall()[0]
        try:
            aka = author_info[2].split()
        except AttributeError:
            aka = []
        info_idp[2].update(
            {normalize(author_info[0]): (author, author_info[1], aka)})
    # Get the information about the authors of publication with id ida.
    info_ida.append(dict())
    for author in info_ida[0].split(','):
        mycursor.execute(
            f'SELECT authname, given_name, aka FROM authors WHERE id={int(author)}')
        author_info = mycursor.fetchall()[0]
        try:
            aka = author_info[2].split(',')
        except AttributeError:
            aka = []
        info_ida[2].update(
            {normalize(author_info[0]): (author, author_info[1], aka)})
    del author_info, author, aka
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
        mycursor.execute(f'UPDATE authors SET aka="{",".join(aliases[ala])}" WHERE id={int(ala)}')
    mydb.commit()
    # Update related records.
    for ref in info_ida[1].split(','):
        mycursor.execute(
            f'SELECT cites FROM publications WHERE eid={int(ref)}')
        referenced_articles = str(mycursor.fetchall()[0][0])
        if str(idp) in referenced_articles.split(','):
            continue
        elif referenced_articles.split(',')[0] == '':
            referenced_articles = str(idp)
        else:
            referenced_articles = referenced_articles + ',' + str(idp)
        mycursor.execute(
            f'UPDATE publications SET cites="{referenced_articles}" WHERE eid={int(ref)}')
        mydb.commit()
    # Remove the other record.
    mycursor.execute(f'DELETE FROM additional WHERE id={ida}')
    mydb.commit()


def merge_matching_doi():
    """Merge all records with identical doi from the two tables."""
    db_data = load(open('mydb_setup.json'))
    mydb = mysql.connector.connect(**db_data)
    mycursor = mydb.cursor()
    mycursor.execute('SELECT doi FROM additional')
    other_dois = mycursor.fetchall()
    mycursor.execute('SELECT doi FROM publications')
    publication_dois = mycursor.fetchall()
    for doi in other_dois:
        if doi in publication_dois:
            mycursor.execute(
                f'SELECT eid from publications WHERE doi="{doi[0]}"')
            idp = mycursor.fetchall()[0][0]
            mycursor.execute(f'SELECT id from additional WHERE doi="{doi[0]}"')
            ida = mycursor.fetchall()[0][0]
            merge_records(mydb, mycursor, idp, ida)


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
        other_titles[normalize(ele[1])] = {'date': str(ele[1]), 'source': ele[2],
                                         'id': ele[3], 'doi': ele[4]}
    for ele in info_publications:
        publication_titles[normalize(ele[1])] = {'date': str(ele[1]), 'source': ele[2],
                                         'eid': ele[3]}
    for ele in publication_titles:
        if ele in other_titles:
            # TODO: Check if other information matches.
            if other_titles[ele]['doi'] == '':
                # Check if dates match.
                if other_titles[ele]['date'] == 'None' or other_titles[ele]['date'][:-3] == publication_titles[ele]['date'][:-3]:
                    merge_records(mydb, mycursor, publication_titles[ele]['eid'], other_titles[ele]['id'])


def get_initials(given_name):
    """Return initials in one string for a given name."""
    initials = ''
    for name in given_name.split(' '):
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
    return initials


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
        Date string.

    """
    if len(date_parts) == 3:
        date = '-'.join(date_parts)
        if len(date) < 10:
            if date[5] != '0' and date[6] == '-':
                date = date[:5] + '0' + date[5:]
        if len(date) < 10:
            date = date[-1] + '0' + date[-1]
    elif len(date_parts) == 2:
        date = '-'.join(date_parts) + '-01'
        if len(date) < 10:
            date = date[:5] + '0' + date[5:]
    elif len(date_parts) == 1:
        date = date_parts[0] + '-01-01'
    else:
        date = ''
    return date


def title_metadata(title, authors, date=''):
    """
    Get the doi and other metadata based on the publication`s title.

    Parameters
    ----------
    title : str
        Title of the publication.
    authors : dict
        Dictionary of information about the authors, keyed by authnames.
    date : str, optional
        Date of publication. The default is ''.

    Returns
    -------
    metadata : dict
        Information about the publication from crossref.

    """
    metadata = dict()
    norm = normalize(title)
    metadata = dict()
    # Update the headers to get into the polite pool.
    headers = requests.utils.default_headers()
    headers['User-Agent'] = headers['User-Agent'] + ' mailto:ugnemmilasiunaite@gmail.com'
    if date == '':
        url = 'https://api.crossref.org/works?query.bibliographic="' + title + '"&rows=5'
    else:
        url = 'https://api.crossref.org/works?query.bibliographic="' + title + ', ' + date + '"&rows=5'
    # Get a list of 5 best matching records based on title and/or date.
    response = requests.get(url, headers=headers).json()['message']['items']
    for i in range(5):
        if normalize(response[i]['title'][0]) == norm:
            # Get info about the authors.
            metadata['authors'] = dict()
            for author in response['author']:
                temp = {
                    'surname': author['family'],
                    'given_name': author['given'],
                    'initials': get_initials(author['given'])
                    }
                authname = author['family'] + ' ' + temp['initials']
                metadata['authors'].update({authname: temp})
            # Check if authors match.
            for author in authors:
                if author not in metadata['authors']:
                    # TODO: consider possiblility of switched names.
                    metadata['authors'] = []
                    break
            # Continue to next record if authors do not match.
            if metadata['authors'] == [] and len(response['author']) != 0:
                continue
            metadata['doi'] = response['DOI']
            metadata['ref_count'] = response['reference-count']
            metadata['references'] = response['reference']
            metadata['type'] = response['type']
            metadata['citedby'] = response['is-referenced-by-count']
            metadata['author_count'] = len(response['author'])
            metadata['source'] = response['container-title'] if type(response['container-title']) is str else '; '.join(response['container-title'])
            metadata['date'] = get_date(response['published']['date-parts'][0])
            if 'abstract' in response:
                metadata['abstract'] = response['abstract']
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
    metadata['authors'] = dict()
    for author in response['author']:
        temp = {
            'surname': author['family'],
            'given_name': author['given'],
            'initials': get_initials(author['given'])
            }
        authname = author['family'] + ' ' + temp['initials']
        metadata['authors'].update({authname: temp})
    metadata['title'] = response['title'][0] if type(response['title']) is list else response['title']
    metadata['ref_count'] = response['reference-count']
    metadata['references'] = response['reference']
    metadata['type'] = response['type']
    metadata['date'] = get_date(response['published']['date-parts'][0])
    # Check if there is an abstract.
    if 'abstract' in response:
        metadata['abstract'] = response['abstract']
    metadata['citedby'] = response['is-referenced-by-count']
    metadata['author_count'] = len(response['author'])
    metadata['source'] = response['container-title'] if type(response['container-title']) is str else '; '.join(response['container-title'])
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
    for row in records:
        # Get metadata related to the doi.
        metadata = doi_metadata(row[1])
        # Prepare which columns to update via making a string.
        string = 'title="' + metadata['title'] + '"'
        # Check if need to update the date.
        if str(row[2]) == '':
            string = string + ', date="' + metadata['date'] + '"'
        elif metadata['date'] != str(row[2]):
            if int(metadata['date'][:4]) == int(str(row[2])[:4]):
                if int(metadata['date'][5:7]) == int(str(row[2])[5:7]):
                    if metadata['date'][-2:] != '01':
                        string = string + ', date="' + metadata['date'] + '"'
                elif int(metadata['date'][5:7]) < int(str(row[2])[5:7]) and metadata['date'][5:] != '01-01':
                    string = string + ', date="' + metadata['date'] + '"'
            elif int(metadata['date'][:4]) < int(str(row[2])[:4]):
                string = string + ', date="' + metadata['date'] + '"'
        # Check if need to update citedby count.
        if int(metadata['citedby']) > int(row[3]):
            string = string + ', citedby=' + str(metadata['citedby'])
        # Check if need to update authors.
        if row[4] != '':
            authors, values_to_insert = [], []
            for author in metadata['authors']:
                authid = data['auth_my']
                data['auth_my'] = data['auth_my'] + 1
                authors.append(str(authid))
                values_to_insert.append(
                    (authid, author['authname'], author['surname'],
                     author['given_name'], author['initials']))
            mycursor.executemany('INSERT INTO authors (id, authname, surname,'
                                 ' given_name, initials) VALUES (%s, %s, %s,'
                                 ' %s, %s)', values_to_insert)
            mydb.commit()
            with open('save.json', 'w', encoding='utf8') as json_file:
                dump(data, json_file, ensure_ascii=False)
            authors = ','.join(authors)
            string = string + ', authors="' + authors + '"'
        # Check if need to update the source.
        if row[5] == '' or row[5].casefold() != metadata['source'].casefold():
            string = string + ', source="' + metadata['source'] + '"'
        # Update the record.
        mycursor.execute(f'UPDATE additional SET {string} WHERE eid={row[0]}')
        mydb.commit()


def main():
    return


if __name__ == '__main__' or __name__ == 'builtins':
    main()
