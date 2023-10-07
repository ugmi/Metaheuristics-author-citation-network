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
    mycursor.execute(
        f'SELECT authors, author_count FROM publications WHERE eid={idp}')
    res = mycursor.fetchall()[0]
    info_idp = {
        'author_string': res[0],
        'author_count': res[1]}
    mycursor.execute(
        f'SELECT authors, referenced_by FROM additional WHERE id={ida}')
    res = mycursor.fetchall()[0]
    info_ida = {
        'author_string': res[0],
        'citing_articles': res[1]}
    # Get the information about the authors of publication with id idp.
    info_idp.update({'authors': dict()})
    for authid in info_idp['author_string'].split(','):
        mycursor.execute(
            f'SELECT authname, given_name, aka FROM authors WHERE id={authid}')
        author_info = mycursor.fetchall()[0]  # (authname, given_name, aka)
        try:
            aka = author_info[2].split()
        except AttributeError:
            aka = []
        info_idp['authors'].update(
            {normalize(author_info[0]): (authid, author_info[1], aka)})
    # Get the information about the authors of publication with id ida.
    info_ida.update({'authors': dict()})
    for authid in info_ida['author_string'].split(','):
        mycursor.execute(
            f'SELECT authname, given_name, aka FROM authors WHERE id={authid}')
        author_info = mycursor.fetchall()[0]  # (authname, given_name, aka)
        try:
            aka = author_info[2].split(',')
        except AttributeError:
            aka = []
        info_ida['authors'].update(
            {normalize(author_info[0]): (authid, author_info[1], aka)})
    aliases = dict()  # Store ids with corresponding aliases.
    for name in info_ida['authors']:
        if name in info_idp['authors']:
            # Add & append ids in aliases if we match atuhor names.
            authid_ida = info_ida['authors'][name][0]
            authid_idp = info_idp['authors'][name][0]
            # Check if not already present in the dictionary.
            if authid_ida in aliases:
                if authid_idp not in aliases[authid_ida]:
                    aliases[authid_ida].append(authid_idp)
            else:
                # Join lists of aliases.
                aliases[authid_ida] = [authid_idp] + \
                    info_ida['authors'][name][2]
            # Check if not already present in the dictionary.
            if authid_idp in aliases:
                if authid_ida not in aliases[authid_idp]:
                    aliases[authid_idp].append(authid_ida)
            else:
                # Join lists of aliases.
                aliases[authid_idp] = [authid_ida] + \
                    info_idp['authors'][name][2]
    # Check if matches for all authors had been found.
    if len(aliases) != len(info_idp['authors'])*2:
        print(info_idp['authors'])
        print(info_ida['authors'])
        raise Exception('Authors don`t match')
    # Merge authors.
    for ala in aliases:
        mycursor.execute(
            f'UPDATE authors SET aka="{",".join(aliases[ala])}" WHERE id={ala}')
    mydb.commit()
    # Update the reference lists of articles that cite the duplicated record.
    for ref in info_ida['citing_articles'].split(','):
        mycursor.execute(
            f'SELECT cites FROM publications WHERE eid={ref}')
        referenced_articles = str(mycursor.fetchall()[0][0])
        if str(idp) in referenced_articles.split(','):
            continue
        elif referenced_articles.split(',')[0] == '':
            referenced_articles = str(idp)
        else:
            referenced_articles = f'{referenced_articles},{idp}'
        mycursor.execute(
            f'UPDATE publications SET cites="{referenced_articles}" WHERE eid={ref}')
        mydb.commit()
    # Remove the other record.
    mycursor.execute(f'DELETE FROM additional WHERE id={ida}')
    mydb.commit()


def merge_matching_doi():
    """Merge all records with identical doi from the two tables.

    For every doi in additional table, if it is also in the publications table,
    get associated scopus ids and titles. If titles match, merge records."""
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
                f'SELECT eid, title from publications WHERE doi="{doi[0]}"')
            res = mycursor.fetchall()[0]
            id_p, title_p = res
            mycursor.execute(
                f'SELECT id, title from additional WHERE doi="{doi[0]}"')
            res = mycursor.fetchall()[0]
            id_a, title_a = res
            if normalize(title_p) == normalize(title_a):
                merge_records(mydb, mycursor, id_p, id_a)


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
    for ele in publication_titles:
        if ele in other_titles:
            # TODO: Check if other information matches.
            if other_titles[ele]['doi'] == '':
                # Check if dates match.
                year_and_month_match = (other_titles[ele]['date'][:-3] == publication_titles[ele]['date'][:-3])
                if other_titles[ele]['date'] == 'None' or year_and_month_match:
                    merge_records(
                        mydb, mycursor, publication_titles[ele]['eid'], other_titles[ele]['id'])


def get_initials(given_name):
    """Return initials in one string for a given name."""
    initials = ''
    for name in given_name.split(' '):
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
    if len(date_parts) == 3:
        date = '-'.join(date_parts)
        if len(date) < 10:  # Zeros missing.
            if date[5] != '0' and date[6] == '-':
                date = f'{date[:5]}0{date[5:]}'
        if len(date) < 10:
            date = f'{date[-1]}0{date[-1]}'
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
    metadata = dict()
    # Update the headers to get into the polite pool.
    headers = requests.utils.default_headers()
    headers['User-Agent'] = f"{headers['User-Agent']} mailto:ugnemmilasiunaite@gmail.com"
    if date == '':
        url = f'https://api.crossref.org/works?query.bibliographic="{title}"&rows=5'
    else:
        url = f'https://api.crossref.org/works?query.bibliographic="{title}, {date}"&rows=5'
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
                authname = f"{author['family']} {temp['initials']}"
                metadata['authors'].update({authname: temp})
            # Check if authors match.
            for author in authors:
                if author not in metadata['authors']:
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
            metadata['source'] = response['container-title'] if type(
                response['container-title']) is str else '; '.join(response['container-title'])
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
    metadata : dictionary
        Dictionary containing the metadata for the given doi.

    """
    metadata = dict()
    # Update the headers to get into the polite pool.
    headers = requests.utils.default_headers()
    headers['User-Agent'] = f"{headers['User-Agent']} mailto:ugnemmilasiunaite@gmail.com"
    url = f'https://api.crossref.org/works/{doi}'
    response = requests.get(url, headers=headers).json()['message']
    # Get info about the authors.
    metadata['authors'] = dict()
    for author in response['author']:
        temp = {
            'surname': author['family'],
            'given_name': author['given'],
            'initials': get_initials(author['given'])
        }
        authname = f"{author['family']} {temp['initials']}"
        metadata['authors'].update({authname: temp})
    metadata['title'] = response['title'][0] if type(
        response['title']) is list else response['title']
    metadata['ref_count'] = response['reference-count']
    metadata['references'] = response['reference']
    metadata['type'] = response['type']
    metadata['date'] = get_date(response['published']['date-parts'][0])
    # Check if there is an abstract.
    if 'abstract' in response:
        metadata['abstract'] = response['abstract']
    metadata['citedby'] = response['is-referenced-by-count']
    metadata['author_count'] = len(response['author'])
    metadata['source'] = response['container-title'] if type(
        response['container-title']) is str else '; '.join(response['container-title'])
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
        string = f'title="{metadata["title"]}"'
    elif match_type == 'title':
        string = f'doi="{metadata["doi"]}"'
    # Check if need to update the date.
    if str(row[2]) == '':
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
    if row[5] == '' or row[5].casefold() != metadata['source'].casefold():
        string = f'{string}, source="{metadata["source"]}"'
    return string


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
    data = load(open('save.json'))
    mydb = mysql.connector.connect(**db_data)
    mycursor = mydb.cursor()
    if match_type == 'doi':
        mycursor.execute('SELECT title, doi, date, citedby, authors, source,'
                         ' id FROM additional WHERE doi != ""')
    elif match_type == 'title':
        if table == 'publications':
            mycursor.execute('SELECT title, doi, date, citedby, authors, '
                             'source, eid FROM publications WHERE doi = "" and'
                             ' authors != ""')
        elif table == 'additional':
            mycursor.execute('SELECT title, doi, date, citedby, authors,'
                             ' source, id FROM additional WHERE title is not'
                             ' NULL and authors != ""')
        else:
            raise Exception('Please choose a different table.')
    else:
        raise Exception('Please choose a different match_type.')
    records = mycursor.fetchall()
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
                mycursor.execute(
                    f'SELECT authname from authors where id={ele}')
                authors.add(mycursor.fetchall()[0][0])
            metadata = title_metadata(row[0], authors, date=date)
        if len(metadata) == 0:
            continue
        # Prepare information to update via making a string.
        string = get_update_string(metadata, row, match_type)
        # Check if need to update authors.
        if row[4] != '':
            authors, values_to_insert = [], []
            for author in metadata['authors']:
                authid = data['auth_my']
                data['auth_my'] += 1
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
            string = f'{string}, authors="{authors}"'
        # Update the record.
        if table == 'additional':
            mycursor.execute(
                f'UPDATE additional SET {string} WHERE id={row[6]}')
        else:
            mycursor.execute(
                f'UPDATE publications SET {string} WHERE eid={row[6]}')
        mydb.commit()


def main():
    return


if __name__ == '__main__' or __name__ == 'builtins':
    main()
