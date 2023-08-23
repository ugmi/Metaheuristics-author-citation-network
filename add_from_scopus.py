#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 23 21:08:14 2023

@author: milasiunaite
"""

import requests
import mysql.connector
from json import load, dump, JSONDecodeError
from unidecode import unidecode


def get_keywords():
    """
    Read keywords and their abbreviations from the text file.

    Example
    -------
        'particle swarm optimization : PSO ' in the text file corresponds to
    the pair {'particle swarm optimization': 'PSO'} in the dictionary.

    Returns
    -------
    keyword_to_abbr : dict
        Dictionary of keywords to their abbreviations.

    """
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
    norm = norm.replace('optimis', 'optimiz')
    norm = norm.replace('-', ' ')
    return norm


def field(ele, data, keyword, kw):
    """
    Assign a subfield to the given publication.

    We assign the keyword as the label of the publication
    if the keyword is contained in the title or abstract.
    Else, set the label to 'OTHER'.
    Return the label and updated data about the process.

    Parameters
    ----------
    ele : dict
        Dictionary with info about the publication.
    data : dict
        Dictionary containing information about current state of the project.
    keyword : string
        Keyword string.
    kw : string
        Abbreviation of the keyword.

    Returns
    -------
    label : string
        Keyword corresponding to the element.
    data : dict
        Updated dictionary.

    """
    try:
        abstract = ele['dc:description']
    except KeyError:
        abstract = ''
    try:
        title = ele['dc:title']
    except KeyError:
        title = ''
    normalized_title = normalize(title)
    if normalized_title.find(keyword) != -1:
        label = kw
        if ele['eid'] not in data['eids']:
            data['eids'].append(ele['eid'])
    else:
        normalized_abstract = normalize(abstract)
        if normalized_abstract.find(keyword) != -1:
            label = kw
            if ele['eid'] not in data['eids']:
                data['eids'].append(ele['eid'])
        else:
            label = 'OTHER'
    return label, data


def values_to_insert(ele, label, authors, affiliations, cites=''):
    """
    Compile a tuple of values to insert to the database.

    Parameters
    ----------
    ele : dict
        Dictionary with info about the publication.
    label : string
        Label corresponding to the publication.
    authors : list
        List of the authors of the publication.
    affiliations : list
        List of author affiliations.
    cites : string, optional
        Id of the cited publication, if applicable. The default is ''.

    Returns
    -------
    val : tuple
        Values to insert to the database.

    """
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
    try:
        authcount = ele['author-count']['@total']
    except KeyError:
        authcount = 0
    val = (ele['eid'][7:], title, source, issn, volume, issue,
           ele['prism:coverDate'], doi, abstract, ele['citedby-count'],
           ','.join(affiliations), ele['subtypeDescription'],
           authcount, ','.join(authors), authkeywords, source_id,
           ele['prism:url'], label, cites)
    return val


def add_record(mydb, mycursor, all_ids, data, ele, sql, keyword, kw, eid=''):
    """
    Insert a row into the database with the required info.

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
    sql : dict
        Dictionary of strings for inserting rows into the database.
    keyword : string
        Keyword string.
    kw : string
        Abbreviation of the keyword.
    eid : string, optional
        Id of publication if it's not in the database. The default is ''.

    Returns
    -------
    all_ids : dict
        Updated dictionary.
    data : TYPE
        Updated dictionary.

    """
    ele_eid = int(ele['eid'][7:])
    if (ele_eid,) not in all_ids['publications']:  # Add new record to table publications
        data['newlyadded'] = data['newlyadded'] + 1
        all_ids['publications'].add((int(ele['eid'][7:]),))
        affiliations, val, authors = [], [], []
        try:
            for affiliation in ele['affiliation']:
                afid = affiliation['afid']
                affiliations.append(afid)
                if (int(afid),) not in all_ids['affiliations']:
                    all_ids['affiliations'].add((int(afid),))
                    val.append((afid, affiliation['affilname'],
                                affiliation['affiliation-city'],
                                affiliation['affiliation-country'],
                                affiliation['affiliation-url']))
            if len(val) > 0:
                mycursor.executemany(sql['v'], val)
                mydb.commit()
        except KeyError:
            pass  # No info on affiliations
        val = []
        try:
            for author in ele['author']:
                authid = author['authid']
                if authid == '':
                    authid = data['auth_my']
                    data['auth_my'] = data['auth_my'] + 1
                authors.append(authid)
                if (int(authid),) not in all_ids['authors']:
                    all_ids['a'].add((int(authid),))
                    try:
                        afids = ','.join([e['$'] for e in author['afid']])
                    except KeyError:
                        afids = ''  # No affiliation ids given
                    val.append((authid, author['authname'], author['surname'],
                                author['given-name'], author['initials'],
                                afids, author['author-url']))
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
        if (ele_eid,) in all_ids['others']:
            # Get publications that cite the given article
            mycursor.execute(
                f'SELECT referenced_by FROM additional WHERE id={ele_eid}')
            refs = str(mycursor.fetchall()[0][0])
            for ref in refs.split(','):
                mycursor.execute(
                    f'SELECT cites FROM publications WHERE eid={ref}')
                referenced_articles = str(mycursor.fetchall()[0][0])
                if str(ele_eid) in referenced_articles.split(','):
                    continue
                elif referenced_articles != '':
                    referenced_articles = referenced_articles + ',' + str(ele_eid)
                else:
                    referenced_articles = str(ele_eid)
                mycursor.execute(
                    f'UPDATE publications SET cites="{referenced_articles}" WHERE eid={ref}')
            # Remove from additional table
            all_ids['others'].discard((ele_eid,))
            mycursor.execute(
                f'DELETE FROM additional WHERE id={ele_eid}')
            mydb.commit()
    elif eid == '':  # Update label for article that's already in the table
        data['indatabase'] = data['indatabase'] + 1
        mycursor.execute(
            f'SELECT field FROM publications WHERE eid={ele_eid}')
        label = str(mycursor.fetchall()[0][0])
        if kw in label.split(','):
            return all_ids, data
        if label == 'OTHER':
            label = kw
            if str(ele['eid']) not in data['eids']:
                data['eids'].append(str(ele['eid']))
        else:
            label = label + ',' + kw
        mycursor.execute(
            f'UPDATE publications SET field="{label}" WHERE eid={ele_eid}')
        mydb.commit()
    return all_ids, data


def main():
    # Set up a connection to the local database
    db_data = load(open('mydb_setup.json'))
    mydb = mysql.connector.connect(**db_data)
    mycursor = mydb.cursor()
    # Configure headers, set api key and string
    headers = requests.utils.default_headers()
    data = load(open('headers.json'))
    for head in data:
        headers[head] = data[head]
    # Read data from file
    data = load(open('save.json'))
    data['records_checked'] = 0
    data['newlyadded'] = 0
    data['indatabase'] = 0
    # Collect ids
    all_ids = dict()
    mycursor.execute('SELECT eid FROM publications')
    all_ids['publications'] = set(mycursor.fetchall())
    mycursor.execute('SELECT id FROM affiliations')
    all_ids['affiliations'] = set(mycursor.fetchall())
    mycursor.execute('SELECT id FROM authors')
    all_ids['authors'] = set(mycursor.fetchall())
    mycursor.execute('SELECT id FROM additional')
    all_ids['others'] = set(mycursor.fetchall())
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
                    # Resolve some strange issue with cursors.
                    x = data['api'].find('cursor')
                    y = data['api'].find('&view')
                    cursor = data['api'][x+7:y]
                    cursor = cursor.replace('%3D', '=')
                # Get documents from SCOPUS that match the specified keyword
                if len(data['eids']) == 0:
                    response = requests.get(data['api'].format(
                        keyword=keyword), headers=headers)
                    try:
                        data['limit'] = int(
                            response.headers['X-RateLimit-Remaining'])
                    except KeyError:
                        data['limit'] = data['limit'] - 1
                    # Convert response object to json, which is easier to work with
                    try:
                        response = response.json()['search-results']
                    except JSONDecodeError:
                        with open('save.json', 'w', encoding='utf8') as json_file:
                            dump(data, json_file, ensure_ascii=False)
                        print(response)
                        print(response.content)
                        return
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
                                data['records_checked'] = data['records_checked'] + 1
                        except KeyError:
                            print(response)
                            continue
                    except KeyboardInterrupt:
                        cursor = response['cursor']['@next']
                        for link in response['link']:
                            if link['@ref'] == 'next':
                                data['api'] = link['@href']
                        with open('save.json', 'w', encoding='utf8') as json_file:
                            dump(data, json_file, ensure_ascii=False)
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
                        try:
                            data['limit'] = int(
                                response.headers['X-RateLimit-Remaining'])
                        except KeyError:
                            data['limit'] = data['limit'] - 1
                        try:
                            response = response.json()['search-results']
                        except KeyError:
                            data['cursor'] = '*'
                            continue
                        except JSONDecodeError:
                            with open('save.json', 'w', encoding='utf8') as json_file:
                                dump(data, json_file, ensure_ascii=False)
                            print(response)
                            print(response.content)
                            return
                        if data['cursor'] == response['cursor']['@next']:
                            data['cursor'] = '*'
                            data['eids'].remove(eid)
                            continue
                        try:
                            data['cursor'] = response['cursor']['@next']
                            for ele in response['entry']:
                                ele_eid = int(ele['eid'][7:])
                                if (ele_eid,) in all_ids['publications']:
                                    data['indatabase'] = data['indatabase'] + 1
                                    data['records_checked'] = data['records_checked'] + 1
                                    mycursor.execute(
                                        f'SELECT cites from publications WHERE eid={ele_eid}')
                                    referenced_articles = str(mycursor.fetchall()[0][0])
                                    if str(int(eid[7:])) in referenced_articles.split(','):
                                        continue
                                    if referenced_articles == '':
                                        referenced_articles = str(int(eid[7:]))
                                    else:
                                        referenced_articles = referenced_articles + ',' + str(int(eid[7:]))
                                    mycursor.execute(
                                        f'UPDATE publications SET cites="{referenced_articles}" WHERE eid={ele_eid}')
                                    mydb.commit()
                                    continue
                                all_ids, data = add_record(
                                    mydb, mycursor, all_ids, data, ele, sql, keyword, kw, eid=eid)
                                data['records_checked'] = data['records_checked'] + 1
                        except KeyboardInterrupt:
                            foo = mycursor.fetchall()  # Collect records to avoid raising errors
                            data['cursor'] = '*'
                            with open('save.json', 'w', encoding='utf8') as json_file:
                                dump(data, json_file, ensure_ascii=False)
                            data['cursor'] = response['cursor']['@next']
                            for ele in response['entry']:
                                ele_eid = int(ele['eid'][7:])
                                if (ele_eid,) in all_ids['publications']:
                                    data['indatabase'] = data['indatabase'] + 1
                                    mycursor.execute(
                                        f'SELECT cites from publications WHERE eid={ele_eid}')
                                    referenced_articles = str(mycursor.fetchall()[0][0])
                                    if str(int(eid[7:])) in referenced_articles.split(','):
                                        continue
                                    if referenced_articles == '':
                                        referenced_articles = str(int(eid[7:]))
                                    else:
                                        referenced_articles = referenced_articles + ',' + str(int(eid[7:]))
                                    mycursor.execute(
                                        f'UPDATE publications SET cites="{referenced_articles}" WHERE eid={ele_eid}')
                                    mydb.commit()
                                    continue
                                all_ids, data = add_record(
                                    mydb, mycursor, all_ids, data, ele, sql, keyword, kw, eid=eid)
                            return
            except KeyboardInterrupt:
                with open('save.json', 'w', encoding='utf8') as json_file:
                    dump(data, json_file, ensure_ascii=False)
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
        dump(data, json_file, ensure_ascii=False)


main()
