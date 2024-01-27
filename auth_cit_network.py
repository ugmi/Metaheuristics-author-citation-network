#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 24 14:04:40 2023

@author: milasiunaite
"""

import mysql.connector
import networkx as nx
import numpy as np
import matplotlib.pyplot as plt
from statistics import stdev, mean, mode, StatisticsError
from tabulate import tabulate
import pandas as pd
from json import load
from itertools import combinations, chain


def to_from_w_labels(G, labels):
    """
    Calculate the number of edges from one subfield to another.

    Parameters
    ----------
    G : nx.Graph or nx.DiGraph
        Graph whose properties we need to describe.
    labels : list
        List containing the labels of the subfields.

    Returns
    -------
    e_to : dict
        Dictionary of edge weights to a subfield.
    e_from : dict
        Dictionary of edge weights from a subfield.
    e_w : dict
        Dictionary of edge weights.

    """
    e_to = dict()
    for lb in labels:
        e_to[lb] = 0
    e_from = e_to.copy()
    e_w = dict()
    for i in range(len(labels)):
        for j in range(len(labels)):
            e_w[(labels[i], labels[j])] = 0
    for e in G.edges:
        f0 = G.nodes[e[0]]["field"]
        f1 = G.nodes[e[1]]["field"]
        e_w[(f0, f1)] += 1
        e_to[f1] += 1
        e_from[f0] += 1
    return e_to, e_from, e_w


def print_table(data, labels, name='table'):
    """
    Print table with the statistics for each subfield.

    Parameters
    ----------
    data : dict
        Dictionary containing entries associated with each subfield.
    labels : list
        List containing the labels of subfields.
    name : string, optional
        Name of text file to write the table in.

    Returns
    -------
    None.

    """
    info = {}
    # Calculate statistics for each subfield.
    for key in labels:
        info[key] = {}
        info[key]['average'] = mean(data[key])
        try:
            info[key]['stdv'] = stdev(data[key])
        except StatisticsError:
            info[key]['stdv'] = 0
        info[key]['total'] = sum(data[key])
        info[key]['size'] = len(data[key])
    # Print table to file.
    f = open(f'{name}.txt', 'w')
    tb = pd.DataFrame(data=info)
    f.write(tabulate(tb.transpose(), tablefmt='fancy_grid', floatfmt=".3f",
                     headers=['subfield', 'avg', 'stdev', 'total', 'size']))
    f.close()


def props_per_cpt(G, labels):
    """
    Calculate properties of the graph for each subfield.

    If graph is directed, return the information about indegrees, outdegrees,
    number of authors per paper for each subfield.
    If graph is undirected, return the information about idegrees and
    number of papers per author for each subfield.

    Parameters
    ----------
    G : nx.Graph or nx.DiGraph
        Graph whose properties we need to describe.
    labels : list
        List containing the labels of subfields.

    Returns
    -------
    tuple
        Tuple of dictionaries with data about the properties of the graph.

    """
    directed = nx.is_directed(G)
    if directed:
        # Dictionary of lists of node degrees in each subfield.
        cdeg_out, cdeg_in = {}, {}
        for key in labels:
            cdeg_out[key], cdeg_in[key] = [], []
        for n in G:
            c = G.nodes[n]['field']
            cdeg_out[c].append(G.out_degree[n])
            cdeg_in[c].append(G.in_degree[n])
        return (cdeg_out, cdeg_in)


def graph_stats(G, name, plot=False):
    """
    Print statistics about a certain property of the graph.

    Parameters
    ----------
    G : nx.Graph or nx.DiGraph
        Graph whose properties we need to describe.
    name : string
        Name of the property whose statistics we need to calculate.
    plot : bool, optional
        If true, print the bar chart of occurences. The default is False.

    Returns
    -------
    None.

    """
    name = name.casefold()
    if name == 'indegree':
        data = [entr[1] for entr in G.in_degree()]
    elif name == 'outdegree':
        data = [entr[1] for entr in G.out_degree()]
    elif name == 'degree':
        data = [entr[1] for entr in G.degree()]
    elif name == 'edge weight':
        data = [weight for u, v, weight in G.edges.data("weight")]
    elif name == 'node weight':
        data = [weight for u, weight in G.nodes.data("weight")]
    else:
        return
    data.sort(reverse=True)
    # Print basic stats.
    print(f'Maximum {name}: {data[0]}')
    print(f'Average {name}: {round(mean(data), 3)}')
    print(f'Standard deviation: {round(stdev(data), 3)}')
    print(f'Mode: {mode(data)}')
    print('Percentage of nodes that are mode:',
          round(data.count(mode(data))/len(data), 3))
    if plot:
        # Plot bar chart of number of occurences of each value of the property.
        plt.figure(figsize=(15, 15))
        plt.hist(data, bins=data[0])
        plt.title(name.capitalize())
        plt.show()


def heatmap(arr, labels, name):
    """
    Print a heatmap for the given array in greyscale.

    Parameters
    ----------
    arr : numpy array
        Array containing data.
    labels : list
        List of row and column labels.
    name : string
        Name of the picture file for the heatmap.

    Returns
    -------
    None.
    """
    n_labels = len(labels)
    ticks = np.arange(0.5, n_labels, 1)
    plt.figure(figsize=(n_labels, n_labels))
    plt.xticks(ticks, labels, fontsize=30, rotation='vertical')
    plt.yticks(ticks, labels, fontsize=30)
    plt.pcolormesh(arr, cmap='Greys')
    for i in range(n_labels):
        for j in range(n_labels):
            if arr[i, j] > 0.5:
                color = 'white'
            else:
                color = 'black'
            plt.annotate(
                str(round(arr[i, j], 2)), xy=(j+0.25, i+0.25), color=color)
    plt.title(name)
    plt.savefig(fname=name, format='svg')
    return


def get_data(source):
    """
    Collect data into a list of lists from csv file or database.

    Parameters
    ----------
    source : str
        Indicates the data source: csv file or database.

    Raises
    ------
    ValueError
        Indicates incorrect argument value.

    Returns
    -------
    publications_data : list
        Each entry contains a list of information about a record.
    others_data : list
        Each entry contains a list of information about a record.

    """
    if source == 'database':
        db_data = load(open('mydb_setup.json'))
        mydb = mysql.connector.connect(**db_data)
        mycursor = mydb.cursor()
        mycursor.execute(
            'SELECT eid, authors, cites, field FROM publications')
        publications_data = mycursor.fetchall()
        mycursor.execute(
            'SELECT id, authors, referenced_by FROM additional')
        others_data = mycursor.fetchall()
        mycursor.execute('SELECT id FROM authors')
        authors_data = set(mycursor.fetchall())
    elif source == 'csv':
        # Change the file paths if needed.
        publications_data = eval(pd.read_csv('publications.csv', sep=',', usecols=['eid', 'field', 'cites', 'authors']).to_json(orient='values'))
        others_data = eval(pd.read_csv('additional.csv', sep=',', usecols=['id', 'authors', 'referenced_by']).to_json(orient='values'))
        authors_data = set(eval(pd.read_csv('authors.csv', sep=',', usecols=['id']).to_json(orient='values')))
    else:
        raise ValueError('argument value not appropriate')
    return (publications_data, others_data, authors_data)


def get_author_fields(subfields, counts):
    fields = dict()
    gen_keys = (a for a in counts)
    for a in gen_keys:
        fd = []
        for f in subfields:
            if counts[a][f] > 0:
                fd.append(f)
        if len(fd) > 0:
            fields[a] = ','.join(fd)
        else:
            fields[a] = 'OTHER'
    return fields


def update_counts(author_list, counts, empty, fd):
    auth_gen = (a for a in author_list)
    for a in auth_gen:
        if a not in counts:
            counts[a] = empty.copy()
        for ele in fd:
            counts[a][ele] = counts[a][ele] + 1
    return counts


def get_generators_and_net(source):
    def concat(a, b):
        yield from a
        yield from b
    data_pub, data_add, authors = get_data(source)
    GA = nx.DiGraph()
    GA.add_nodes_from(authors)
    del authors
    print('Authors added')
    work_to_auth = dict()
    chain_works = concat(data_pub, data_add)
    pub_gen = (x for x in data_pub)
    add_gen = (x for x in data_add)
    del data_pub, data_add
    print('Generators finished')
    for entry in chain_works:
        work_to_auth[str(entry[0])] = entry[1].split(',')
    return GA, pub_gen, add_gen, work_to_auth


def author_citation_graph(subfields, source='database'):
    """
    Generate the author-citation network.

    Parameters
    ----------
    subfields : set
        Set of abbreviated names of subfields.
    source : str, optional
        Indicates the data source. The default is 'database'.

    Returns
    -------
    GA : nx.DiGraph
        Author-citation network.

    """
    GA, pub_gen, add_gen, work_to_auth = get_generators_and_net(source)
    weights = dict()
    counts = dict()
    empty = dict()
    for entry in subfields:
        empty[entry] = 0
    print('Adding pub')
    # Add authors from publications table.
    for entry in pub_gen:
        # fd = entry[3].split(',')
        auth = entry[1].split(',')
        # counts = update_counts(auth, counts, empty, fd)
        try:
            cites = set(entry[2].split(','))
            if cites != {''}:
                cites = (ele for ele in cites)
            else:
                continue
        except AttributeError:
            continue
        for ele in cites:
            auth_gen = (x for x in work_to_auth[ele])
            for w in auth_gen:
                for a in auth:
                    GA.add_edge(a, w)
                    
                    if (a, w) in weights:
                        weights[(a, w)] = weights[(a, w)] + 1
                    else:
                        GA.add_edge(a, w)
                        weights[(a, w)] = 1
                    
    print('Pub finished')
    # Add authors from additional table.
    for entry in add_gen:
        auth = entry[1].split(',')
        # counts = update_counts(auth, counts, empty, ['OTHER'])
        try:
            citedby = set(entry[2].split(','))
            if citedby != {''}:
                citedby = (ele for ele in citedby)
            else:
                continue
        except AttributeError:
            continue
        for ele in citedby:
            auth_gen = (x for x in work_to_auth[ele])
            for w in auth_gen:
                for a in auth:
                    GA.add_edge(w, a)
                    
                    if (w, a) in weights:
                        weights[(w, a)] = weights[(w, a)] + 1
                    else:
                        GA.add_edge(w, a)
                        weights[(w, a)] = 1
                    
    print('Add finished')
    subfields.discard('OTHER')
    # Set labels for the nodes.
    fields = get_author_fields(subfields, counts)
    nx.set_edge_attributes(GA, weights, 'weight')
    nx.set_node_attributes(GA, fields, 'field')
    return GA


def get_subfields(file_name):
    keyword_to_abbr = dict()
    f = open(file_name, 'r')
    line = f.readline()
    while line != '':
        ln = line.split(':')
        keyword_to_abbr[ln[0][:-1]] = ln[1][1:-1]
        line = f.readline()
    f.close()
    return set(keyword_to_abbr.values())


def get_labels(subfields, method):
    labels = []
    if method == 1:
        for i in range(1, len(subfields)):
            labels = labels + list(combinations(subfields, i))
        labels = [','.join(t) for t in labels]
    elif method == 2:
        for field in subfields:
            n_labels = len(labels)
            for j in range(n_labels):
                labels.append(labels[j] + ',' + field)
            labels.append(field)
    else:
        raise Exception('Incorrect method')
    labels.insert(0, 'OTHER')
    return labels


def main():
    # subfields = get_subfields('added_keywords.txt')
    subfields = {'GSO', 'GwSO', 'BA ', 'GSA', 'FA', 'ALO', 'KH', 'MBO', 'WOA',
                 'CSO', 'ANT', 'CRO', 'PSO', 'CS', 'BeA', 'BA', 'FOA', 'BFO',
                 'SFLA', 'ICA', 'WCA', 'FPA', 'BSO', 'IWO', 'TLBO', 'DE',
                 'MFO', 'GWO', 'BeA ', 'FWA', 'BBBC', 'CSS'}
    labels = get_labels(subfields, method=1)
    subfields.add('OTHER')

    GA = author_citation_graph(subfields)
    e_to, e_from, e_w = to_from_w_labels(GA, labels)
    # Remove intersections of subfields with no authors within.
    for lb in labels.copy():
        if e_to[lb] + e_from[lb] == 0:
            labels.remove(lb)
            e_to.pop(lb)
            e_from.pop(lb)
            for i in labels:
                e_w.pop((i, lb))
                e_w.pop((lb, i))
    n_labels = len(labels)

    # Calculate array for incoming citations.
    arr2 = np.empty((n_labels, n_labels), dtype=float)
    for i in range(n_labels):
        n = e_to[labels[i]]
        for j in range(n_labels):
            try:
                arr2[i, j] = e_w[(labels[j], labels[i])] / n
            except ZeroDivisionError:
                arr2[i, j] = n
    # Save array as heatmap.
    if False:
        heatmap(arr=arr2, labels=labels, name='Incoming')

    # Calculate array for outgoing citations.
    arr1 = np.empty((n_labels, n_labels), dtype=float)
    for i in range(n_labels):
        n = e_from[labels[i]]
        for j in range(n_labels):
            arr1[i, j] = e_w[(labels[i], labels[j])] / n
    # Save array as heatmap.
    if False:
        heatmap(arr=arr1, labels=labels, name='Outgoing')

    deg_out, deg_in = props_per_cpt(GA, labels)
    graph_stats(GA, name='outdegree')
    graph_stats(GA, name='indegree')
    if False:
        print_table(deg_in, labels, "table_in")
        print_table(deg_out, labels, "table_out")
    return


#main()
