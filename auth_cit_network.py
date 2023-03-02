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


def to_from_w_labels(G, labels):
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


def print_table(data, labels):
    """
    Print table with the statistics for each subfield.

    Parameters
    ----------
    data : dict
        Dictionary containing entries associated with each subfield.

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
    f = open('table.txt', 'w')
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
    print('Maximum ' + name + ':', data[0])
    print('Average ' + name + ':', round(mean(data), 3))
    print('Standard deviation:', round(stdev(data), 3))
    print('Mode:', mode(data))
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
    # plt.savefig(fname=name, format='svg')
    return


def author_citation_graph(subfields):
    mydb = mysql.connector.connect(
        host='localhost',
        user='root',
        database='trial',
        password='RunTheNum6!',
        auth_plugin='mysql_native_password',
    )
    mycursor = mydb.cursor()
    mycursor.execute(
        'SELECT eid, field, cites, authors FROM publications')
    data_pub = mycursor.fetchall()
    mycursor.execute(
        "SELECT id, authors, referenced_by FROM additional")
    data_add = mycursor.fetchall()
    authors = set()
    connections = set()
    work_to_auth = dict()
    weights = dict()
    fields = dict()
    counts = dict()
    empty = dict()
    for entry in subfields:
        empty[entry] = 0
    for entry in data_pub:
        work_to_auth[str(entry[0])] = entry[3].split(',')
    for entry in data_add:
        work_to_auth[str(entry[0])] = entry[1].split(',')
    # Add authors from publications table.
    for entry in data_pub:
        fd = entry[1].split(',')
        auth = entry[3].split(',')
        for a in auth:
            if a not in counts:
                counts[a] = empty.copy()
            for ele in fd:
                counts[a][ele] = counts[a][ele] + 1
        authors.update(auth)
        try:
            cites = entry[2].split(',')
        except AttributeError:
            continue
        if cites[0] != '':
            for ele in set(cites):
                for w in work_to_auth[ele]:
                    for a in auth:
                        if (a, w) in connections:
                            weights[(a, w)] = weights[(a, w)] + 1
                        else:
                            connections.add((a, w))
                            weights[(a, w)] = 1
    # Add authors from additional table.
    for entry in data_add:
        auth = entry[1].split(',')
        fd = 'OTHER'
        auth = entry[1].split(',')
        for a in auth:
            if not(a in counts):
                counts[a] = empty.copy()
            counts[a][fd] = counts[a][fd] + 1
        authors.update(auth)
        try:
            citedby = entry[2].split(',')
        except AttributeError:
            continue
        for ele in set(citedby):
            for w in work_to_auth[ele]:
                for a in auth:
                    if (w, a) in connections:
                        weights[(w, a)] = weights[(w, a)] + 1
                    else:
                        connections.add((w, a))
                        weights[(w, a)] = 1
    mycursor.close()
    mydb.close()
    subfields.discard('OTHER')
    # Set labels for the nodes.
    for a in counts:
        fd = []
        for f in subfields:
            if counts[a][f] > 0:
                fd.append(f)
        if len(fd) > 0:
            fields[a] = ','.join(fd)
        else:
            fields[a] = 'OTHER'
    # Build the graph.
    GA = nx.DiGraph()
    GA.add_nodes_from(authors)
    GA.add_edges_from(connections)
    nx.set_edge_attributes(GA, weights, 'weight')
    nx.set_node_attributes(GA, fields, 'field')
    return GA


def main():
    keyword_to_abbr = dict()
    f = open('added_keywords.txt', 'r')
    line = f.readline()
    while line != '':
        ln = line.split(':')
        keyword_to_abbr[ln[0][:-1]] = ln[1][1:-1]
        line = f.readline()
    f.close()
    subfields = set(keyword_to_abbr.items())
    labels = []
    for field in subfields:
        n_labels = len(labels)
        for j in range(n_labels):
            labels.append(labels[j] + ',' + field)
        labels.append(field)
    subfields.add('OTHER')
    labels.insert(0, 'OTHER')

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
    heatmap(arr=arr2, labels=labels, name='Incoming')

    # Calculate array for outgoing citations.
    arr1 = np.empty((n_labels, n_labels), dtype=float)
    for i in range(n_labels):
        n = e_from[labels[i]]
        for j in range(n_labels):
            arr1[i, j] = e_w[(labels[i], labels[j])] / n
    # Save array as heatmap.
    heatmap(arr=arr1, labels=labels, name='Outgoing')

    deg_out, deg_in = props_per_cpt(GA, labels)
    graph_stats(GA, name='outdegree')
    # print_table(deg_out, labels)
    graph_stats(GA, name='indegree')
    print_table(deg_in, labels)
    return


main()
